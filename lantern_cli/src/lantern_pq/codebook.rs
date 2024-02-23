use crate::lantern_logger::Logger;
use crate::lantern_utils::quote_ident;
use rayon::prelude::*;
use std::cmp;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::Instant;
use postgres::{Client, NoTls, Transaction};

use super::{set_and_report_progress, report_progress, DatasetItem};
use linfa::traits::Fit;
use linfa::DatasetBase;
use linfa_clustering::KMeans;
use ndarray::Array2;

// Will run kmeans over dataset and return centroids
pub fn create_codebook_for_subset(
    dataset: Vec<&[f32]>,
    cluster_count: usize,
    subvector_id: usize,
    logger: &Logger,
) -> Result<Vec<Vec<f32>>, anyhow::Error> {
    let dim = dataset[0].len();
    let dataset_creation_time = Instant::now();
    let observations = DatasetBase::from(Array2::from_shape_vec(
        (dataset.len(), dim),
        dataset
            .iter()
            .cloned()
            .map(|s| s.to_vec())
            .flatten()
            .collect(),
    )?);
    logger.debug(&format!(
        "Subset {subvector_id} convert slice to ndarray duration: {}s",
        dataset_creation_time.elapsed().as_secs()
    ));

    let kmeans_iteration_time = Instant::now();
    let rng = rand::thread_rng();
    let model = KMeans::params_with_rng(cluster_count, rng.clone())
        .tolerance(1e-1)
        .n_runs(1)
        .max_n_iterations(20)
        .fit(&observations)?;

    logger.debug(&format!(
        "Subset {subvector_id} kmeans iteration duration: {}s",
        kmeans_iteration_time.elapsed().as_secs()
    ));

    let centroid_extracton_time = Instant::now();
    let centroids = model
        .centroids()
        .into_iter()
        .cloned()
        .collect::<Vec<f32>>()
        .chunks(dim)
        .map(|s| s.to_vec())
        .collect();
    logger.debug(&format!(
        "Subset {subvector_id} centroid extraction duration: {}s",
        centroid_extracton_time.elapsed().as_secs()
    ));
    Ok(centroids)
}

pub struct CreateCodebookArgs<'a> {
   pub logger: &'a Logger,
   pub main_progress: &'a AtomicU8,
   pub progress_cb: &'a Option<super::ProgressCbFn>,
   pub db_uri: &'a str,
   pub pk: &'a str,
   pub column: &'a str,
   pub full_table_name: &'a str,
   pub codebook_table_name: &'a str,
   pub total_row_count: usize,
   pub max_connections: usize,
   pub splits: usize,
   pub vector_dim: usize,
   pub subvector_dim: usize,
   pub cluster_count: usize,
   pub subvector_id: &'a Option<usize>,
   pub parallel_task_count: &'a Option<usize>,
}

pub fn create_codebook<'a> (
    args: CreateCodebookArgs, transaction: &mut Transaction<'a>)
 -> Result<(HashMap<usize, Vec<Vec<f32>>>, Arc<Vec<DatasetItem>>), anyhow::Error> {

    let logger = args.logger;
    let cluster_count = args.cluster_count;
    let progress_cb = args.progress_cb;
    let main_progress = args.main_progress;
    let codebook_table_name = args.codebook_table_name;
    let total_row_count = args.total_row_count;
    let db_uri = args.db_uri;
    let pk = args.pk;
    let column = args.column;
    let full_table_name = args.full_table_name;
    let splits = args.splits;
    let subvector_id = args.subvector_id;
    let subvector_dim = args.subvector_dim;
    let parallel_task_count = args.parallel_task_count.unwrap_or(splits);
    let max_connections = args.max_connections;
    let vector_dim = args.vector_dim;
    
    let mut subvector_start_idx = 0;
    let mut subvector_end_idx = args.vector_dim;

    if let Some(subvector_id) = subvector_id {
        if *subvector_id >= splits {
            anyhow::bail!(
                "--subvector-id {subvector_id} should be smaller than --splits {}",
                splits
            );
        }
        // If subvector_id is provided
        // That means the job is run in BATCH job, and we should only
        // Process one subvector here not all subvectors
        // So we will take the start and end indices for current subvector_id
        subvector_start_idx = subvector_id * subvector_dim;

        if *subvector_id == splits - 1 {
            subvector_end_idx = vector_dim;
        } else {
            subvector_end_idx = subvector_start_idx + subvector_dim;
        }
    }

    logger.debug(&format!("Splits: {}, Subvector ID: {:?} Vector dim: {}, Subvector dim: {}, Subvector: vector[{subvector_start_idx}:{subvector_end_idx}]", splits, subvector_id, vector_dim, subvector_dim ));

    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let  num_connections: usize = if subvector_id.is_some() {
        // If there's subvector id we expect this to be batch job
        // So each task will get max_connections / split connection pool
        // Be it won't be higher than cpu count
        cmp::min(num_cores, (max_connections - 2) / parallel_task_count)
    } else {
        // In this case as this will be only task running we can use whole connection pool
        let active_connections = transaction.query_one("SELECT COUNT(DISTINCT pid) FROM pg_stat_activity", &[])?;
        let active_connections = active_connections.get::<usize, i64>(0) as usize;
        cmp::min(num_cores, max_connections - active_connections)
    };

    // Avoid division by zero error
    let num_connections = cmp::max(num_connections, 1);
    let chunk_size = total_row_count / num_connections;
    logger.debug(&format!("max_connections: {max_connections}, num_cores: {num_cores}, num_connections: {num_connections}", max_connections = max_connections));

    let total_fetch_start_time = Instant::now();
    // Select all data from database
    // If this is for one subvector, only that portion will be selected from original vectors
    // But if no subvector_id is provided whole vector will be selected
    // (the indices will be 0;vector_dim)
    // Data will be fetched in parallel and then merged to speed up the fetch time
    
    let rows = (0..num_connections)
        .into_par_iter()
        .map(|i| {
            let mut client = Client::connect(db_uri, NoTls)?;
            let mut transaction = client.transaction()?;
            let range_start = i * chunk_size;
            let range_end = if i == num_cores - 1 { total_row_count + 1 } else { range_start + chunk_size };

            let fetch_start_time = Instant::now();
            let rows = transaction.query(
                &format!(
                    "SELECT {pk}::text, {column}[{start_idx}:{end_idx}] FROM {full_table_name} WHERE {pk} >= {range_start} AND {pk} < {range_end} ORDER BY id;",
                    pk = quote_ident(&pk),
                    column = quote_ident(column),
                    start_idx = subvector_start_idx + 1,
                    end_idx = subvector_end_idx + 1,
                ),
                &[],
            )?;
   
            logger.info(&format!(
                "Fetched {} items in {}s",
                rows.len(),
                fetch_start_time.elapsed().as_secs()
            ));
            
            let rows = rows
                .iter()
                .filter_map(|r| {
                    let vec = r.get::<usize, Option<Vec<f32>>>(1);

                    if let Some(v) = vec {
                        Some(DatasetItem {
                            id: r.get::<usize, String>(0),
                            vec: v
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<DatasetItem>>();
            
            Ok::<Vec<DatasetItem>, anyhow::Error>(rows)
        }).collect::<Vec<Result<Vec<DatasetItem>, anyhow::Error>>>();

        let mut dataset: Vec<DatasetItem> = Vec::with_capacity(total_row_count);

        for row in rows {
            for item in row? {
                dataset.push(item);
            }
        }
        
        logger.info(&format!(
            "Fetched {} items in {}s",
            dataset.len(),
            total_fetch_start_time.elapsed().as_secs()
        ));

    // progress indicator is: 5% load, 70% codebook, 15% quantization, 10% export
    report_progress(&progress_cb, &logger, &args.main_progress, 5);

    let mut codebooks_hashmap: HashMap<usize, Vec<Vec<f32>>> = HashMap::new();
    let codebook_creation_start = Instant::now();
    logger.info(&format!(
        "Starting kmeans with params (cluster_count={cluster_count}, subset_count={splits})",
        cluster_count = cluster_count,
        splits = splits
    ));
 
    let dataset = Arc::new(dataset);
    let dataset_clone = dataset.clone();

    // If this is for all subvectors the range will be 0;$splits
    // If this is for one subvector the range will be $subvector_id;($subvector_id+1)
    let subvector_range_start = subvector_start_idx / subvector_dim;
    let subvector_range_end = subvector_end_idx / subvector_dim;
    let subvector_count = subvector_range_end - subvector_range_start;


    let progress_per_chunk = 70.0 / (subvector_count) as f32;
    let all_centroids: Vec<(usize, Vec<Vec<f32>>)> = (subvector_range_start..subvector_range_end)
        .into_par_iter()
        .enumerate()
        .map_with(dataset_clone, |dataset, (i, subvector_id)| {
            let training_time_start = Instant::now();
            let start_index = i * subvector_dim;
            let end_index = start_index + subvector_dim;

            let subset_dataset = dataset
                .iter()
                .map(|r| &r.vec[start_index..end_index])
                .collect::<Vec<&[f32]>>();
            // Prallel iterate over the subvectors and run kmeans returning centroids
            let centroids =
                create_codebook_for_subset(subset_dataset, cluster_count, subvector_id, &logger).unwrap();

            logger.debug(&format!(
                "Subset {subvector_id} training duration: {}s",
                training_time_start.elapsed().as_secs()
            ));

            report_progress(
                &progress_cb,
                &logger,
                &main_progress,
                progress_per_chunk as u8,
            );
            (subvector_id, centroids)
        })
        .collect();

    set_and_report_progress(
        &progress_cb,
        &logger,
        &main_progress,
        75 as u8,
    );
    let codebook_write_time_start = Instant::now();
 
    // Write the generated centroids in codebook table
    let mut writer = transaction.copy_in(&format!("COPY {codebook_table_name} FROM stdin", codebook_table_name = codebook_table_name))?;
    for (subvector_id, centroids) in all_centroids {
        for (centroid_id, centroid) in centroids.iter().enumerate() {
            writer.write(subvector_id.to_string().as_bytes())?;
            writer.write("\t".as_bytes())?;
            writer.write(centroid_id.to_string().as_bytes())?;
            writer.write("\t".as_bytes())?;
            writer.write("{".as_bytes())?;
            let row_str: String = centroid.iter().map(|&x| x.to_string() + ",").collect();
            writer.write(row_str[0..row_str.len() - 1].as_bytes())?;
            writer.write("}".as_bytes())?;
            writer.write("\n".as_bytes())?;
            codebooks_hashmap.insert(subvector_id, centroids.clone());
        }
    }
    
    writer.flush()?;
    writer.finish()?;
 
    logger.debug(&format!(
        "Codebook write duration: {}s",
        codebook_write_time_start.elapsed().as_secs()
    ));

    logger.debug(&format!(
        "Codebook creation duration: {}s",
        codebook_creation_start.elapsed().as_secs()
    ));

    Ok((codebooks_hashmap, dataset))
}
