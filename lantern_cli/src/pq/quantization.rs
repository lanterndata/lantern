use crate::logger::Logger;
use crate::utils::{get_full_table_name, quote_ident};
use postgres::{Client, NoTls, Transaction};
use rand::Rng;
use rayon::prelude::*;
use std::cmp;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use super::{
    report_progress, set_and_report_progress, AnyhowVoidResult, DatasetItem, ProgressCbFn,
};

fn l2sq_dist(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| ((*x) - (*y)) * ((*x) - (*y)))
        .fold(0.0 as f32, ::std::ops::Add::add)
}

// Will iterate over all clusters and search the closes centroid to provided vector
fn get_closest_centroid(centroids: &Vec<Vec<f32>>, subvector: &[f32]) -> u8 {
    let mut closest_distance = f32::MAX;
    let mut closest_index = 0;

    for (idx, centroid) in centroids.iter().enumerate() {
        let distance = l2sq_dist(&centroid, subvector);
        if distance < closest_distance {
            closest_distance = distance;
            closest_index = idx as u8;
        }
    }

    closest_index
}

// Will parallel iterate over the dataset
// Then iterate over each subvector of the vector and return
// closest centroid id for that subvector
// Result will be vector with row id and quantized vector
pub fn quantize_vectors(
    dataset: &Vec<DatasetItem>,
    vector_dim: usize,
    subvector_dim: usize,
    splits: usize,
    codebooks_hashmap: Arc<RwLock<HashMap<usize, Vec<Vec<f32>>>>>,
    logger: &Logger,
) -> Result<Vec<(String, Vec<u8>)>, anyhow::Error> {
    let quantization_start = Instant::now();
    let rows: Vec<_> = dataset
        .iter()
        .map(|r| r.clone())
        .collect::<Vec<DatasetItem>>()
        .into_par_iter()
        .map_with(codebooks_hashmap, |s, x| {
            (
                x.id.clone(),
                (0..splits)
                    .map(|i| {
                        let map = s.read().unwrap();
                        let split_centroids = map.get(&i).unwrap();
                        let start_index = i * subvector_dim;
                        let end_index = cmp::min(start_index + subvector_dim, vector_dim);
                        get_closest_centroid(split_centroids, &x.vec[start_index..end_index])
                    })
                    .collect::<Vec<u8>>(),
            )
        })
        .collect();

    logger.debug(&format!(
        "Vector quantization duration: {}s",
        quantization_start.elapsed().as_secs()
    ));
    Ok(rows)
}

// This function will write quantized vector into temporary table
// Using COPY protocol and then update the original table via pk mapping
// So we will use only one UPDATE query to write quantized vectors
// This function can be run in parallel
pub fn write_quantized_rows<'a>(
    transaction: &mut Transaction<'a>,
    rows: &Vec<(String, Vec<u8>)>,
    schema: &str,
    table: &str,
    pq_column: &str,
    pk: &str,
    tmp_table_suffix: &str,
    main_progress: &AtomicU8,
    progress_cb: &Option<ProgressCbFn>,
    logger: &Logger,
) -> AnyhowVoidResult {
    let mut rng = rand::thread_rng();
    let full_table_name = get_full_table_name(schema, table);
    let temp_table_name = format!("_pq_tmp_{tmp_table_suffix}_{}", rng.gen_range(0..1000000));
    let export_time_start = Instant::now();

    transaction
            .execute(
                &format!(
                    "CREATE TEMPORARY TABLE {temp_table_name} AS SELECT {pk} as id, '{{1}}'::PQVEC AS {pq_column} FROM {full_table_name} LIMIT 0",
                    pq_column = quote_ident(pq_column),
                    pk = quote_ident(pk)
                ),
                &[],
            )?;

    let mut writer = transaction.copy_in(&format!("COPY {temp_table_name} FROM stdin"))?;
    let update_sql = &format!("UPDATE {full_table_name} dest SET {pq_column} = src.{pq_column} FROM {temp_table_name} src WHERE src.id = dest.{pk}", pq_column = quote_ident(pq_column), temp_table_name = quote_ident(&temp_table_name), pk = quote_ident(pk));

    let mut processed_row_cnt = 0;
    let total_row_cnt = rows.len();

    for row in rows {
        writer.write(row.0.as_bytes())?;
        writer.write("\t".as_bytes())?;
        writer.write("{".as_bytes())?;
        let row_str: String = row.1.iter().map(|&x| x.to_string() + ",").collect();
        writer.write(row_str[0..row_str.len() - 1].as_bytes())?;
        drop(row_str);
        writer.write("}".as_bytes())?;
        writer.write("\n".as_bytes())?;
        processed_row_cnt += 1;

        if processed_row_cnt % 1000 == 0 {
            // Max 5% progress from this task
            let progress = (5.0 * (processed_row_cnt as f32 / total_row_cnt as f32)) as u8;

            if progress > main_progress.load(Ordering::SeqCst) {
                report_progress(&progress_cb, &logger, main_progress, progress);
            }
        }
    }

    if processed_row_cnt == 0 {
        return Ok(());
    }

    writer.flush()?;
    writer.finish()?;
    transaction.execute(update_sql, &[])?;

    logger.info(&format!("Vectors exported under column {pq_column}",));
    logger.debug(&format!(
        "Vector export duration: {}s",
        export_time_start.elapsed().as_secs()
    ));

    Ok(())
}
// This function is intended to be run on batch job
// It is optimized for parallel runs
// The data read/write will be done in parallel using rayon
// It can operate over range of data from the whole table,
// so it can be split over multiple vm instances to speed up quantization times
pub struct QuantizeAndWriteVectorArgs<'a> {
    pub codebook_table_name: &'a str,
    pub full_table_name: &'a str,
    pub db_uri: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub column: &'a str,
    pub pq_column_name: &'a str,
    pub pk: &'a str,
    pub splits: usize,
    pub total_row_count: usize,
    pub total_task_count: &'a Option<usize>,
    pub parallel_task_count: &'a Option<usize>,
    pub quantization_task_id: &'a Option<usize>,
    pub max_connections: usize,
    pub main_progress: &'a AtomicU8,
    pub progress_cb: &'a Option<super::ProgressCbFn>,
    pub logger: &'a Logger,
}

pub fn quantize_and_write_vectors(
    args: QuantizeAndWriteVectorArgs,
    mut client: Client,
) -> super::AnyhowVoidResult {
    let mut transaction = client.transaction()?;
    let logger = args.logger;
    let db_uri = args.db_uri;
    let full_table_name = args.full_table_name;
    let full_codebook_table_name = args.codebook_table_name;
    let column = args.column;
    let splits = args.splits;
    let schema = args.schema;
    let table = args.table;
    let pq_column_name = args.pq_column_name;
    let pk = args.pk;
    let main_progress = args.main_progress;
    let progress_cb = args.progress_cb;

    let mut limit_start = 0;
    let mut limit_end = args.total_row_count;

    // In batch mode each task will operate on a range of vectors from dataset
    // Here we will determine the range from the task id
    if let Some(quantization_task_id) = args.quantization_task_id {
        if args.total_task_count.is_none() {
            anyhow::bail!(
                "Please provide --total-task-count when providing --quantization-task-id"
            );
        }
        let quantization_task_count = args.total_task_count.as_ref().unwrap();

        let chunk_per_task = limit_end / quantization_task_count;
        limit_start = chunk_per_task * quantization_task_id;
        limit_end = if *quantization_task_id == quantization_task_count - 1 {
            limit_end + 1
        } else {
            limit_start + chunk_per_task
        };
    }

    // Read all codebook and create a hashmap from it
    let codebook_read_start = Instant::now();
    let codebook_rows = transaction.query(
        &format!(
            "SELECT subvector_id, centroid_id, c FROM {full_codebook_table_name} ORDER BY centroid_id ASC;"
        ),
        &[],
    )?;

    if codebook_rows.len() == 0 {
        anyhow::bail!("Codebook does not contain any entries");
    }

    logger.debug(&format!(
        "Coedbook fetched in {}s",
        codebook_read_start.elapsed().as_secs()
    ));

    let mut codebooks_hashmap: HashMap<usize, Vec<Vec<f32>>> = HashMap::new();
    let cluster_count = codebook_rows.len() / splits;

    let codebook_hashmap_creation_start = Instant::now();
    let subvector_dim = codebook_rows[0].get::<usize, Vec<f32>>(2).len();
    // Create hashmap from codebook
    // The hashmap will contain { [subvector_id]: Vec<f32> }
    for row in codebook_rows {
        let subvector_id = row.get::<usize, i32>(0) as usize;
        let centroid_id = row.get::<usize, i32>(1) as usize;
        let centroid = row.get::<usize, Vec<f32>>(2);
        let subvector_codebook = codebooks_hashmap
            .entry(subvector_id)
            .or_insert(Vec::with_capacity(cluster_count));
        subvector_codebook.insert(centroid_id, centroid);
    }

    if codebooks_hashmap.len() != splits {
        anyhow::bail!(
            "Incomplete codebook: expected size equal to {}, got: {}",
            splits,
            codebooks_hashmap.len()
        );
    }

    logger.debug(&format!(
        "Coedbook hashmap created in {}s",
        codebook_hashmap_creation_start.elapsed().as_secs()
    ));
    set_and_report_progress(progress_cb, logger, main_progress, 10);

    let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));

    // Here we will read the range of data for this chunk in parallel
    // Based on total task count and machine CPU count
    // Then we will quantize the range chunk and write to database
    let range_row_count = limit_end - limit_start;
    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let num_connections: usize = if args.quantization_task_id.is_some() {
        // This will never fail as it is checked on start to be specified if task id is present
        let parallel_task_count = args
            .parallel_task_count
            .as_ref()
            .unwrap_or(args.total_task_count.as_ref().unwrap());
        // If there's quantization task id we expect this to be batch job
        // So each task will get (max_connections / parallel task count) connection pool
        // But it won't be higher than cpu count
        cmp::min(num_cores, (args.max_connections - 2) / parallel_task_count)
    } else {
        // In this case as this will be only task running we can use whole connection pool
        let active_connections =
            transaction.query_one("SELECT COUNT(DISTINCT pid) FROM pg_stat_activity", &[])?;
        let active_connections = active_connections.get::<usize, i64>(0) as usize;
        cmp::min(num_cores, args.max_connections - active_connections)
    };

    // Avoid division by zero error
    let num_connections = cmp::max(num_connections, 1);
    let chunk_size = range_row_count / num_connections;

    logger.debug(&format!("max_connections: {}, num_cores: {num_cores}, num_connections: {num_connections}, chunk_count: {chunk_size}", args.max_connections));

    let quantization_and_write_start_time = Instant::now();

    let results = (0..num_connections)
        .into_par_iter()
        .map_with(codebooks_hashmap, |map, i| {
            let mut client = Client::connect(&db_uri, NoTls)?;
            let mut transaction = client.transaction()?;
            let range_start = limit_start + (i * chunk_size);
            let range_end = if i == num_cores - 1 { limit_end } else { range_start + chunk_size };

            let fetch_start_time = Instant::now();
            let rows = transaction.query(
                &format!(
            "SELECT id::text, {column} FROM {full_table_name} WHERE id >= {range_start} AND id < {range_end} ORDER BY id;",
            full_table_name = full_table_name,
            column = quote_ident(column),
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
            let vector_dim = rows[0].vec.len();
            let rows = quantize_vectors(
                &rows,
                vector_dim,
                subvector_dim,
                splits,
                map.clone(),
                &logger,
            )?;

            write_quantized_rows(
                &mut transaction,
                &rows,
                schema,
                table,
                pq_column_name,
                pk,
                &range_start.to_string(),
                &main_progress,
                progress_cb,
                &logger,
            )?;
            transaction.commit()?;
            Ok::<(), anyhow::Error>(())
        }).collect::<Vec<Result<(), anyhow::Error>>>();

    for result in results {
        result?;
    }

    logger.debug(&format!(
        "Vectors quantized and exported in {}s",
        quantization_and_write_start_time.elapsed().as_secs()
    ));
    transaction.commit()?;
    Ok(())
}
