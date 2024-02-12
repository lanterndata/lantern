use lantern_logger::{LogLevel, Logger};
use lantern_utils::{append_params_to_uri, get_full_table_name, quote_ident};
use linfa::traits::Fit;
use linfa::DatasetBase;
use linfa_clustering::KMeans;
use ndarray::Array2;
use rand::Rng;
use rayon::prelude::*;
use std::cmp;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use postgres::{Client, NoTls, Transaction};

pub mod cli;

type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

fn report_progress(
    progress_cb: &Option<ProgressCbFn>,
    logger: &Logger,
    old_progress: &AtomicU8,
    progress: u8,
) {
    old_progress.fetch_add(progress, Ordering::SeqCst);
    let new_progress = old_progress.load(Ordering::SeqCst);
    logger.info(&format!("Progress {new_progress}%"));
    if progress_cb.is_some() {
        let cb = progress_cb.as_ref().unwrap();
        cb(new_progress);
    }
}

fn set_and_report_progress(
    progress_cb: &Option<ProgressCbFn>,
    logger: &Logger,
    old_progress: &AtomicU8,
    progress: u8,
) {
    let old_progress_value = old_progress.load(Ordering::SeqCst);
    if old_progress_value >= progress {
        return;
    }

    let diff = progress - old_progress_value;
    report_progress(progress_cb, logger, old_progress, diff);
}

#[derive(Clone, Debug)]
struct DatasetItem {
    id: String,
    vec: Vec<f32>,
}

fn setup_triggers<'a>(
    transaction: &mut Transaction<'a>,
    full_table_name: &str,
    codebook_table_name: &str,
    pq_column: &str,
    column: &str,
    distance_metric: &str,
    splits: usize,
) -> AnyhowVoidResult {
    // Setup triggers for new data
    let name_hash = md5::compute(format!("{}{}", full_table_name, pq_column));
    let insert_trigger_name = format!("_pq_trigger_in_{:x}", name_hash);
    let update_trigger_name = format!("_pq_trigger_up_{:x}", name_hash);
    let trigger_fn_name = format!("_set_pq_col_{:x}", name_hash);

    transaction.batch_execute(&format!("
      DROP TRIGGER IF EXISTS {insert_trigger_name} ON {full_table_name};
      DROP TRIGGER IF EXISTS {update_trigger_name} ON {full_table_name};

      CREATE OR REPLACE FUNCTION {trigger_fn_name}()
          RETURNS trigger
          LANGUAGE plpgsql AS
      $body$
        BEGIN
          IF NEW.{column} IS NULL THEN
            NEW.{pq_column} := NULL;
          ELSE
            NEW.{pq_column} := _lantern_internal.compress_vector(NEW.{column}, {splits}, {codebook_table_name}::regclass, '{distance_metric}');
          END IF;
          RETURN NEW;
        END
      $body$;

      CREATE TRIGGER {insert_trigger_name} BEFORE INSERT ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {trigger_fn_name}();
      CREATE TRIGGER {update_trigger_name} BEFORE UPDATE OF {column} ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {trigger_fn_name}();

    ", pq_column=quote_ident(pq_column), column=quote_ident(column), codebook_table_name=quote_ident(codebook_table_name)))?;
    Ok(())
}

// This function will write compressed vector into temporary table
// Using COPY protocol and then update the original table via pk mapping
// So we will use only one UPDATE query to write compressed vectors
// This function can be run in parallel
fn write_compressed_rows<'a>(
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
    let temp_table_name = format!("_lantern_pq_tmp_{tmp_table_suffix}_{}", rng.gen_range(0..1000000));
    let export_time_start = Instant::now();

    transaction
            .execute(
                &format!(
                    "CREATE TEMPORARY TABLE {temp_table_name} AS SELECT {pk} as id, '{{}}'::PQVEC AS {pq_column} FROM {full_table_name} LIMIT 0",
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

// Will run kmeans over dataset and return centroids
fn create_codebook(
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

// Will create a codebook table add neccessary indexes and add PQVEC column into target table
fn _setup_tables<'a>(
    transaction: &mut Transaction<'a>,
    full_table_name: &str,
    codebook_table_name: &str,
    pq_column_name: &str,
    logger: &Logger,
) -> AnyhowVoidResult {
    transaction.batch_execute(&format!(
        "
             CREATE TABLE {codebook_table_name} (subvector_id INT, centroid_id INT, c REAL[]);
             ALTER TABLE {full_table_name} ADD COLUMN {pq_column_name} PQVEC;
             CREATE INDEX ON {codebook_table_name} USING BTREE(subvector_id, centroid_id);
             CREATE INDEX ON {codebook_table_name} USING BTREE(centroid_id);
        ",
        codebook_table_name = quote_ident(&codebook_table_name),
        pq_column_name = quote_ident(&pq_column_name)
    ))?;
    logger.info(&format!(
        "{codebook_table_name} table and {pq_column_name} column created successfully"
    ));
    Ok(())
}

// Will parallel iterate over the dataset
// Then iterate over each subvector of the vector and return
// closest centroid id for that subvector
// Result will be vector with row id and compressed vector 
fn _compress_vectors(
    dataset: &Vec<DatasetItem>,
    vector_dim: usize,
    subvector_dim: usize,
    splits: usize,
    codebooks_hashmap: Arc<RwLock<HashMap<usize, Vec<Vec<f32>>>>>,
    logger: &Logger,
) -> Result<Vec<(String, Vec<u8>)>, anyhow::Error> {
    let compression_start = Instant::now();
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
        "Vector compression duration: {}s",
        compression_start.elapsed().as_secs()
    ));
    Ok(rows)
}

// This function is intended to be run on batch job
// It is optimized for parallel runs
// The data read/write will be done in parallel using rayon
// It can operate over range of data from the whole table, 
// so it can be split over multiple vm instances to speed up compression times
fn compress_and_write_vectors<'a>(
    mut client: Client,
    codebook_table_name: &str,
    full_table_name: &str,
    db_uri: &str,
    schema: &str,
    table: &str,
    column: &str,
    pq_column_name: &str,
    pk: &str,
    splits: usize,
    limit_start: usize,
    limit_end: usize,
    main_progress: &AtomicU8,
    progress_cb: &Option<ProgressCbFn>,
    logger: &Logger,
) -> AnyhowVoidResult {
    let mut transaction = client.transaction()?;

    let codebook_read_start = Instant::now();
    let codebook_rows = transaction.query(
        &format!(
            "SELECT subvector_id, centroid_id, c FROM {codebook_table_name} ORDER BY centroid_id ASC;",
            codebook_table_name = quote_ident(&codebook_table_name),
        ),
        &[],
    )?;

    if codebook_rows.len() == 0 {
        anyhow::bail!("Codebook does not contain any entries");
    }

    logger.debug(&format!("Coedbook fetched in {}s", codebook_read_start.elapsed().as_secs()));

    let mut codebooks_hashmap: HashMap<usize, Vec<Vec<f32>>> = HashMap::new();
    let cluster_count = codebook_rows.len() / splits;

    let codebook_hashmap_creation_start = Instant::now();
    let subvector_dim = codebook_rows[0].get::<usize, Vec<f32>>(2).len();
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
            "Incomplete codebook: expected size equal to {splits}, got: {}",
            codebooks_hashmap.len()
        );
    }

    logger.debug(&format!("Coedbook hashmap created in {}s", codebook_hashmap_creation_start.elapsed().as_secs()));
    set_and_report_progress(progress_cb, logger, main_progress, 10);

    let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));
 
    let row_count = limit_end - limit_start;
    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let chunk_count = row_count / num_cores;

    let compression_and_write_start_time = Instant::now();
    let results = (0..num_cores)
        .into_par_iter()
        .map_with(codebooks_hashmap, |map, i| {
            let mut client = Client::connect(&db_uri, NoTls)?;
            let mut transaction = client.transaction()?;
            let range_start = limit_start + (i * chunk_count);
            let range_end = if i == num_cores - 1 { limit_end + 1 } else { range_start + chunk_count + 1 };

            let fetch_start_time = Instant::now();
            let rows = transaction.query(
                &format!(
            "SELECT id::text, {column} FROM {full_table_name} WHERE id > {range_start} AND id < {range_end} ORDER BY id;",
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
            let rows = _compress_vectors(
                &rows,
                vector_dim,
                subvector_dim,
                splits,
                map.clone(),
                &logger,
            )?;
            
            write_compressed_rows(
                &mut transaction,
                &rows,
                schema,
                table,pq_column_name,
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

    logger.debug(&format!("Vectors compressed and exported in {}s", compression_and_write_start_time.elapsed().as_secs()));
    transaction.commit()?;
    Ok(())
}

// This code can be used in 2 modes
// The first one is to quantize the whole table for all subvectors
// In this mode whole vectors will be fetched from the table and kmeans will be run for all
// subvectors then codebook will be created, vectors will be compressed and written to table
//
// The second mode is meant to horizontally scale this job, so only one subvector will be fetched
// for the job and codebook will be created for that subvector
// Then separate job will be run to compress vectors and write to table

pub fn quantize_table(
    args: &cli::PQArgs,
    progress_cb: Option<ProgressCbFn>,
    is_canceled: Option<Arc<RwLock<bool>>>,
    logger: Option<Logger>,
) -> AnyhowVoidResult {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern PQ", LogLevel::Debug)));
    logger.info("Lantern CLI - Quantize Table");

    let main_progress = AtomicU8::new(0);
    let is_canceled = is_canceled.unwrap_or(Arc::new(RwLock::new(false)));
    let total_time_start = Instant::now();
    let column = &args.column;
    let schema = &args.schema;
    let table = &args.table;
    let full_table_name = get_full_table_name(schema, table);
    let codebook_table_name = format!("_lantern_codebook_{}", args.table);
    let pq_column_name = format!("{column}_pq");

    let uri = append_params_to_uri(&args.uri, CONNECTION_PARAMS);
    let mut client = Client::connect(&uri, NoTls)?;
    let mut transaction = client.transaction()?;


    // Create codebook table and add pqvec column to table
    if !args.skip_table_setup {
        _setup_tables(
            &mut transaction,
            &full_table_name,
            &codebook_table_name,
            &pq_column_name,
            &logger,
        )?;
        setup_triggers(&mut transaction, &full_table_name, &codebook_table_name, &pq_column_name, column, "l2sq", args.splits)?;

        // Commit and return if the task is to only set up tables
        if args.only_setup {
            transaction.commit()?;
            set_and_report_progress(&progress_cb, &logger, &main_progress, 100);
            return Ok(());
        }
    }

    let row_cnt = transaction.query_one(
        &format!(
            "SELECT COUNT({pk}) FROM {full_table_name};",
            pk = quote_ident(&args.pk)
        ),
        &[],
    )?;

    let row_cnt = row_cnt.try_get::<usize, i64>(0)? as usize;

    // Only compress will be passed if task is run from Batch job
    // As there will be three phases 
    // 1. table setup, 2. codebook craetion 3. table compression 4. trigger setup
    // 2 and 3 phases will be run in parallel
    if args.only_compress {
        let mut limit_start = 0;
        let mut limit_end = row_cnt ;

        if let Some(compression_task_id) = args.compression_task_id {
        if args.compression_task_count.is_none() {
            anyhow::bail!("Please provide --compression-task-count when providing --compression-task-id");
        }
        let compression_task_count = args.compression_task_count.unwrap();
        
        let chunk_per_task = limit_end / compression_task_count;
        limit_start = chunk_per_task * compression_task_id;
        limit_end = if compression_task_id == compression_task_count - 1 { limit_end } else { limit_start + chunk_per_task };
        }

        drop(transaction);
        compress_and_write_vectors(
            client,
            &codebook_table_name,
            &full_table_name,
            &uri,
            schema,
            table,
            column,
            &pq_column_name,
            &args.pk,
            args.splits,
            limit_start,
            limit_end,
            &main_progress,
            &progress_cb,
            &logger,
        )?;
        set_and_report_progress(&progress_cb, &logger, &main_progress, 100);
        return Ok(());
    }

    // Get full vector dimension
    let row = transaction.query_one(
        &format!(
            "SELECT ARRAY_LENGTH({column}, 1) FROM {full_table_name} WHERE {column} IS NOT NULL LIMIT 1;",
            column = quote_ident(column)
        ),
        &[],
    )?;
    let vector_dim = row.get::<usize, i32>(0) as usize;
    // Get subvector dimension
    // It is not neccessary that vector_dim will be divisible to split count
    // If there's reminder the last subvector's dimensions will be higher
    // But it is better to provide a value which is divisible
    let subvector_dim = vector_dim / args.splits;

    let mut subvector_start_idx = 0;
    let mut subvector_end_idx = vector_dim;

    if let Some(subvector_id) = args.subvector_id {
        if subvector_id >= args.splits {
            anyhow::bail!(
                "--subvector-id {subvector_id} should be smaller than --splits {}",
                args.splits
            );
        }
        // If subvector_id is provided
        // That means the job is run in BATCH job, and we should only
        // Process one subvector here not all subvectors
        // So we will take the start and end indices for current subvector_id
        subvector_start_idx = subvector_id * subvector_dim;

        if subvector_id == args.splits - 1 {
            subvector_end_idx = vector_dim;
        } else {
            subvector_end_idx = subvector_start_idx + subvector_dim;
        }
    }

    logger.debug(&format!("Splits: {}, Subvector ID: {:?} Vector dim: {vector_dim}, Subvector dim: {subvector_dim}, Subvector: vector[{subvector_start_idx}:{subvector_end_idx}]", args.splits, args.subvector_id));
    let max_connections = transaction.query_one("SELECT setting::int FROM pg_settings WHERE name = 'max_connections'", &[])?;
    let max_connections = max_connections.get::<usize, i32>(0) as usize;

    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let  num_connections: usize = if args.subvector_id.is_some() {
        // If there's subvector id we expect this to be batch job
        // So each task will get max_connections / split connection pool
        // Be it won't be higher than cpu count
        cmp::min(num_cores, (max_connections - 2) / args.splits)
    } else {
        // In this case as this will be only task running we can use whole connection pool
        let active_connections = transaction.query_one("SELECT COUNT(DISTINCT pid) FROM pg_stat_activity", &[])?;
        let active_connections = active_connections.get::<usize, i64>(0) as usize;
        cmp::min(num_cores, max_connections - active_connections)
    };

    let chunk_count = row_cnt / num_connections;
    logger.debug(&format!("max_connections: {max_connections}, num_cores: {num_cores}, num_connections: {num_connections}"));

    let total_fetch_start_time = Instant::now();
    // Select all data from database
    // If this is for one subvector, only that portion will be selected from original vectors
    // But if no subvector_id is provided whole vector will be selected
    // (the indices will be 0;vector_dim)
    // Data will be fetched in parallel and then merged to speed up the fetch time
    let rows = (0..num_connections)
        .into_par_iter()
        .map(|i| {
            let mut client = Client::connect(&uri, NoTls)?;
            let mut transaction = client.transaction()?;
            let range_start = i * chunk_count;
            let range_end = if i == num_cores - 1 { row_cnt + 1 } else { range_start + chunk_count + 1 };

            let fetch_start_time = Instant::now();
            let rows = transaction.query(
                &format!(
                    "SELECT {pk}::text, {column}[{start_idx}:{end_idx}] FROM {full_table_name} WHERE {pk} > {range_start} AND {pk} < {range_end} ORDER BY id;",
                    pk = quote_ident(&args.pk),
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

        let mut dataset: Vec<DatasetItem> = Vec::with_capacity(row_cnt);

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

    // progress indicator is: 5% load, 70% codebook, 15% compression, 10% export
    report_progress(&progress_cb, &logger, &main_progress, 5);

    let mut codebooks_hashmap: HashMap<usize, Vec<Vec<f32>>> = HashMap::new();
    let codebook_creation_start = Instant::now();
    logger.info(&format!(
        "Starting kmeans with params (clouster_count={}, subset_count={})",
        args.clusters, args.splits
    ));
 
    let dataset = Arc::new(dataset);
    let dataset_clone = dataset.clone();

    // If this is for all subvectors the range will be 0;$args.splits
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
                create_codebook(subset_dataset, args.clusters, subvector_id, &logger).unwrap();

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
    let mut writer = transaction.copy_in(&format!("COPY {codebook_table_name} FROM stdin"))?;
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

    // Compress vectors using codebook
    // And write results to target table
    if !args.skip_vector_compression {
        let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));

        let dataset = _compress_vectors(
            &dataset,
            vector_dim,
            subvector_dim,
            args.splits,
            codebooks_hashmap,
            &logger,
        )?;
        set_and_report_progress(&progress_cb, &logger, &main_progress, 90);

        if *is_canceled.read().unwrap() {
            // This variable will be changed from outside to gracefully
            // exit job on next chunk
            anyhow::bail!("Job canceled");
        }

        write_compressed_rows(
            &mut transaction,
            &dataset,
            &args.schema,
            &args.table,
            &pq_column_name,
            &args.pk,
            "compress",
            &main_progress,
            &progress_cb,
            &logger,
        )?;
    }

    transaction.commit()?;

    set_and_report_progress(&progress_cb, &logger, &main_progress, 100);

    logger.debug(&format!(
        "Total duration: {}s",
        total_time_start.elapsed().as_secs()
    ));

    Ok(())
}
