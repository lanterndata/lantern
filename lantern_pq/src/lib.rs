use lantern_logger::{LogLevel, Logger};
use lantern_utils::{append_params_to_uri, get_full_table_name, quote_ident};
use linfa::traits::Fit;
use linfa::DatasetBase;
use linfa_clustering::KMeans;
use ndarray::Array2;
use rand::Rng;
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use postgres::{Client, NoTls, Transaction};

pub mod cli;

type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

fn report_progress(progress_cb: &Option<ProgressCbFn>, logger: &Logger, progress: u8) {
    logger.info(&format!("Progress {progress}%"));
    if progress_cb.is_some() {
        let cb = progress_cb.as_ref().unwrap();
        cb(progress);
    }
}

struct DatasetItem {
    id: String,
    vec: Vec<f32>,
}

// DB exporter worker will create temp table with name _lantern_tmp_${rand(0,1000)}
// Then it will create writer stream which will COPY bytes from stdin to that table
// After that it will receiver the output embeddings mapped with row ids over the channel
// And write them using writer instance
// At the end we will flush the writer commit the transaction and UPDATE destination table
// Using our TEMP table data
fn write_compressed_rows<'a>(
    mut transaction: Transaction<'a>,
    rows: Vec<(String, Vec<u8>)>,
    schema: &str,
    table: &str,
    column: &str,
    pq_column: &str,
    codebook_table_name: &str,
    distance_metric: &str,
    progress_cb: Option<ProgressCbFn>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let mut rng = rand::thread_rng();
    let full_table_name = get_full_table_name(schema, table);
    let temp_table_name = format!("_lantern_tmp_{}", rng.gen_range(0..1000));

    transaction
            .execute(
                &format!(
                    "CREATE TEMPORARY TABLE {temp_table_name} AS SELECT ctid::TEXT as id, '{{}}'::PQVEC AS {pq_column} FROM {full_table_name} LIMIT 0",
                    pq_column = quote_ident(pq_column)
                ),
                &[],
            )?;

    let mut writer = transaction.copy_in(&format!("COPY {temp_table_name} FROM stdin"))?;
    let update_sql = &format!("UPDATE {full_table_name} dest SET {pq_column} = src.{pq_column} FROM {temp_table_name} src WHERE src.id::tid = dest.ctid", pq_column = quote_ident(pq_column), temp_table_name = quote_ident(&temp_table_name));

    let mut old_progress = 0;

    let mut processed_row_cnt = 0;
    let total_row_cnt = rows.len();

    for row in &rows {
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
            let progress = (5.0 * (processed_row_cnt as f32 / total_row_cnt as f32)) as u8;

            if progress > old_progress {
                old_progress = progress;
                // Max 95% progress from this task, starting from 90%
                report_progress(&progress_cb, &logger, 90 + progress);
            }
        }
    }

    if processed_row_cnt == 0 {
        return Ok(());
    }

    writer.flush()?;
    writer.finish()?;
    transaction.execute(update_sql, &[])?;

    // Setup triggers for new data
    let name_hash = md5::compute(format!("{}{}", full_table_name, pq_column));
    let insert_trigger_name = format!("_pq_trigger_in_{:x}", name_hash);
    let update_trigger_name = format!("_pq_trigger_up_{:x}", name_hash);
    let trigger_fn_name = format!("_set_pq_col_{:x}", name_hash);
    let splits = rows[0].1.len();

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
    logger.info("Commiting transaction to database");
    transaction.commit()?;

    if old_progress != 100 {
        report_progress(&progress_cb, &logger, 100);
    }
    logger.info(&format!("Vectors exported under column {pq_column}",));

    Ok(())
}

fn create_codebook(
    dataset: Vec<&[f32]>,
    cluster_count: usize,
) -> Result<Vec<Vec<f32>>, anyhow::Error> {
    let dim = dataset[0].len();
    let observations = DatasetBase::from(Array2::from_shape_vec(
        (dataset.len(), dim),
        dataset
            .iter()
            .cloned()
            .map(|s| s.to_vec())
            .flatten()
            .collect(),
    )?);

    let rng = rand::thread_rng();
    let model = KMeans::params_with_rng(cluster_count, rng.clone())
        .tolerance(1e-1)
        .n_runs(1)
        .max_n_iterations(20)
        .fit(&observations)?;

    let centroids = model
        .centroids()
        .into_iter()
        .cloned()
        .collect::<Vec<f32>>()
        .chunks(dim)
        .map(|s| s.to_vec())
        .collect();
    Ok(centroids)
}

fn l2sq_dist(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| ((*x) - (*y)) * ((*x) - (*y)))
        .fold(0.0 as f32, ::std::ops::Add::add)
}

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

pub fn quantize_table(
    args: &cli::PQArgs,
    progress_cb: Option<ProgressCbFn>,
    is_canceled: Option<Arc<RwLock<bool>>>,
    logger: Option<Logger>,
) -> AnyhowVoidResult {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern PQ", LogLevel::Debug)));
    logger.info("Lantern CLI - Quantize Table");

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
    transaction.execute("SET lock_timeout='5s'", &[])?;
    transaction.execute(
        &format!("LOCK TABLE ONLY {full_table_name} IN SHARE MODE"),
        &[],
    )?;

    transaction.batch_execute(&format!(
        "
             CREATE TABLE {codebook_table_name} (subvector_id INT, centroid_id INT, c REAL[]);
             ALTER TABLE {full_table_name} ADD COLUMN {pq_column_name} PQVEC;
             CREATE INDEX ON {codebook_table_name} USING BTREE(subvector_id, centroid_id);
        ",
        codebook_table_name = quote_ident(&codebook_table_name),
        pq_column_name = quote_ident(&pq_column_name)
    ))?;

    let rows = transaction.query(
        &format!(
            "SELECT ctid::text, {column} FROM {full_table_name} WHERE {column} IS NOT NULL;",
            column = quote_ident(column),
        ),
        &[],
    )?;

    let rows = rows
        .iter()
        .map(|r| DatasetItem {
            id: r.get::<usize, String>(0),
            vec: r.get::<usize, Vec<f32>>(1),
        })
        .collect::<Vec<DatasetItem>>();
    // 5% load, 70% codebook, 15% compression, 10% export
    logger.info(&format!("Fetched {} items", rows.len()));
    report_progress(&progress_cb, &logger, 5);
    let vector_dim = rows[0].vec.len();
    let subvector_dim = vector_dim / args.splits;

    let mut codebooks_hashmap: HashMap<usize, Vec<Vec<f32>>> = HashMap::new();
    let codebook_creation_start = Instant::now();
    logger.info(&format!(
        "Starting kmeans with params (clouster_count={}, subset_count={})",
        args.clusters, args.splits
    ));
    for i in 0..args.splits {
        let training_time_start = Instant::now();
        let start_index = i * subvector_dim;
        let mut end_index = start_index + subvector_dim;

        if end_index >= vector_dim {
            end_index = start_index + (vector_dim - start_index);
        }

        let subset_dataset = rows
            .iter()
            .map(|r| &r.vec[start_index..end_index])
            .collect::<Vec<&[f32]>>();

        let centroids = create_codebook(subset_dataset, args.clusters)?;

        logger.debug(&format!(
            "Subset {i} training duration: {}s",
            training_time_start.elapsed().as_secs()
        ));

        let training_time_start = Instant::now();
        for (centroid_id, centroid) in centroids.iter().enumerate() {
            transaction.execute(
                &format!("INSERT INTO {codebook_table_name} (subvector_id, centroid_id, c) VALUES ($1, $2, $3)",codebook_table_name=quote_ident(&codebook_table_name)),
                &[&(i as i32), &(centroid_id as i32), &centroid],
            )?;
        }

        codebooks_hashmap.insert(i, centroids);
        logger.debug(&format!(
            "Subset {i} codebook export duration: {}ms",
            training_time_start.elapsed().as_millis()
        ));

        // Max 70% progress from this task, starting from 5%
        report_progress(
            &progress_cb,
            &logger,
            5 + (70.0 * ((i + 1) as f32 / args.splits as f32)) as u8,
        );

        if *is_canceled.read().unwrap() {
            // This variable will be changed from outside to gracefully
            // exit job on next chunk
            anyhow::bail!("Job canceled");
        }
    }
    logger.debug(&format!(
        "Codebook creation duration: {}s",
        codebook_creation_start.elapsed().as_secs()
    ));

    let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));

    let now = Instant::now();
    let rows: Vec<_> = rows
        .into_par_iter()
        .map_with(codebooks_hashmap, |s, x| {
            (
                x.id.clone(),
                (0..args.splits)
                    .map(|i| {
                        let map = s.read().unwrap();
                        let split_centroids = map.get(&i).unwrap();
                        let start_index = i * subvector_dim;
                        let mut end_index = start_index + subvector_dim;

                        if end_index >= vector_dim {
                            end_index = start_index + (vector_dim - start_index);
                        }

                        get_closest_centroid(split_centroids, &x.vec[start_index..end_index])
                    })
                    .collect::<Vec<u8>>(),
            )
        })
        .collect();
    report_progress(&progress_cb, &logger, 90);

    logger.debug(&format!(
        "Vector compression duration: {}s",
        now.elapsed().as_secs()
    ));

    if *is_canceled.read().unwrap() {
        // This variable will be changed from outside to gracefully
        // exit job on next chunk
        anyhow::bail!("Job canceled");
    }

    let export_time_start = Instant::now();
    write_compressed_rows(
        transaction,
        rows,
        &args.schema,
        &args.table,
        &column,
        &pq_column_name,
        &codebook_table_name,
        "l2sq", // TODO:: get from args
        progress_cb,
        logger.clone(),
    )?;
    logger.debug(&format!(
        "Vector export duration: {}s",
        export_time_start.elapsed().as_secs()
    ));
    logger.debug(&format!(
        "Total duration: {}s",
        total_time_start.elapsed().as_secs()
    ));
    Ok(())
}
