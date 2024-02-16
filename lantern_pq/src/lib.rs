use codebook::CreateCodebookArgs;
use compression::CompressAndWriteVectorArgs;
use lantern_logger::{LogLevel, Logger};
use lantern_utils::{append_params_to_uri, get_full_table_name, quote_ident};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use postgres::{Client, NoTls};

pub mod cli;
mod codebook;
mod compression;
mod gcp_batch;
mod setup;

type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

// This function will increment current progress and report it
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

// This function will set current progress to the specified one
// If it is greater then current one
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

// This code can be used in 2 modes
// The first one is to quantize the whole table for all subvectors
// In this mode whole vectors will be fetched from the table and kmeans will be run for all
// subvectors then codebook will be created, vectors will be compressed and written to table
//
// The second mode is meant to horizontally scale this job, so only one subvector will be fetched
// for the job and codebook will be created for that subvector
// Then separate job will be run to compress vectors and write to table

fn quantize_table_local(
    args: cli::PQArgs,
    main_progress: AtomicU8,
    db_uri: &str,
    full_table_name: &str,
    codebook_table_name: &str,
    pq_column_name: &str,
    progress_cb: Option<ProgressCbFn>,
    is_canceled: Option<Arc<RwLock<bool>>>,
    logger: &Logger,
) -> AnyhowVoidResult {
    let is_canceled = is_canceled.unwrap_or(Arc::new(RwLock::new(false)));
    let column = &args.column;
    let schema = &args.schema;
    let table = &args.table;

    let mut client = Client::connect(db_uri, NoTls)?;
    let mut transaction = client.transaction()?;

    // Create codebook table and add pqvec column to table
    if !args.skip_table_setup {
        setup::setup_tables(
            &mut transaction,
            &full_table_name,
            &codebook_table_name,
            &pq_column_name,
            &logger,
        )?;

        setup::setup_triggers(
            &mut transaction,
            &full_table_name,
            &codebook_table_name,
            &pq_column_name,
            column,
            "l2sq",
            args.splits,
        )?;

        // Creating new transaction, because  current transaction will lock table reads
        // and block the process
        transaction.commit()?;
        transaction = client.transaction()?;

        // Commit and return if the task is to only set up tables
        if args.only_setup {
            set_and_report_progress(&progress_cb, &logger, &main_progress, 100);
            return Ok(());
        }
    }

    let total_row_count = transaction.query_one(
        &format!(
            "SELECT COUNT({pk}) FROM {full_table_name};",
            pk = quote_ident(&args.pk)
        ),
        &[],
    )?;

    let total_row_count = total_row_count.try_get::<usize, i64>(0)? as usize;

    let max_connections = transaction.query_one(
        "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",
        &[],
    )?;
    let max_connections = max_connections.get::<usize, i32>(0) as usize;

    // Only compress will be passed if task is run from Batch job
    // As there will be three phases
    // 1. table setup, 2. codebook craetion 3. table compression 4. trigger setup
    // 2 and 3 phases will be run in parallel
    if args.only_compress {
        drop(transaction);
        compression::compress_and_write_vectors(
            CompressAndWriteVectorArgs {
                codebook_table_name: &codebook_table_name,
                full_table_name: &full_table_name,
                db_uri,
                schema,
                table,
                column,
                pq_column_name: &pq_column_name,
                pk: &args.pk,
                splits: args.splits,
                total_row_count,
                total_task_count: &args.total_task_count,
                parallel_task_count: &args.parallel_task_count,
                compression_task_id: &args.compression_task_id,
                max_connections,
                main_progress: &main_progress,
                progress_cb: &progress_cb,
                logger: &logger,
            },
            client,
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
    let vector_dim = row.try_get::<usize, i32>(0)? as usize;
    // Get subvector dimension
    // It is not neccessary that vector_dim will be divisible to split count
    // If there's reminder the last subvector's dimensions will be higher
    // But it is better to provide a value which is divisible
    let subvector_dim = vector_dim / args.splits;

    // Create codebook
    let (codebooks_hashmap, dataset) = codebook::create_codebook(
        CreateCodebookArgs {
            logger: &logger,
            main_progress: &main_progress,
            progress_cb: &progress_cb,
            db_uri,
            pk: &args.pk,
            column,
            full_table_name: &full_table_name,
            codebook_table_name: &codebook_table_name,
            total_row_count,
            max_connections,
            splits: args.splits,
            vector_dim,
            subvector_dim,
            cluster_count: args.clusters,
            subvector_id: &args.subvector_id,
            parallel_task_count: &args.parallel_task_count,
        },
        &mut transaction,
    )?;

    // Compress vectors using codebook
    // And write results to target table
    if !args.skip_vector_compression {
        let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));

        let dataset = compression::compress_vectors(
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

        compression::write_compressed_rows(
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

    Ok(())
}

pub fn quantize_table(
    args: cli::PQArgs,
    progress_cb: Option<ProgressCbFn>,
    is_canceled: Option<Arc<RwLock<bool>>>,
    logger: Option<Logger>,
) -> AnyhowVoidResult {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern PQ", LogLevel::Debug)));
    logger.info("Lantern CLI - Quantize Table");

    let main_progress = AtomicU8::new(0);
    let total_time_start = Instant::now();
    let full_table_name = get_full_table_name(&args.schema, &args.table);
    let codebook_table_name = args
        .codebook_table_name
        .clone()
        .unwrap_or(format!("_lantern_codebook_{}", args.table));
    let pq_column_name = format!("{}_pq", args.column);
    let db_uri = append_params_to_uri(&args.uri, CONNECTION_PARAMS);

    if args.run_on_gcp {
        gcp_batch::quantize_table_on_gcp(
            args,
            main_progress,
            &db_uri,
            &full_table_name,
            &codebook_table_name,
            &pq_column_name,
            progress_cb,
            &logger,
        )?;
    } else {
        quantize_table_local(
            args,
            main_progress,
            &db_uri,
            &full_table_name,
            &codebook_table_name,
            &pq_column_name,
            progress_cb,
            is_canceled,
            &logger,
        )?;
    }

    logger.debug(&format!(
        "Total duration: {}s",
        total_time_start.elapsed().as_secs()
    ));
    Ok(())
}
