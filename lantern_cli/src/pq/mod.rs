use crate::logger::{LogLevel, Logger};
use crate::utils::{append_params_to_uri, get_full_table_name, quote_ident};
use codebook::CreateCodebookArgs;
use quantization::QuantizeAndWriteVectorArgs;
use rand::Rng;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use postgres::{Client, NoTls};

pub mod cli;
mod codebook;
mod gcp_batch;
mod quantization;
mod setup;

type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";
pub static LANTERN_INTERNAL_SCHEMA_NAME: &'static str = "_lantern_internal";

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
// subvectors then codebook will be created, vectors will be quantized and written to table
//
// The second mode is meant to horizontally scale this job, so only one subvector will be fetched
// for the job and codebook will be created for that subvector
// Then separate job will be run to quantize vectors and write to table

fn quantize_table_local(
    args: cli::PQArgs,
    main_progress: AtomicU8,
    db_uri: &str,
    full_table_name: &str,
    full_codebook_table_name: &str,
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
            &full_codebook_table_name,
            &pq_column_name,
            &logger,
        )?;

        setup::setup_triggers(
            &mut transaction,
            &full_table_name,
            &full_codebook_table_name,
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
        if args.skip_codebook_creation && args.skip_vector_quantization {
            set_and_report_progress(&progress_cb, &logger, &main_progress, 100);
            return Ok(());
        }
    }

    let limit = if let Some(limit) = args.dataset_limit {
        limit
    } else {
        0
    };

    if limit > 0 && limit < args.clusters {
        anyhow::bail!("--dataset-limit should be greater than or equal to cluster count");
    }

    let total_row_count = match args.dataset_size {
        Some(row_cnt) => row_cnt,
        None => {
            let count_query = transaction.query_one(
                &format!(
                    "SELECT COUNT({pk}) FROM {full_table_name};",
                    pk = quote_ident(&args.pk)
                ),
                &[],
            )?;

            count_query.try_get::<usize, i64>(0)? as usize
        }
    };

    if total_row_count < args.clusters {
        anyhow::bail!(
            "--clusters ({clusters}) should be smaller than dataset size ({total_row_count})",
            clusters = args.clusters
        );
    }

    let start_offset_id = if args.start_offset_id.is_some() {
        args.start_offset_id.unwrap()
    } else if limit > 0 {
        let mut rng = rand::thread_rng();
        let max_id = if limit > total_row_count {
            0
        } else {
            total_row_count - limit
        };

        // Generate random offset to take portion of dataset
        // We are not doing order by random() limit X, because it is slow, and chunking based on id
        // will become harder
        rng.gen_range(0..max_id)
    } else {
        0
    };

    let total_row_count = if limit > 0 && limit <= total_row_count {
        limit
    } else {
        total_row_count
    };

    let max_connections = transaction.query_one(
        "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",
        &[],
    )?;
    let max_connections = max_connections.get::<usize, i32>(0) as usize;

    // If --skip-codebook-creation is passed that means we only need to quantize and write vectors
    // As there will be three phases
    // 1. table setup, 2. codebook craetion 3. table quantization 4. trigger setup
    // 2 and 3 phases will be run in parallel
    if args.skip_codebook_creation && !args.skip_vector_quantization {
        drop(transaction);
        quantization::quantize_and_write_vectors(
            QuantizeAndWriteVectorArgs {
                codebook_table_name: &full_codebook_table_name,
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
                quantization_task_id: &args.quantization_task_id,
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
    if vector_dim < args.splits {
        anyhow::bail!(
            "--splits ({splits}) should be less than or equal to vector dimensions ({vector_dim})",
            splits = args.splits
        )
    }
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
            codebook_table_name: &full_codebook_table_name,
            total_row_count,
            start_offset_id,
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

    if args.subvector_id.is_none() {
        // We will only run this if clustering is run for whole dataset
        // As we can not know if this is the last task or not
        // So it is the responsibility of workflow orchestrator
        // To make the codebook table logged and readonly
        setup::make_codebook_logged_and_readonly(&mut transaction, &full_codebook_table_name)?;
    }

    // quantize vectors using codebook
    // And write results to target table
    if !args.skip_vector_quantization {
        let codebooks_hashmap = Arc::new(RwLock::new(codebooks_hashmap));

        let dataset = quantization::quantize_vectors(
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

        quantization::write_quantized_rows(
            &mut transaction,
            &dataset,
            &args.schema,
            &args.table,
            &pq_column_name,
            &args.pk,
            "quantize",
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
        .unwrap_or(format!("pq_{}_{}", args.table, args.column));

    if codebook_table_name.len() > 63 {
        anyhow::bail!("Codebook table name \"{codebook_table_name}\" exceeds 63 char limit")
    }

    let full_codebook_table_name =
        get_full_table_name(LANTERN_INTERNAL_SCHEMA_NAME, &codebook_table_name);
    let pq_column_name = format!("{}_pq", args.column);
    let db_uri = append_params_to_uri(&args.uri, CONNECTION_PARAMS);

    if args.run_on_gcp {
        gcp_batch::quantize_table_on_gcp(
            args,
            main_progress,
            &db_uri,
            &full_table_name,
            &full_codebook_table_name,
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
            &full_codebook_table_name,
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
