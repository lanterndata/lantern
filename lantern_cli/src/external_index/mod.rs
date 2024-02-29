use rand::Rng;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::sync::{mpsc, RwLock};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{fs, io};
use usearch::ffi::{IndexOptions, ScalarKind};
use usearch::Index;

use crate::logger::{LogLevel, Logger};
use crate::types::*;
use crate::utils::{get_full_table_name, quote_ident};
use postgres::{Client, NoTls, Row};
use postgres_large_objects::LargeObject;
use postgres_types::FromSql;

pub mod cli;
mod postgres_large_objects;
pub mod utils;

// Used to control chunk size when copying index file to postgres server
static COPY_BUFFER_CHUNK_SIZE: usize = 1024 * 1024 * 10; // 10MB

#[derive(Debug)]
struct Tid {
    label: u64,
}

impl<'a> FromSql<'a> for Tid {
    fn from_sql(
        _: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut bytes: Vec<u8> = Vec::with_capacity(raw.len());

        // Copy bytes of block_number->bi_hi first 2 bytes
        for b in raw[..2].iter().rev() {
            bytes.push(*b);
        }

        // Copy bytes of block_number->bi_lo next 2 bytes
        for b in raw[2..4].iter().rev() {
            bytes.push(*b);
        }

        // Copy bytes of index_number last 2 bytes
        for b in raw[4..6].iter().rev() {
            bytes.push(*b);
        }

        let label: u64 = utils::bytes_to_integer_le(&bytes);
        Ok(Tid { label })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        ty.name() == "tid"
    }
}

fn index_chunk(rows: Vec<Row>, index: Arc<ThreadSafeIndex>) -> AnyhowVoidResult {
    for row in rows {
        let ctid: Tid = row.get(0);
        let vec: Vec<f32> = row.get(1);
        index.add(ctid.label, &vec)?;
    }
    Ok(())
}

struct ThreadSafeIndex {
    inner: Index,
}

impl ThreadSafeIndex {
    fn add(&self, label: u64, data: &[f32]) -> AnyhowVoidResult {
        self.inner.add(label, data)?;
        Ok(())
    }
    fn save(&self, path: &str) -> AnyhowVoidResult {
        self.inner.save(path)?;
        Ok(())
    }
}

unsafe impl Sync for ThreadSafeIndex {}
unsafe impl Send for ThreadSafeIndex {}

fn report_progress(progress_cb: &Option<ProgressCbFn>, logger: &Logger, progress: u8) {
    logger.info(&format!("Progress {progress}%"));
    if progress_cb.is_some() {
        let cb = progress_cb.as_ref().unwrap();
        cb(progress);
    }
}

pub fn create_usearch_index(
    args: &cli::CreateIndexArgs,
    progress_cb: Option<ProgressCbFn>,
    is_canceled: Option<Arc<RwLock<bool>>>,
    logger: Option<Logger>,
) -> Result<(), anyhow::Error> {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Index", LogLevel::Debug)));
    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let total_start_time = Instant::now();
    logger.info(&format!("Number of available CPU cores: {}", num_cores));

    // get all row count
    let mut client = Client::connect(&args.uri, NoTls)?;
    let mut transaction = client.transaction()?;
    let full_table_name = get_full_table_name(&args.schema, &args.table);

    transaction.execute("SET lock_timeout='5s'", &[])?;
    transaction.execute(
        &format!("LOCK TABLE ONLY {full_table_name} IN SHARE MODE"),
        &[],
    )?;

    let rows = transaction.query(&format!("SELECT ARRAY_LENGTH({col}, 1) as dim FROM {full_table_name} WHERE {col} IS NOT NULL LIMIT 1",col=quote_ident(&args.column)), &[])?;

    if rows.len() == 0 {
        anyhow::bail!("Cannot create an external index on empty table");
    }

    let row = rows.first().unwrap();
    let infered_dimensions = row.try_get::<usize, i32>(0)? as usize;

    if args.dims != 0 && infered_dimensions != args.dims {
        // I didn't complitely remove the dimensions from args
        // To have extra validation when reindexing external index
        // This is invariant and should never be a case
        anyhow::bail!("Infered dimensions ({infered_dimensions}) does not match with the provided dimensions ({dims})", dims=args.dims);
    }

    let dimensions = infered_dimensions;

    logger.info(&format!(
        "Creating index with parameters dimensions={} m={} ef={} ef_construction={}",
        dimensions, args.m, args.ef, args.efc
    ));

    let mut pq_codebook: *const f32 = std::ptr::null();
    let mut v: Vec<f32> = vec![];
    let mut num_centroids: usize = 0;
    let mut num_subvectors: usize = 0;

    if args.pq {
        let codebook_table_name = format!(
            "pq_{table_name}_{column_name}",
            table_name = &args.table,
            column_name = &args.column
        );
        let full_codebook_table_name =
            get_full_table_name("_lantern_internal", &codebook_table_name);

        let rows_codebook_exists = transaction.query("SELECT true FROM information_schema.tables WHERE table_schema='_lantern_internal' AND table_name=$1;", &[&codebook_table_name])?;

        if rows_codebook_exists.len() == 0 {
            anyhow::bail!("Codebook table {full_codebook_table_name} does not exist");
        }

        let rows_c = transaction.query(
            &format!("SELECT COUNT(*) FROM {full_codebook_table_name} WHERE subvector_id = 0;"),
            &[],
        )?;
        let rows_sv = transaction.query(
            &format!("SELECT COUNT(*) FROM {full_codebook_table_name} WHERE centroid_id = 0;"),
            &[],
        )?;

        if rows_c.len() == 0 || rows_sv.len() == 0 {
            anyhow::bail!("Invalid codebook table");
        }

        num_centroids = rows_c.first().unwrap().get::<usize, i64>(0) as usize;
        num_subvectors = rows_sv.first().unwrap().get::<usize, i64>(0) as usize;

        v.resize(num_centroids * dimensions, 0.);

        let rows = transaction.query(
            &format!("SELECT subvector_id, centroid_id, c FROM {full_codebook_table_name};",),
            &[],
        )?;

        logger.info(&format!(
            "Codebook has {} rows - {num_centroids} centroids and {num_subvectors} subvectors",
            rows.len()
        ));

        for r in rows {
            let subvector_id: i32 = r.get(0);
            let centroid_id: i32 = r.get(1);
            let subvector: Vec<f32> = r.get(2);
            for i in 0..subvector.len() {
                v[centroid_id as usize * dimensions
                    + subvector_id as usize * subvector.len()
                    + i] = subvector[i];
            }
        }
        pq_codebook = v.as_ptr();
    }

    let options = IndexOptions {
        dimensions,
        metric: args.metric_kind.value(),
        quantization: ScalarKind::F32,
        multi: false,
        connectivity: args.m,
        expansion_add: args.efc,
        expansion_search: args.ef,

        num_threads: 0, // automatic

        // note: pq_construction and pq_output distinction is not yet implemented in usearch
        // in the future, if pq_construction is false, we will use full vectors in memory (and
        // require large memory for construction) but will output pq-quantized graph
        //
        // currently, regardless of pq_construction value, as long as pq_output is true,
        // we construct a pq_quantized index using quantized values during construction
        pq_construction: args.pq,
        pq_output: args.pq,
        num_centroids,
        num_subvectors,
        codebook: pq_codebook,
    };
    let index = Index::new(&options)?;

    let start_time = Instant::now();
    let rows = transaction.query(
        &format!(
            "SELECT COUNT(*) FROM {full_table_name} WHERE {} IS NOT NULL;",
            quote_ident(&args.column)
        ),
        &[],
    )?;
    logger.debug(&format!(
        "Count estimation took {}",
        start_time.elapsed().as_secs()
    ));

    let start_time = Instant::now();
    let count: i64 = rows[0].get(0);
    // reserve enough memory on index
    index.reserve(count as usize)?;
    let thread_safe_index = ThreadSafeIndex { inner: index };

    logger.info(&format!("Items to index {}", count));

    let index_arc = Arc::new(thread_safe_index);

    // Create a vector to store thread handles
    let mut handles = vec![];

    let (tx, rx): (SyncSender<Vec<Row>>, Receiver<Vec<Row>>) = mpsc::sync_channel(num_cores);
    let rx_arc = Arc::new(Mutex::new(rx));
    let is_canceled = is_canceled.unwrap_or(Arc::new(RwLock::new(false)));
    let (progress_tx, progress_rx): (Sender<u8>, Receiver<u8>) = mpsc::channel();
    let progress_logger = logger.clone();
    let should_create_index = args.import;

    std::thread::spawn(move || -> AnyhowVoidResult {
        let mut prev_progress = 0;
        for progress in progress_rx {
            if progress == prev_progress {
                continue;
            }
            prev_progress = progress;
            report_progress(&progress_cb, &progress_logger, progress);

            if progress == 100 {
                break;
            }
        }
        Ok(())
    });

    let processed_cnt = Arc::new(AtomicU64::new(0));
    for _ in 0..num_cores {
        // spawn thread
        let index_ref = index_arc.clone();
        let receiver = rx_arc.clone();
        let is_canceled = is_canceled.clone();
        let progress_tx = progress_tx.clone();
        let processed_cnt = processed_cnt.clone();

        let handle = std::thread::spawn(move || -> AnyhowVoidResult {
            loop {
                let lock = receiver.lock();

                if let Err(e) = lock {
                    anyhow::bail!("{e}");
                }

                let rx = lock.unwrap();
                let rows = rx.recv();
                // release the lock so other threads can take rows
                drop(rx);

                if rows.is_err() {
                    // channel has been closed
                    break;
                }

                if *is_canceled.read().unwrap() {
                    // This variable will be changed from outside to gracefully
                    // exit job on next chunk
                    anyhow::bail!("Job canceled");
                }

                let rows = rows.unwrap();
                let rows_cnt = rows.len();
                index_chunk(rows, index_ref.clone())?;
                let all_count = processed_cnt.fetch_add(rows_cnt as u64, Ordering::SeqCst);
                let mut progress = (all_count as f64 / count as f64 * 100.0) as u8;
                if should_create_index {
                    // reserve 20% progress for index import
                    progress = if progress > 20 { progress - 20 } else { 0 };
                }

                if progress > 0 {
                    progress_tx.send(progress)?;
                }
            }
            Ok(())
        });
        handles.push(handle);
    }

    // With portal we can execute a query and poll values from it in chunks
    let portal = transaction.bind(
        &format!(
            "SELECT ctid, {col} FROM {table} WHERE {col} IS NOT NULL;",
            col = quote_ident(&args.column),
            table = get_full_table_name(&args.schema, &args.table)
        ),
        &[],
    )?;

    loop {
        // poll 2000 rows from portal and send it to worker threads via channel
        let rows = transaction.query_portal(&portal, 2000)?;
        if rows.len() == 0 {
            break;
        }
        if *is_canceled.read().unwrap() {
            // This variable will be changed from outside to gracefully
            // exit job on next chunk
            anyhow::bail!("Job canceled");
        }
        tx.send(rows)?;
    }

    // Exit all channels
    drop(tx);

    // Wait for all threads to finish processing
    for handle in handles {
        if let Err(e) = handle.join() {
            logger.error("{e}");
            anyhow::bail!("{:?}", e);
        }
    }
    logger.debug(&format!(
        "Indexing took {}s",
        start_time.elapsed().as_secs()
    ));

    index_arc.save(&args.out)?;
    logger.info(&format!(
        "Index saved under {} in {}s",
        &args.out,
        start_time.elapsed().as_secs()
    ));

    drop(index_arc);
    drop(portal);
    drop(rx_arc);

    if args.import {
        let op_class = args.metric_kind.to_ops();
        if args.remote_database {
            let start_time = Instant::now();
            logger.info("Copying index file into database server...");
            let mut rng = rand::thread_rng();
            let data_dir = transaction.query_one("SHOW data_directory", &[])?;
            let data_dir: String = data_dir.try_get(0)?;
            let index_path = format!("{data_dir}/ldb-index-{}.usearch", rng.gen_range(0..1000));
            let mut large_object = LargeObject::new(transaction, &index_path);
            large_object.create()?;
            let mut reader = fs::File::open(Path::new(&args.out))?;
            let mut buf_writer =
                BufWriter::with_capacity(COPY_BUFFER_CHUNK_SIZE, &mut large_object);
            io::copy(&mut reader, &mut buf_writer)?;
            logger.debug(&format!(
                "Index copy to database took {}s",
                start_time.elapsed().as_secs()
            ));
            progress_tx.send(90)?;
            drop(reader);
            drop(buf_writer);
            logger.info("Creating index from file...");
            let start_time = Instant::now();
            large_object.finish(
                &get_full_table_name(&args.schema, &args.table),
                &quote_ident(&args.column),
                args.index_name.as_deref(),
                &op_class,
                args.ef,
                args.efc,
                dimensions,
                args.m,
                args.pq,
            )?;
            logger.debug(&format!(
                "Index import took {}s",
                start_time.elapsed().as_secs()
            ));
            fs::remove_file(Path::new(&args.out))?;
        } else {
            // If job is run on the same server as database we can skip copying part
            progress_tx.send(90)?;
            logger.info("Creating index from file...");
            let start_time = Instant::now();

            let mut idx_name = "".to_owned();

            if let Some(name) = &args.index_name {
                idx_name = quote_ident(name);
                transaction.execute(&format!("DROP INDEX IF EXISTS {idx_name}"), &[])?;
            }

            transaction.execute(
            &format!("CREATE INDEX {idx_name} ON {table_name} USING lantern_hnsw({column_name} {op_class}) WITH (_experimental_index_path='{index_path}', pq={pq}, ef={ef}, dim={dim}, m={m}, ef_construction={ef_construction});", index_path=args.out, table_name=&get_full_table_name(&args.schema, &args.table),
            column_name=&quote_ident(&args.column), pq=args.pq, m=args.m, ef=args.ef, ef_construction=args.efc, dim=dimensions),
            &[],
            )?;

            transaction.commit()?;
            logger.debug(&format!(
                "Index import took {}s",
                start_time.elapsed().as_secs()
            ));
            fs::remove_file(Path::new(&args.out))?;
        }
        progress_tx.send(100)?;
        logger.info(&format!(
            "Index imported to table {} and removed from filesystem",
            &args.table
        ));
        logger.debug(&format!(
            "Total indexing took {}s",
            total_start_time.elapsed().as_secs()
        ));
    }

    Ok(())
}
