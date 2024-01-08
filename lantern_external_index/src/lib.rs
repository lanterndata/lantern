extern crate postgres;

use rand::Rng;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::sync::{mpsc, RwLock};
use std::sync::{Arc, Mutex};
use std::{fs, io};

use cxx::UniquePtr;
use lantern_logger::{LogLevel, Logger};
use lantern_utils::{get_full_table_name, quote_ident};
use postgres::{Client, NoTls, Row};
use postgres_large_objects::LargeObject;
use postgres_types::FromSql;
use usearch::ffi::*;

mod postgres_large_objects;
pub mod utils;

pub mod cli;

type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type ProgressCbFn = Box<dyn Fn(u8) + Send + Sync>;
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

fn index_chunk(
    rows: Vec<Row>,
    thread_n: usize,
    index: Arc<ThreadSafeIndex>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let row_count = rows.len();

    for row in rows {
        let ctid: Tid = row.get(0);
        let vec: Vec<f32> = row.get(1);
        index.add_in_thread(ctid.label, &vec, thread_n)?;
    }
    logger.debug(&format!(
        "{} items added to index from thread {}",
        row_count, thread_n
    ));
    Ok(())
}

struct ThreadSafeIndex {
    inner: UniquePtr<usearch::ffi::Index>,
}

impl ThreadSafeIndex {
    fn add_in_thread(&self, label: u64, data: &Vec<f32>, thread: usize) -> AnyhowVoidResult {
        self.inner.add_in_thread(label, data, thread)?;
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
    logger.info(&format!("Number of available CPU cores: {}", num_cores));

    // get all row count
    let mut client = Client::connect(&args.uri, NoTls)?;
    let mut transaction = client.transaction()?;
    let full_table_name = get_full_table_name(&args.schema, &args.table);

    transaction.execute("SET lock_timeout='5s'", &[])?;
    transaction.execute(
        &format!("LOCK TABLE ONLY {full_table_name} IN ACCESS EXCLUSIVE MODE"),
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

    let options = IndexOptions {
        dimensions,
        metric: args.metric_kind.value(),
        quantization: ScalarKind::F32,
        connectivity: args.m,
        expansion_add: args.efc,
        expansion_search: args.ef,
    };
    let index = new_index(&options)?;

    let rows = transaction.query(&format!("SELECT COUNT(*) FROM {full_table_name};",), &[])?;
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
    for n in 0..num_cores {
        // spawn thread
        let index_ref = index_arc.clone();
        let logger_ref = logger.clone();
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
                index_chunk(rows, n, index_ref.clone(), logger_ref.clone())?;
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
            "SELECT ctid, {} FROM {};",
            quote_ident(&args.column),
            get_full_table_name(&args.schema, &args.table)
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

    index_arc.save(&args.out)?;
    logger.info(&format!("Index saved under {}", &args.out));

    drop(index_arc);
    drop(portal);
    drop(rx_arc);

    if args.import {
        if args.remote_database {
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
            fs::remove_file(Path::new(&args.out))?;
            progress_tx.send(90)?;
            drop(reader);
            drop(buf_writer);
            logger.info("Creating index from file...");
            large_object.finish(
                &get_full_table_name(&args.schema, &args.table),
                &quote_ident(&args.column),
                args.index_name.as_deref(),
                args.ef,
                args.efc,
                dimensions,
                args.m,
            )?;
        } else {
            // If job is run on the same server as database we can skip copying part
            progress_tx.send(90)?;
            logger.info("Creating index from file...");

            let mut idx_name = "".to_owned();

            if let Some(name) = &args.index_name {
                idx_name = quote_ident(name);
                transaction.execute(&format!("DROP INDEX IF EXISTS {idx_name}"), &[])?;
            }

            transaction.execute(
            &format!("CREATE INDEX {idx_name} ON {table_name} USING hnsw({column_name}) WITH (_experimental_index_path='{index_path}', ef={ef}, dim={dim}, m={m}, ef_construction={ef_construction});", index_path=args.out, table_name=&get_full_table_name(&args.schema, &args.table),column_name=&quote_ident(&args.column), m=args.m, ef=args.ef, ef_construction=args.efc, dim=dimensions),
            &[],
            )?;

            fs::remove_file(Path::new(&args.out))?;
        }
        progress_tx.send(100)?;
        logger.info(&format!(
            "Index imported to table {} and removed from filesystem",
            &args.table
        ));
    }

    Ok(())
}
