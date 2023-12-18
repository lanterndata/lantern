extern crate postgres;

use rand::Rng;
use std::path::Path;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
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
mod utils;

pub mod cli;

type AnyhowVoidResult = Result<(), anyhow::Error>;

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

pub fn create_usearch_index(
    args: &cli::CreateIndexArgs,
    logger: Option<Logger>,
    client: Option<Client>,
) -> Result<(), anyhow::Error> {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Index", LogLevel::Debug)));
    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    logger.info(&format!("Number of available CPU cores: {}", num_cores));

    // get all row count
    let mut client = client.unwrap_or(Client::connect(&args.uri, NoTls)?);
    let mut transaction = client.transaction()?;
    let full_table_name = get_full_table_name(&args.schema, &args.table);

    transaction.execute(
        &format!("LOCK TABLE ONLY {full_table_name} IN ACCESS EXCLUSIVE MODE NOWAIT"),
        &[],
    )?;

    let rows = transaction.query(&format!("SELECT COUNT(*) FROM {full_table_name};",), &[])?;

    let row = transaction.query_one(&format!("SELECT ARRAY_LENGTH({col}, 1) as dim FROM {full_table_name} WHERE {col} IS NOT NULL LIMIT 1",col=quote_ident(&args.column)), &[])?;

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

    let count: i64 = rows[0].get(0);
    // reserve enough memory on index
    index.reserve(count as usize)?;
    let thread_safe_index = ThreadSafeIndex { inner: index };

    logger.info(&format!("Items to index {}", count));

    let index_arc = Arc::new(thread_safe_index);

    // Create a vector to store thread handles
    let mut handles = vec![];

    let (tx, rx): (Sender<Vec<Row>>, Receiver<Vec<Row>>) = mpsc::channel();
    let rx_arc = Arc::new(Mutex::new(rx));

    for n in 0..num_cores {
        // spawn thread
        let index_ref = index_arc.clone();
        let logger_ref = logger.clone();
        let receiver = rx_arc.clone();

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
                let rows = rows.unwrap();
                index_chunk(rows, n, index_ref.clone(), logger_ref.clone())?;
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
        tx.send(rows)?;
    }

    // Exit all channels
    drop(tx);

    // Wait for all threads to finish processing
    for handle in handles {
        if let Err(e) = handle.join() {
            anyhow::bail!("Erro while joining thread: {:?}", e);
        }
    }

    index_arc.save(&args.out)?;
    logger.info(&format!("Index saved under {}", &args.out));

    if args.import {
        // Close portal, so we will be able to create index
        transaction.execute("CLOSE ALL", &[])?;
        let mut rng = rand::thread_rng();
        let index_path = format!("/tmp/index-{}.usearch", rng.gen_range(0..1000));
        let mut large_object = LargeObject::new(transaction, &index_path);
        large_object.create()?;
        let mut reader = fs::File::open(Path::new(&args.out))?;
        io::copy(&mut reader, &mut large_object)?;
        fs::remove_file(Path::new(&args.out))?;
        large_object.finish(
            &get_full_table_name(&args.schema, &args.table),
            &quote_ident(&args.column),
            args.index_name.as_deref(),
            args.ef,
            args.efc,
            dimensions,
            args.m,
        )?;
        logger.info(&format!(
            "Index imported to table {} and removed from filesystem",
            &args.table
        ));
    }

    Ok(())
}
