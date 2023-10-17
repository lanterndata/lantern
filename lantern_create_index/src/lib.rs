use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use cxx::UniquePtr;
use postgres::{Client, NoTls, Row};
use postgres_types::FromSql;
use usearch::ffi::*;

mod utils;

pub mod cli;

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
) -> Result<(), anyhow::Error> {
    let row_count = rows.len();

    for row in rows {
        let ctid: Tid = row.get(0);
        let vec: Vec<f32> = row.get(1);
        index.add_in_thread(ctid.label, &vec, thread_n);
    }
    println!(
        "[*] {} items added to index from thread {}",
        row_count, thread_n
    );
    Ok(())
}

struct ThreadSafeIndex {
    inner: UniquePtr<usearch::ffi::Index>,
}

impl ThreadSafeIndex {
    fn add_in_thread(&self, label: u64, data: &Vec<f32>, thread: usize) {
        self.inner.add_in_thread(label, data, thread).unwrap();
    }
    fn save(&self, path: &str) {
        self.inner.save(path).unwrap();
    }
}

unsafe impl Sync for ThreadSafeIndex {}
unsafe impl Send for ThreadSafeIndex {}

pub fn create_usearch_index(args: &cli::CreateIndexArgs) -> Result<(), anyhow::Error> {
    println!(
        "[*] Creating index with parameters dimensions={} m={} ef={} ef_construction={}",
        args.dims, args.m, args.ef, args.efc
    );

    let options = IndexOptions {
        dimensions: args.dims,
        metric: args.metric_kind.value(),
        quantization: ScalarKind::F32,
        connectivity: args.m,
        expansion_add: args.efc,
        expansion_search: args.ef,
    };

    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    println!("[*] Number of available CPU cores: {}", num_cores);

    let index = new_index(&options)?;
    // get all row count
    let mut client = Client::connect(&args.uri, NoTls).unwrap();
    let mut transaction = client.transaction()?;
    let rows = transaction.query(&format!("SELECT COUNT(*) FROM \"{}\";", args.table), &[])?;

    let count: i64 = rows[0].get(0);
    // reserve enough memory on index
    index.reserve(count as usize)?;
    let thread_safe_index = ThreadSafeIndex { inner: index };

    println!("[*] Items to index {}", count);

    let index_arc = Arc::new(thread_safe_index);

    // Create a vector to store thread handles
    let mut handles = vec![];

    let (tx, rx): (Sender<Vec<Row>>, Receiver<Vec<Row>>) = mpsc::channel();
    let rx_arc = Arc::new(Mutex::new(rx));

    for n in 0..num_cores {
        // spawn thread
        let index_ref = index_arc.clone();
        let receiver = rx_arc.clone();

        let handle = std::thread::spawn(move || loop {
            let rx = receiver.lock().unwrap();
            let rows = rx.recv();
            // release the lock so other threads can take rows
            drop(rx);

            if rows.is_err() {
                // channel has been closed
                break;
            }
            let rows = rows.unwrap();
            index_chunk(rows, n, index_ref.clone()).unwrap();
        });
        handles.push(handle);
    }

    // With portal we can execute a query and poll values from it in chunks
    let portal = transaction
        .bind(
            &format!("SELECT ctid, {} FROM \"{}\";", &args.column, &args.table),
            &[],
        )
        .unwrap();

    loop {
        // poll 2000 rows from portal and send it to worker threads via channel
        let rows = transaction.query_portal(&portal, 2000).unwrap();
        if rows.len() == 0 {
            break;
        }
        tx.send(rows).unwrap();
    }

    // Exit all channels
    drop(tx);

    // Wait for all threads to finish processing
    for handle in handles {
        handle.join().unwrap();
    }

    index_arc.save(&args.out);
    println!("[*] Index saved under {}", &args.out);
    Ok(())
}
