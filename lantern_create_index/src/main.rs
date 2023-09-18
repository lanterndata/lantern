use std::{
    cmp,
    sync::{Arc, Barrier},
};
use threadpool::ThreadPool;

use clap::Parser;
use cxx::UniquePtr;
use postgres::{Client, NoTls};
use postgres_types::FromSql;
use usearch::ffi::*;

mod cli;
mod utils;

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
    offset: usize,
    limit: usize,
    thread_n: usize,
    client: &mut postgres::Client,
    index: Arc<ThreadSafeIndex>,
    args: Arc<cli::Args>,
) -> Result<(), anyhow::Error> {
    let rows = client.query(
        &format!(
            "SELECT ctid, {} FROM {} ORDER BY ctid LIMIT {} OFFSET {};",
            args.column, args.table, limit, offset
        ),
        &[],
    )?;

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

fn create_usearch_index(args: cli::Args) -> Result<(), anyhow::Error> {
    let options = IndexOptions {
        dimensions: args.dims,
        metric: args.metric_kind.value(),
        quantization: ScalarKind::F32,
        connectivity: args.m,
        expansion_add: args.efc,
        expansion_search: args.ef,
    };

    let num_cores: usize = std::thread::available_parallelism().unwrap().into();
    let pool = ThreadPool::new(num_cores);
    println!("[*] Number of available CPU cores: {}", num_cores);

    let index = new_index(&options)?;
    // get all row count
    let mut main_client = Client::connect(&args.uri, NoTls).unwrap();
    let rows = main_client.query(&format!("SELECT COUNT(*) FROM {};", args.table), &[])?;

    let count: i64 = rows[0].get(0);
    let count: usize = count as usize;
    // reserve enough memory on index
    index.reserve(count as usize)?;

    // data that each thread will process
    let data_per_thread = if num_cores > count {
        count
    } else {
        (num_cores + count - 1) / num_cores
    };

    let thread_safe_index = ThreadSafeIndex { inner: index };

    let index_arc = Arc::new(thread_safe_index);
    let args_arc = Arc::new(args);

    println!("[*] Items to index {}", count);

    let barrier = Arc::new(Barrier::new(num_cores + 1));
    for n in 0..num_cores {
        let barrier = barrier.clone();
        // spawn thread
        let index_ref = index_arc.clone();
        let args_ref = args_arc.clone();
        pool.execute(move || {
            // chunk count that each thread will process
            // if data for each thread is more than chunks
            // it will be devided into sub chunks and processed sequentially inside the thread
            // this is done to not consume too much memory
            let chunks = cmp::min(data_per_thread, 10000);

            let iterations = if data_per_thread < chunks {
                1
            } else {
                (chunks + data_per_thread - 1) / chunks
            };

            let thread_offset = n * data_per_thread;
            let mut client = Client::connect(&args_ref.uri, NoTls).unwrap();

            for i in 0..iterations {
                let offset = thread_offset + i * chunks;
                index_chunk(
                    offset,
                    chunks,
                    n,
                    &mut client,
                    index_ref.clone(),
                    args_ref.clone(),
                )
                .unwrap();
            }

            barrier.wait();
        });
    }
    barrier.wait();

    index_arc.save(&args_arc.out);
    println!("[*] Index saved under {}", &args_arc.out);
    Ok(())
}

fn main() {
    let args = cli::Args::parse();
    println!(
        "[*] Creating index with parameters dimensions={} m={} ef={} ef_construction={}",
        args.dims, args.m, args.ef, args.efc
    );
    create_usearch_index(args).unwrap();
}
