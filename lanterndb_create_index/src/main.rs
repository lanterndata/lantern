use std::sync::{Arc, Mutex};

use clap::Parser;
use cxx::UniquePtr;
use usearch::ffi::*;

mod cli;
mod utils;
use postgres_types::FromSql;
use tokio_postgres::{Client, NoTls};

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

async fn index_chunk(
    offset: usize,
    limit: usize,
    client: Arc<Client>,
    index: Arc<ThreadSafeIndex>,
    args: Arc<cli::Args>,
) -> Result<(), anyhow::Error> {
    let rows = client
        .query(
            &format!(
                "SELECT ctid, {} FROM {} LIMIT {} OFFSET {};",
                args.column, args.table, limit, offset
            ),
            &[],
        )
        .await?;

    let row_count = rows.len();
    for row in rows {
        let ctid: Tid = row.get(0);
        let vec: Vec<f32> = row.get(1);
        index.add(ctid.label, &vec);
    }
    println!("[*] {} items added to index", row_count);
    Ok(())
}

struct ThreadSafeIndex {
    inner: Mutex<UniquePtr<usearch::ffi::Index>>,
}

impl ThreadSafeIndex {
    fn add(&self, label: u64, data: &Vec<f32>) {
        let index = self.inner.lock().unwrap();
        index.add(label, data).unwrap();
    }
    fn save(&self, path: &str) {
        let index = self.inner.lock().unwrap();
        index.save(path).unwrap();
    }
}

unsafe impl Sync for ThreadSafeIndex {}
unsafe impl Send for ThreadSafeIndex {}

async fn create_usearch_index(args: cli::Args, client: Client) -> Result<(), anyhow::Error> {
    let options = IndexOptions {
        dimensions: args.dims,
        metric: args.metric_kind.value(),
        quantization: ScalarKind::F32,
        connectivity: args.m,
        expansion_add: args.efc,
        expansion_search: args.ef,
    };

    let index = new_index(&options)?;
    let rows = client
        .query(&format!("SELECT COUNT(*) FROM {}", args.table), &[])
        .await?;

    let count: i64 = rows[0].get(0);

    index.reserve(count as usize)?;

    let chunks = 10000;
    let mut tasks = Vec::new();

    let iterations = if count < chunks {
        1
    } else {
        (chunks + count - 1) / chunks
    };

    let thread_safe_index = ThreadSafeIndex {
        inner: Mutex::new(index),
    };

    let index_arc = Arc::new(thread_safe_index);

    let client_arc = Arc::new(client);
    let args_arc = Arc::new(args);

    println!("[*] Items to index {}", count);

    for i in 0..iterations {
        let offset = i * chunks;
        let index_ref = index_arc.clone();
        let client_ref = client_arc.clone();
        let args_ref = args_arc.clone();
        tasks.push(tokio::spawn(index_chunk(
            offset as usize,
            chunks as usize,
            client_ref,
            index_ref,
            args_ref,
        )));
    }

    for task in tasks {
        task.await??;
    }

    index_arc.save(&args_arc.out);
    println!("[*] Index saved under {}", &args_arc.out);
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();
    let (client, connection) = tokio_postgres::connect(&args.uri, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            panic!("connection error: {}", e);
        }
    });

    println!(
        "[*] Creating index with parameters dimensions={} m={} ef={} ef_construction={}",
        args.dims, args.m, args.ef, args.efc
    );
    create_usearch_index(args, client).await.unwrap();
}
