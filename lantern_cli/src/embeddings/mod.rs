use crate::logger::{LogLevel, Logger};
use crate::types::*;
use crate::utils::{append_params_to_uri, get_full_table_name, quote_ident};
use bytes::BytesMut;
use core::get_available_runtimes;
use csv::Writer;
use futures::SinkExt;
use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use tokio_postgres::{Client, NoTls, Row};

use self::core::EmbeddingRuntime;

pub mod cli;
pub mod core;
pub mod measure_speed;

type EmbeddingRecord = (String, Vec<f32>);

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

// Helper function to calculate progress using total and processed row count
fn calculate_progress(total: i64, processed: usize) -> u8 {
    if total <= 0 {
        return 0;
    }

    return ((processed as f64 / total as f64) * 100.0) as u8;
}

async fn estimate_count(
    client: &mut Client,
    full_table_name: &str,
    filter_sql: &str,
    limit_sql: &str,
    logger: Arc<Logger>,
) -> Result<i64, anyhow::Error> {
    let transaction = client.transaction().await?;

    let rows = transaction
        .query(
            &format!("SELECT COUNT(*) FROM {full_table_name} {filter_sql} {limit_sql};"),
            &[],
        )
        .await;

    if let Err(e) = rows {
        anyhow::bail!("{e}");
    }

    let rows = rows.unwrap();

    let count: i64 = rows[0].get(0);

    if count > 0 {
        logger.info(&format!(
            "Found approximately {} items in table \"{}\"",
            count, full_table_name,
        ));
    }

    Ok(count)
}

// This function will do the following
// 1. Get approximate number of rows from pg_class (this is just for info logging)
// 2. Create transaction portal which will poll data from database of batch size provided via args
// 3. Send the rows over the channel
async fn producer_worker(
    client: &mut Client,
    pk: &str,
    column: &str,
    full_table_name: &str,
    filter_sql: &str,
    limit_sql: &str,
    batch_size: usize,
    tx: Sender<Vec<Row>>,
) -> AnyhowVoidResult {
    let transaction = client.transaction().await?;
    // With portal we can execute a query and poll values from it in chunks
    let portal = transaction.bind(
            &format!(
                "SELECT {pk}::text, {column}::text FROM {full_table_name} {filter_sql} {limit_sql};",
                column = quote_ident(&column),
                pk = quote_ident(&pk)
            ),
            &[],
        ).await?;

    loop {
        // poll batch_size rows from portal and send it to embedding thread via channel
        let rows = transaction.query_portal(&portal, batch_size as i32).await?;

        if rows.len() == 0 {
            break;
        }

        if tx.send(rows).await.is_err() {
            break;
        }
    }
    Ok(())
}

// Embedding worker will listen to the producer channel
// and execute embeddings_core's corresponding function to generate embeddings
// we will here map each vector to it's row id before sending the results over channel
// So we will get Vec<Row<String, String> and output Vec<(String, Vec<f32>)> the output will
// contain generated embeddings for the text. If text will be null we will skip that row
async fn embedding_worker(
    args: Arc<cli::EmbeddingArgs>,
    mut rx: Receiver<Vec<Row>>,
    tx: Sender<Vec<EmbeddingRecord>>,
    cancel_token: CancellationToken,
    logger: Arc<Logger>,
) -> AnyhowUsizeResult {
    let mut count: usize = 0;
    let mut processed_tokens: usize = 0;
    let model = &args.model;
    let mut start = Instant::now();
    let runtime = EmbeddingRuntime::new(&args.runtime, None, &args.runtime_params)?;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                if msg.is_none() {
                    break;
                }
                let rows = msg.unwrap();
                if count == 0 {
                    // mark exact start time
                    start = Instant::now();
                }

                let mut input_vectors: Vec<&str> = Vec::with_capacity(rows.len());
                let mut input_ids: Vec<String> = Vec::with_capacity(rows.len());

                for row in &rows {
                    if let Ok(Some(src_data)) = row.try_get::<usize, Option<&str>>(1) {
                        if src_data.trim() != "" {
                            input_vectors.push(src_data);
                            input_ids.push(row.get::<usize, String>(0));
                        }
                    }
                }

                if input_vectors.len() == 0 {
                    continue;
                }

                let embedding_response = runtime.process(&model, &input_vectors).await;

                if let Err(e) = embedding_response {
                    anyhow::bail!("{}", e);
                }

                let embedding_response = embedding_response.unwrap();

                processed_tokens += embedding_response.processed_tokens;
                let mut embeddings = embedding_response.embeddings;

                count += embeddings.len();

                let duration = start.elapsed().as_secs();
                // avoid division by zero error
                let duration = if duration > 0 { duration } else { 1 };
                logger.debug(&format!(
                    "Generated {} embeddings - speed {} emb/s",
                    count,
                    count / duration as usize
                ));

                let mut response_data = Vec::with_capacity(rows.len());

                for _ in 0..embeddings.len() {
                    response_data.push((input_ids.pop().unwrap(), embeddings.pop().unwrap()));
                }

                if tx.send(response_data).await.is_err() {
                    // Error occured in exporter worker and channel has been closed
                    break;
                }
            },
            _ = cancel_token.cancelled() => {
               anyhow::bail!(JOB_CANCELLED_MESSAGE);
            }
        }
    }

    if count > 0 {
        logger.info("Embedding generation finished, waiting to export results...");
    } else {
        logger.warn("No data to generate embeddings");
    }
    Ok(processed_tokens)
}

// DB exporter worker will create temp table with name _lantern_tmp_${rand(0,1000)}
// Then it will create writer stream which will COPY bytes from stdin to that table
// After that it will receiver the output embeddings mapped with row ids over the channel
// And write them using writer instance
// At the end we will flush the writer commit the transaction and UPDATE destination table
// Using our TEMP table data
async fn db_exporter_worker(
    uri: &str,
    args: Arc<cli::EmbeddingArgs>,
    mut rx: Receiver<Vec<EmbeddingRecord>>,
    item_count: i64,
    progress_cb: Option<ProgressCbFn>,
    logger: Arc<Logger>,
) -> Result<JoinHandle<AnyhowUsizeResult>, anyhow::Error> {
    let (mut client, connection) = tokio_postgres::connect(&uri, NoTls).await?;
    tokio::spawn(async move { connection.await });
    let transaction = client.transaction().await?;
    let temp_table_name = format!("_lantern_tmp_{}", rand::thread_rng().gen_range(0..1000));

    let column = args.out_column.clone();
    let table = args.out_table.clone().unwrap_or(args.table.clone());
    let schema = args.schema.clone();
    let pk = args.pk.clone();
    let stream = args.stream;
    let full_table_name = get_full_table_name(&schema, &table);

    if args.create_column {
        transaction
            .execute(
                &format!(
                    "ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS {column} REAL[]",
                    column = quote_ident(&column)
                ),
                &[],
            )
            .await?;
    }

    // Try to check if user has write permissions to table
    let res = transaction.query("SELECT grantee FROM information_schema.column_privileges WHERE table_schema=$1 AND table_name=$2 AND column_name=$3 AND privilege_type='UPDATE' AND grantee=current_user UNION SELECT usename from pg_user where usename = CURRENT_USER AND usesuper=true;", &[&schema, &table, &column]).await?;

    if res.get(0).is_none() {
        anyhow::bail!("User does not have write permissions to target table");
    }

    transaction.commit().await?;
    let handle = tokio::spawn(async move {
        let transaction = client.transaction().await?;
        transaction
            .execute(
                &format!(
                    "CREATE TEMPORARY TABLE {temp_table_name} AS SELECT {pk}, '{{}}'::REAL[] AS {column} FROM {full_table_name} LIMIT 0",
                    pk=quote_ident(&pk),
                    column=quote_ident(&column)
                ),
                &[],
            ).await?;
        transaction.commit().await?;

        let mut transaction = client.transaction().await?;
        let mut writer_sink = Box::pin(
            transaction
                .copy_in(&format!(
                    "COPY {temp_table_name} FROM stdin WITH NULL AS 'NULL'"
                ))
                .await?,
        );
        let chunk_size = 1024 * 1024 * 10; // 10 MB
        let mut buf = BytesMut::with_capacity(chunk_size * 2);
        let update_sql = &format!("UPDATE {full_table_name} dest SET {column} = src.{column} FROM {temp_table_name} src WHERE src.{pk} = dest.{pk}", column=quote_ident(&column), temp_table_name=quote_ident(&temp_table_name), pk=quote_ident(&pk));

        let flush_interval = 10;
        let min_flush_rows = 50;
        let max_flush_rows = 1000;
        let mut start = Instant::now();
        let mut collected_row_cnt = 0;
        let mut processed_row_cnt = 0;
        let mut old_progress = 0;

        while let Some(rows) = rx.recv().await {
            for row in &rows {
                buf.extend_from_slice(row.0.as_bytes());
                buf.extend_from_slice("\t".as_bytes());
                if row.1.len() > 0 {
                    buf.extend_from_slice("{".as_bytes());
                    let row_str: String = row.1.iter().map(|&x| x.to_string() + ",").collect();
                    buf.extend_from_slice(row_str[0..row_str.len() - 1].as_bytes());
                    drop(row_str);
                    buf.extend_from_slice("}".as_bytes());
                } else {
                    buf.extend_from_slice("NULL".as_bytes());
                }
                buf.extend_from_slice("\n".as_bytes());
                collected_row_cnt += 1;
            }

            if buf.len() > chunk_size {
                writer_sink.send(buf.split().freeze()).await?
            }

            processed_row_cnt += rows.len();
            let progress = calculate_progress(item_count, processed_row_cnt);

            if progress > old_progress {
                old_progress = progress;
                logger.debug(&format!("Progress {progress}%",));
                if progress_cb.is_some() {
                    let cb = progress_cb.as_ref().unwrap();
                    cb(progress);
                }
            }

            drop(rows);

            if !stream {
                continue;
            }

            if collected_row_cnt >= max_flush_rows
                || (flush_interval <= start.elapsed().as_secs()
                    && collected_row_cnt >= min_flush_rows)
            {
                // if job is run in streaming mode
                // it will write results to target table each 10 seconds (if collected rows are
                // more than 50) or if collected row count is more than 1000 rows
                if !buf.is_empty() {
                    writer_sink.send(buf.split().freeze()).await?;
                }
                writer_sink.as_mut().finish().await?;
                transaction
                    .batch_execute(&format!(
                        "
                    {update_sql};
                    TRUNCATE TABLE {temp_table_name};
                "
                    ))
                    .await?;
                transaction.commit().await?;
                transaction = client.transaction().await?;
                writer_sink = Box::pin(
                    transaction
                        .copy_in(&format!("COPY {temp_table_name} FROM stdin"))
                        .await?,
                );
                collected_row_cnt = 0;
                start = Instant::now();
            }
        }

        // There might be a case when filter is provided manually
        // And `{column} IS NOT NULL` will be missing from the table
        // So we will check if the column is null in rust code before generating embedding
        // Thus the processed rows may be less than the actual estimated row count
        // And progress will not be 100
        if old_progress != 100 {
            logger.debug("Progress 100%");
            if progress_cb.is_some() {
                let cb = progress_cb.as_ref().unwrap();
                cb(100);
            }
        }

        if processed_row_cnt == 0 {
            return Ok(processed_row_cnt);
        }

        if !buf.is_empty() {
            writer_sink.send(buf.split().freeze()).await?
        }
        writer_sink.as_mut().finish().await?;
        transaction.execute(update_sql, &[]).await?;
        transaction.commit().await?;
        logger.info(&format!(
            "Embeddings exported to table {} under column {}",
            &table, &column
        ));

        Ok(processed_row_cnt)
    });

    Ok(handle)
}

async fn csv_exporter_worker(
    args: Arc<cli::EmbeddingArgs>,
    mut rx: Receiver<Vec<EmbeddingRecord>>,
    logger: Arc<Logger>,
) -> Result<JoinHandle<AnyhowUsizeResult>, anyhow::Error> {
    let handle = tokio::spawn(async move {
        let csv_path = args.out_csv.as_ref().unwrap();
        let mut wtr = Writer::from_path(&csv_path).unwrap();
        let mut processed_row_cnt = 0;
        while let Some(rows) = rx.recv().await {
            for row in &rows {
                let vector_string = &format!(
                    "{{{}}}",
                    row.1
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                );
                wtr.write_record(&[&row.0.to_string(), vector_string])
                    .unwrap();
                processed_row_cnt += rows.len();
            }
        }
        wtr.flush().unwrap();
        logger.info(&format!("Embeddings exported to {}", &csv_path));
        Ok(processed_row_cnt)
    });
    Ok(handle)
}

pub fn get_default_batch_size(model: &str) -> usize {
    match model {
        "clip/ViT-B-32-textual" => 2000,
        "clip/ViT-B-32-visual" => 50,
        "BAAI/bge-small-en" => 300,
        "BAAI/bge-base-en" => 100,
        "BAAI/bge-large-en" => 60,
        "jinaai/jina-embeddings-v2-small-en" => 500,
        "jinaai/jina-embeddings-v2-base-en" => 80,
        "intfloat/e5-base-v2" => 300,
        "intfloat/e5-large-v2" => 100,
        "llmrails/ember-v1" => 100,
        "thenlper/gte-base" => 1000,
        "thenlper/gte-large" => 800,
        "microsoft/all-MiniLM-L12-v2" => 1000,
        "naver/splade-v3" => 150,
        "microsoft/all-mpnet-base-v2" => 400,
        "transformers/multi-qa-mpnet-base-dot-v1" => 300,
        "openai/text-embedding-ada-002" => 500,
        "openai/text-embedding-3-small" => 500,
        "openai/text-embedding-3-large" => 500,
        "cohere/embed-english-v3.0"
        | "cohere/embed-multilingual-v3.0"
        | "cohere/embed-english-light-v3.0"
        | "cohere/embed-multilingual-light-v3.0"
        | "cohere/embed-english-v2.0"
        | "cohere/embed-english-light-v2.0"
        | "cohere/embed-multilingual-v2.0" => 5000,
        _ => 100,
    }
}

pub async fn create_embeddings_from_db(
    args: cli::EmbeddingArgs,
    track_progress: bool,
    progress_cb: Option<ProgressCbFn>,
    cancel_token: CancellationToken,
    logger: Option<Logger>,
) -> Result<(usize, usize), anyhow::Error> {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Embeddings", LogLevel::Debug)));
    logger.info("Lantern CLI - Create Embeddings");
    let args = Arc::new(args);
    let batch_size = args
        .batch_size
        .unwrap_or(get_default_batch_size(&args.model));

    logger.debug(&format!(
        "Model - {}, Visual - {}, Batch Size - {}",
        args.model, args.visual, batch_size
    ));

    let column = args.column.clone();
    let schema = args.schema.clone();
    let table = args.table.clone();
    let full_table_name = get_full_table_name(&schema, &table);

    let filter_sql = if args.filter.is_some() {
        format!("WHERE {}", args.filter.as_ref().unwrap())
    } else {
        format!("WHERE {column} IS NOT NULL", column = quote_ident(&column))
    };

    let limit_sql = if args.limit.is_some() {
        format!("LIMIT {}", args.limit.as_ref().unwrap())
    } else {
        "".to_owned()
    };

    let uri = append_params_to_uri(&args.uri, CONNECTION_PARAMS);
    let (mut client, connection) = tokio_postgres::connect(&uri, NoTls).await?;

    tokio::spawn(async move { connection.await });

    let mut item_cnt = 0;
    if track_progress {
        item_cnt = estimate_count(
            &mut client,
            &full_table_name,
            &filter_sql,
            &limit_sql,
            logger.clone(),
        )
        .await?;
    }
    // Create channel that will send the database rows to embedding worker
    let (producer_tx, producer_rx): (Sender<Vec<Row>>, Receiver<Vec<Row>>) = mpsc::channel(1);
    let (embedding_tx, embedding_rx): (
        Sender<Vec<EmbeddingRecord>>,
        Receiver<Vec<EmbeddingRecord>>,
    ) = mpsc::channel(1);

    // Create exporter based on provided args
    // For now we only have csv and db exporters
    let exporter_handle = if args.out_csv.is_some() {
        csv_exporter_worker(args.clone(), embedding_rx, logger.clone()).await?
    } else {
        db_exporter_worker(
            &uri,
            args.clone(),
            embedding_rx,
            item_cnt,
            progress_cb,
            logger.clone(),
        )
        .await?
    };

    let (exporter_result, embedding_result, producer_result) = tokio::join!(
        exporter_handle,
        embedding_worker(
            args.clone(),
            producer_rx,
            embedding_tx,
            cancel_token,
            logger.clone(),
        ),
        producer_worker(
            &mut client,
            &args.pk,
            &args.column,
            &full_table_name,
            &filter_sql,
            &limit_sql,
            batch_size,
            producer_tx,
        ),
    );

    producer_result?;
    let processed_tokens = embedding_result?;
    let processed_rows = exporter_result??;
    Ok((processed_rows, processed_tokens))
}

pub async fn show_available_models(
    args: &cli::ShowModelsArgs,
    logger: Option<Logger>,
) -> AnyhowVoidResult {
    let logger = logger.unwrap_or(Logger::new("Lantern Embeddings", LogLevel::Info));
    logger.info("Available Models\n");
    let runtime = EmbeddingRuntime::new(&args.runtime, None, &args.runtime_params)?;
    logger.print_raw(&runtime.get_available_models().await.0);
    Ok(())
}

pub fn show_available_runtimes(logger: Option<Logger>) -> AnyhowVoidResult {
    let logger = logger.unwrap_or(Logger::new("Lantern Embeddings", LogLevel::Info));
    let mut runtimes_str = get_available_runtimes().join("\n");
    runtimes_str.push_str("\n");
    logger.info("Available Runtimes\n");
    logger.print_raw(&runtimes_str);
    Ok(())
}
