use crate::logger::{LogLevel, Logger};
use crate::types::*;
use crate::utils::{append_params_to_uri, get_full_table_name, quote_ident};
use bytes::BytesMut;
use core::get_available_runtimes;
use futures::SinkExt;
use rand::Rng;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use tokio_postgres::{Client, NoTls, Row};

use self::cli::EmbeddingJobType;
use self::core::EmbeddingRuntime;

pub mod cli;
pub mod core;
pub mod measure_speed;

struct EmbeddingRecord {
    pk: String,
    record: BytesMut,
}

impl EmbeddingRecord {
    fn from_vec(pk: String, value: Vec<f32>) -> EmbeddingRecord {
        let mut buf = BytesMut::with_capacity(4);

        if value.len() > 0 {
            let chunk_size = 1024 * 1024; // 1 MB
            buf = BytesMut::with_capacity(chunk_size);
            buf.extend_from_slice("{".as_bytes());
            let row_str: String = value.iter().map(|&x| x.to_string() + ",").collect();
            buf.extend_from_slice(row_str[0..row_str.len() - 1].as_bytes());
            drop(row_str);
            buf.extend_from_slice("}".as_bytes());
        } else {
            buf.extend_from_slice("NULL".as_bytes());
        }

        EmbeddingRecord { pk, record: buf }
    }

    #[allow(dead_code)]
    fn from_string(pk: String, value: String) -> EmbeddingRecord {
        let buf: BytesMut;
        if value.len() > 0 {
            buf = BytesMut::from(
                value
                    .replace('\\', "\\\\")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t")
                    .as_bytes(),
            )
        } else {
            buf = BytesMut::from("NULL".as_bytes());
        }

        EmbeddingRecord { pk, record: buf }
    }
}

static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

pub fn get_try_cast_fn_sql(schema: &str) -> String {
    format!(
        "
CREATE OR REPLACE FUNCTION {schema}.ldb_try_cast(_in text, INOUT _out ANYELEMENT)
  LANGUAGE plpgsql AS
$func$
BEGIN
   EXECUTE format('SELECT %L::%s', $1, pg_typeof(_out))
   INTO  _out;
EXCEPTION WHEN others THEN
END
$func$;",
        schema = quote_ident(schema)
    )
}

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
    job_type: EmbeddingJobType,
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

                let mut inputs: Vec<&str> = Vec::with_capacity(rows.len());
                let mut input_ids: Vec<String> = Vec::with_capacity(rows.len());

                for row in &rows {
                    if let Ok(Some(src_data)) = row.try_get::<usize, Option<&str>>(1) {
                        if src_data.trim() != "" {
                            inputs.push(src_data);
                            input_ids.push(row.get::<usize, String>(0));
                        }
                    }
                }

                if inputs.len() == 0 {
                    continue;
                }

                let mut response_data = Vec::with_capacity(rows.len());

                match job_type {
                    EmbeddingJobType::EmbeddingGeneration => {
                        let embedding_response = runtime.process(&model, &inputs).await?;
                        processed_tokens += embedding_response.processed_tokens;
                        let mut embeddings = embedding_response.embeddings;

                        count += embeddings.len();
                        for _ in 0..embeddings.len() {
                            response_data.push(EmbeddingRecord::from_vec(input_ids.pop().unwrap(), embeddings.pop().unwrap()))
                        }
                    },
                    EmbeddingJobType::Completion => {
                        let embedding_response = runtime.batch_completion(&model, &inputs).await?;
                        processed_tokens += embedding_response.processed_tokens;
                        let mut messages = embedding_response.messages;
                        count += messages.len();
                        for _ in 0..messages.len() {
                            response_data.push(EmbeddingRecord::from_string(input_ids.pop().unwrap(), messages.pop().unwrap()))
                        }
                    }
                }

                let duration = start.elapsed().as_secs();
                // avoid division by zero error
                let duration = if duration > 0 { duration } else { 1 };
                logger.debug(&format!(
                    "Generated {} embeddings - speed {} emb/s",
                    count,
                    count / duration as usize
                ));

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

fn get_update_query_and_col_type(
    args: Arc<cli::EmbeddingArgs>,
    full_table_name: &str,
    temp_table_name: &str,
    column_type: &str,
) -> (String, String) {
    let column = args.out_column.clone();
    let cast_fn_name = get_full_table_name(&args.internal_schema, "ldb_try_cast");
    let mut tmp_col_type = column_type.to_owned();
    let mut type_check_sql = "".to_owned();
    let mut failed_rows_sql = "SELECT 1 WHERE FALSE;".to_owned(); // nop operation
    let mut temp_table_subquery = temp_table_name.to_owned(); // nop operation

    if let Some(failed_rows_table) = &args.failed_rows_table {
        let failed_rows_table = get_full_table_name(&args.internal_schema, failed_rows_table);
        failed_rows_sql = format!(
            "
            INSERT INTO {failed_rows_table} (job_id, row_id, value)
            SELECT {job_id} as job_id, row_id, value FROM failed_rows;
        ",
            job_id = args.job_id,
        );

        temp_table_subquery = format!(
            "
            (
            SELECT * FROM {temp_table_name} tmp
            WHERE NOT EXISTS (
                SELECT 1 FROM failed_rows fr WHERE fr.row_id = tmp.{pk}
            )
        )
",
            temp_table_name = quote_ident(&temp_table_name),
            pk = quote_ident(&args.pk)
        )
    }

    if args.check_column_type {
        tmp_col_type = "TEXT".to_owned();
        type_check_sql = format!(
            "
                 failed_rows AS (
                    DELETE FROM {temp_table_name} src
                    WHERE {cast_fn_name}(src.{column}, NULL::{column_type}) IS NULL
                    RETURNING src.{pk} AS row_id, src.{column} AS value
                ),
        ",
            column = quote_ident(&column),
            temp_table_name = quote_ident(&temp_table_name),
            pk = quote_ident(&args.pk)
        );
    }

    /*
    * For embedding jobs the temp table will be created with REAL[] type
    * And the type_check_sql with failed_rows_sql will be set to empty string
    * So we will just perform update from tmp to src truncating the temp table
    * Query will be like this:
    *   WITH updated_rows AS (
            UPDATE "public"."_completion_test_openai_failed_rows" dst
            SET "chars" = src."chars"::TEXT[]
            FROM "_lantern_tmp_861" src
            WHERE src."id" = dst."id"
        )
        SELECT 1 WHERE FALSE;
        TRUNCATE TABLE _lantern_tmp_861;
    * ---
    * For completion jobs it is recommended to set the check_column_type and
    * failed_rows_table arguments.
    * If the check_column_type will be set, we will try to delete the rows from tmp table
    * Which do not pass the type cast (this can be because LLMs can return invalid data).
    * Then we will perform the update operation with the remaining rows which can be casted
    * correctly.
    * Then the failed rows will be inserted into the failed_rows_table, so there will be
    * visibility about the rows which were failed to cast
    *
    * Query will be like this:
    *   WITH failed_rows AS (
            DELETE FROM "_lantern_tmp_861" src
            WHERE ldb_try_cast(src."chars", NULL::TEXT[]) IS NULL
            RETURNING src."id" AS row_id, src."chars" AS value
        ),
        updated_rows AS (
            UPDATE "public"."_completion_test_openai_failed_rows" dst
            SET "chars" = src."chars"::TEXT[]
            FROM (
                SELECT * FROM "_lantern_tmp_861"
                WHERE NOT EXISTS (
                    SELECT 1 FROM failed_rows fr WHERE fr.row_id = "_lantern_tmp_861"."id"
                )
            ) src
            WHERE src."id" = dst."id"
        )
        INSERT INTO embedding_failre_info (job_id, row_id, value)
            SELECT {args.job_id} as job_id, row_id, value FROM failed_rows;
        TRUNCATE TABLE _lantern_tmp_861;
    * */
    let update_sql = format!(
        "
                WITH {type_check_sql} 
                updated_rows AS (
                    UPDATE {full_table_name} dst
                    SET {column} = src.{column}::{column_type}
                    FROM {temp_table_subquery} src
                    WHERE src.{pk} = dst.{pk}
                ) 
                {failed_rows_sql}
                TRUNCATE TABLE {temp_table_name};
        ",
        column = quote_ident(&column),
        temp_table_name = quote_ident(&temp_table_name),
        pk = quote_ident(&args.pk)
    );

    (update_sql, tmp_col_type)
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
    column_type: String,
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
                    "ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS {column} {column_type}",
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

    if args.create_cast_fn {
        transaction
            .execute(&get_try_cast_fn_sql(&args.internal_schema), &[])
            .await?;
    }

    let (update_sql, tmp_col_type) = get_update_query_and_col_type(
        args.clone(),
        &full_table_name,
        &temp_table_name,
        &column_type,
    );

    transaction.commit().await?;
    let handle = tokio::spawn(async move {
        let transaction = client.transaction().await?;
        transaction
            .execute(
                &format!(
                    "CREATE TEMPORARY TABLE {temp_table_name} AS SELECT {pk}, {column}::{tmp_col_type} FROM {full_table_name} LIMIT 0",
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
                    "COPY {temp_table_name} FROM stdin WITH NULL AS 'NULL' "
                ))
                .await?,
        );
        let chunk_size = 1024 * 1024 * 10; // 10 MB
        let mut buf = BytesMut::with_capacity(chunk_size * 2);

        let flush_interval = 10;
        let min_flush_rows = 50;
        let max_flush_rows = 1000;
        let mut start = Instant::now();
        let mut collected_row_cnt = 0;
        let mut processed_row_cnt = 0;
        let mut old_progress = 0;

        while let Some(rows) = rx.recv().await {
            for row in &rows {
                buf.extend_from_slice(row.pk.as_bytes());
                buf.extend_from_slice("\t".as_bytes());
                buf.extend_from_slice(&row.record);
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
                let copy_start = Instant::now();
                if !buf.is_empty() {
                    writer_sink.send(buf.split().freeze()).await?;
                }
                writer_sink.as_mut().finish().await?;
                transaction.batch_execute(&update_sql).await?;
                transaction.commit().await?;
                transaction = client.transaction().await?;

                let duration = copy_start.elapsed().as_millis();
                logger.debug(&format!("Copied {collected_row_cnt} rows in {duration}ms"));

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

        let copy_start = Instant::now();
        if !buf.is_empty() {
            writer_sink.send(buf.split().freeze()).await?
        }
        writer_sink.as_mut().finish().await?;
        transaction.batch_execute(&update_sql).await?;
        transaction.commit().await?;
        let duration = copy_start.elapsed().as_millis();
        logger.debug(&format!("Copied {collected_row_cnt} rows in {duration}ms"));
        logger.info(&format!(
            "Embeddings exported to table {} under column {}",
            &table, &column
        ));

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
        // Openai Models
        "text-embedding-ada-002" | "text-embedding-3-small" | "text-embedding-3-large" => 500,
        // Cohere Models
        "embed-english-v3.0"
        | "embed-multilingual-v3.0"
        | "embed-english-light-v3.0"
        | "embed-multilingual-light-v3.0"
        | "embed-english-v2.0"
        | "embed-english-light-v2.0"
        | "embed-multilingual-v2.0" => 5000,
        // Completion models
        "gpt-4" | "gpt-4o" | "gpt-4-turbo" => 2,
        "gpt-4o-mini" => 10,
        _ => 100,
    }
}

fn get_default_column_type(job_type: &EmbeddingJobType) -> String {
    match job_type {
        &EmbeddingJobType::Completion => "TEXT".to_owned(),
        &EmbeddingJobType::EmbeddingGeneration => "REAL[]".to_owned(),
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
    let job_type = args
        .job_type
        .clone()
        .unwrap_or(EmbeddingJobType::EmbeddingGeneration);
    let column_type = args
        .column_type
        .clone()
        .unwrap_or(get_default_column_type(&job_type));
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
    let exporter_handle = db_exporter_worker(
        &uri,
        args.clone(),
        embedding_rx,
        item_cnt,
        column_type,
        progress_cb,
        logger.clone(),
    )
    .await?;

    let (exporter_result, embedding_result, producer_result) = tokio::join!(
        exporter_handle,
        embedding_worker(
            args.clone(),
            producer_rx,
            embedding_tx,
            job_type,
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
    logger.print_raw(
        &runtime
            .get_available_models(
                args.job_type
                    .clone()
                    .unwrap_or(EmbeddingJobType::EmbeddingGeneration),
            )
            .await
            .0,
    );
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
