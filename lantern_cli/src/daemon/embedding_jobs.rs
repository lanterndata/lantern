/*
    Embedding Jobs Table should have the following structure:
    CREATE TABLE "public"."embedding_generation_jobs" (
        "id" SERIAL PRIMARY KEY,
        "database_id" text NOT NULL,
        "db_connection" text NOT NULL,
        "schema" text NOT NULL,
        "table" text NOT NULL,
        "pk" text NOT NULL,
        "runtime" text NOT NULL,
        "runtime_params" jsonb,
        "src_column" text NOT NULL,
        "dst_column" text NOT NULL,
        "embedding_model" text NOT NULL,
        "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "canceled_at" timestamp,
        "init_started_at" timestamp,
        "init_finished_at" timestamp,
        "init_failed_at" timestamp,
        "init_failure_reason" text,
        "init_progress" int2 DEFAULT 0,
        "failed_requests" bigint DEFAULT 0,
        "failed_rows" bigint DEFAULT 0
    );
*/

use super::cli;
use super::client_embedding_jobs::toggle_client_job;
use super::helpers::{
    db_notification_listener, get_missing_rows_filter, remove_job_handle, schedule_job_retry,
    set_job_handle, startup_hook,
};
use super::types::{
    EmbeddingJob, JobCancellationHandlersMap, JobInsertNotification, JobUpdateNotification,
    VoidFuture,
};
use crate::embeddings::cli::EmbeddingArgs;
use crate::logger::Logger;
use crate::utils::{get_full_table_name, quote_ident};
use crate::{embeddings, types::*};
use futures::future;
use std::collections::HashMap;
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

const EMB_LOCK_TABLE_NAME: &'static str = "_lantern_emb_job_locks";

lazy_static! {
    static ref JOBS: JobCancellationHandlersMap = RwLock::new(HashMap::new());
    static ref JOB_BATCHING_HASHMAP: Arc<Mutex<HashMap<i32, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

async fn lock_row(
    client: Arc<Client>,
    lock_table_name: &str,
    logger: Arc<Logger>,
    job_id: i32,
    row_id: &str,
) -> bool {
    let res = client
        .execute(
            &format!("INSERT INTO {lock_table_name} (job_id, row_id) VALUES ($1, $2)"),
            &[&job_id, &row_id],
        )
        .await;

    if let Err(e) = res {
        if !e.to_string().to_lowercase().contains("duplicate") {
            logger.error(&format!(
                "Error while locking row: {row_id} for job: {job_id} : {e}"
            ));
        }
        return false;
    }
    true
}

#[allow(dead_code)]
async fn lock_rows(
    client: Arc<Client>,
    lock_table_name: &str,
    logger: Arc<Logger>,
    job_id: i32,
    row_ids: &Vec<String>,
) -> bool {
    let mut statement = format!("INSERT INTO {lock_table_name} (job_id, row_id) VALUES ");

    let mut idx = 0;
    let values: Vec<&(dyn ToSql + Sync)> = row_ids
        .iter()
        .flat_map(|row_id| {
            let comma = if idx == row_ids.len() - 1 { "" } else { "," };
            statement = format!(
                "{statement}({job_id}, ${param_num}){comma}",
                param_num = idx + 1
            );
            idx += 1;
            [row_id as &(dyn ToSql + Sync)]
        })
        .collect();

    let res = client.execute(&statement, &values[..]).await;

    if let Err(e) = res {
        if !e.to_string().to_lowercase().contains("duplicate") {
            logger.error(&format!(
                "Error while locking rows: {:?} for job: {job_id} : {e}",
                row_ids
            ));
        }
        return false;
    }
    true
}

async fn unlock_rows(
    client: Arc<Client>,
    lock_table_name: &str,
    logger: Arc<Logger>,
    job_id: i32,
    row_ids: &Vec<String>,
) {
    let mut row_ids_query = "".to_owned();
    let mut params: Vec<&(dyn ToSql + Sync)> = row_ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            let comma = if idx < row_ids.len() - 1 { "," } else { "" };
            row_ids_query = format!("{row_ids_query}${}{comma}", idx + 1);
            id as &(dyn ToSql + Sync)
        })
        .collect();
    params.push(&job_id);
    let res = client
        .execute(
            &format!("DELETE FROM {lock_table_name} WHERE job_id=${job_id_pos} AND row_id IN ({row_ids_query})", job_id_pos=params.len()),
            &params,
        )
        .await;

    if let Err(e) = res {
        logger.error(&format!(
            "Error while unlocking rows: {:?} for job: {job_id} : {e}",
            row_ids
        ));
    }
}

async fn stream_job(
    logger: Arc<Logger>,
    main_client: Arc<Client>,
    job_queue_tx: Sender<EmbeddingJob>,
    notifications_tx: UnboundedSender<JobInsertNotification>,
    jobs_table_name: String,
    _lock_table_name: String,
    job: EmbeddingJob,
) -> AnyhowVoidResult {
    let top_logger = logger.clone();
    let job_id = job.id;
    let task = tokio::spawn(async move {
        logger.info(&format!("Start streaming job {}", job.id));
        // Enable triggers for job
        if job.is_init {
            toggle_client_job(
                job.id.clone(),
                job.db_uri.clone(),
                job.pk.clone(),
                job.column.clone(),
                job.out_column.clone(),
                job.table.clone(),
                job.schema.clone(),
                logger.level.clone(),
                Some(notifications_tx.clone()),
                true,
            )
            .await?;
        }

        let (job_error_tx, mut job_error_rx): (Sender<String>, Receiver<String>) = mpsc::channel(1);
        set_job_handle(&JOBS, job.id, job_error_tx).await?;

        let connection_logger = logger.clone();

        let (mut job_client, connection) = tokio_postgres::connect(&job.db_uri, NoTls).await?;
        let job_id = job.id;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                connection_logger.error(&format!("Error while streaming job: {job_id}. {e}"));
            }
        });

        let column = &job.column;
        let out_column = &job.out_column;
        let schema = &job.schema;
        let table = &job.table;
        let full_table_name = get_full_table_name(schema, table);

        let filter_sql = format!(
            "WHERE {out_column} IS NULL AND {column} IS NOT NULL AND {column} != ''",
            column = quote_ident(column)
        );

        let transaction = job_client.transaction().await?;

        transaction
            .execute("SET idle_in_transaction_session_timeout=3600000", &[])
            .await?;
        let total_rows = transaction
            .query_one(
                &format!("SELECT COUNT(*) FROM {full_table_name} {filter_sql};"),
                &[],
            )
            .await?;
        let total_rows: i64 = total_rows.get(0);
        let total_rows = total_rows as usize;

        if total_rows == 0 {
            return Ok(());
        }

        let portal = transaction
            .bind(
                &format!(
                    "SELECT {pk}::text FROM {full_table_name} {filter_sql};",
                    pk = quote_ident(&job.pk)
                ),
                &[],
            )
            .await?;
        let mut progress = 0;
        let mut processed_rows = 0;

        let batch_size = embeddings::get_default_batch_size(&job.model) as i32;
        loop {
            // Check if job was errored or cancelled
            if let Ok(err_msg) = job_error_rx.try_recv() {
                logger.error(&format!("Error received on job {job_id}. {err_msg}"));
                if job.is_init {
                    // set init failed at if this is init job
                    main_client.execute(&format!("UPDATE {jobs_table_name} SET init_failed_at=NOW(), updated_at=NOW(), init_failure_reason=$1 WHERE id=$2"), &[&err_msg.to_string(), &job_id]).await?;
                }
                toggle_client_job(
                    job.id.clone(),
                    job.db_uri.clone(),
                    job.pk.clone(),
                    job.column.clone(),
                    job.out_column.clone(),
                    job.table.clone(),
                    job.schema.clone(),
                    logger.level.clone(),
                    Some(notifications_tx.clone()),
                    false,
                )
                .await?;
                anyhow::bail!(err_msg);
            }

            // poll batch_size rows from portal and send it to embedding thread via channel
            let rows = transaction.query_portal(&portal, batch_size).await?;

            if rows.len() == 0 {
                break;
            }

            let mut streamed_job = job.clone();
            let row_ids: Vec<String> = rows.iter().map(|r| r.get::<usize, String>(0)).collect();
            // If this is not init job, but startup job to generate missing rows
            // We will lock the row ids, so if 2 daemons will be started at the same time
            // Same rows won't be processed by both of them
            // TODO:: This check is now commented because
            // There might be case when the job will be stopped abnoarmally and
            // Rows will stay locked, so in next run the missed rows will be considered as locked
            // And won't be taken to be processed
            // streamed_job.set_row_ids(row_ids);
            // if !job.is_init
            //     && !lock_rows(
            //         main_client.clone(),
            //         &lock_table_name,
            //         logger.clone(),
            //         job_id,
            //         streamed_job.row_ids.as_ref().unwrap(),
            //     )
            //     .await
            // {
            //     logger.warn("Another daemon instance is already processing job {job_id}. Exitting streaming loop");
            //     break;
            // }

            streamed_job.set_id_filter(&row_ids);

            processed_rows += row_ids.len();
            if job.is_init {
                let new_progress = ((processed_rows as f32 / total_rows as f32) * 100.0) as u8;

                if new_progress > progress {
                    progress = new_progress;
                    streamed_job.set_report_progress(progress);
                }
            }

            if processed_rows == total_rows {
                streamed_job.set_is_last_chunk(true);
            }

            job_queue_tx.send(streamed_job).await?;
        }

        Ok::<(), anyhow::Error>(())
    });

    if let Err(e) = task.await? {
        top_logger.error(&format!("Error while streaming job {job_id}: {e}"));
        remove_job_handle(&JOBS, job_id).await?;
    }

    Ok(())
}

async fn embedding_worker(
    mut job_queue_rx: Receiver<EmbeddingJob>,
    job_queue_tx: Sender<EmbeddingJob>,
    db_uri: String,
    schema: String,
    internal_schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);
    let jobs_table_name = Arc::new(get_full_table_name(&schema, &table));

    tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
        tokio::spawn(async move { connection.await.unwrap() });
        let client = Arc::new(client);
        logger.info("Embedding worker started");
        while let Some(job) = job_queue_rx.recv().await {
            let client_ref = client.clone();
            let orig_job_clone = job.clone();
            let job = Arc::new(job);
            let schema_ref = schema.clone();

            let task_logger = Logger::new(
                &format!("Embedding Job {}|{:?}", job.id, job.runtime),
                logger.level.clone(),
            );
            let job_clone = job.clone();

            let result = crate::embeddings::create_embeddings_from_db(
                EmbeddingArgs {
                    pk: job.pk.clone(),
                    model: job_clone.model.clone(),
                    schema: job_clone.schema.clone(),
                    uri: job_clone.db_uri.clone(),
                    out_uri: Some(job_clone.db_uri.clone()),
                    table: job_clone.table.clone(),
                    out_table: Some(job_clone.table.clone()),
                    column: job_clone.column.clone(),
                    out_column: job_clone.out_column.clone(),
                    batch_size: job_clone.batch_size,
                    runtime: job_clone.runtime.clone(),
                    runtime_params: job_clone.runtime_params.clone(),
                    visual: false,
                    stream: true,
                    create_column: false,
                    out_csv: None,
                    filter: job_clone.filter.clone(),
                    limit: None,
                },
                false,
                None,
                None,
                Some(task_logger),
            );

            match result {
                Ok((processed_rows, processed_tokens)) => {
                    if processed_tokens > 0 {
                        let fn_name = get_full_table_name(
                            &schema_ref,
                            "increment_embedding_usage_and_tokens",
                        );
                        let res = client_ref
                            .execute(
                                &format!(
                                    "SELECT {fn_name}({job_id},{usage},{tokens}::bigint)",
                                    job_id = job.id,
                                    usage = processed_rows,
                                    tokens = processed_tokens
                                ),
                                &[],
                            )
                            .await;

                        if let Err(e) = res {
                            logger.error(&format!(
                                "Error while updating usage for {job_id}: {e}",
                                job_id = job.id
                            ));
                        }
                    }

                    if let Some(progress) = job.report_progress {
                        let update_statement = if job.is_last_chunk {
                            "init_finished_at=NOW(), updated_at=NOW(), init_progress=100".to_owned()
                        } else {
                            format!("init_progress={progress}")
                        };

                        client_ref
                            .execute(
                                &format!(
                                    "UPDATE {jobs_table_name} SET {update_statement} WHERE id=$1"
                                ),
                                &[&job.id],
                            )
                            .await?;
                    }

                    if job.is_last_chunk {
                        remove_job_handle(&JOBS, job.id).await?;
                    }
                }
                Err(e) => {
                    logger.error(&format!(
                        "Error while executing job {job_id}: {e}",
                        job_id = job.id
                    ));
                    if !job.is_init {
                        schedule_job_retry(
                            logger.clone(),
                            orig_job_clone,
                            job_queue_tx.clone(),
                            Duration::from_secs(300),
                        )
                        .await?;

                        let fn_name =
                            get_full_table_name(&schema_ref, "increment_embedding_failures");

                        let row_count = if job.row_ids.is_some() {
                            job.row_ids.as_ref().unwrap().len()
                        } else {
                            0
                        };

                        let res =
                            client_ref
                                .execute(
                                    &format!(
                                        "SELECT {fn_name}({job_id},{row_count})",
                                        job_id = job.id,
                                    ),
                                    &[],
                                )
                                .await;

                        if let Err(e) = res {
                            logger.error(&format!(
                                "Error while updating failures for {job_id}: {e}",
                                job_id = job.id
                            ));
                        }
                    } else {
                        // Send error via channel, so init streaming task will catch that
                        let jobs = JOBS.read().await;
                        if let Some(tx) = jobs.get(&job.id) {
                            tx.send(e.to_string()).await?;
                        }
                        drop(jobs);
                    }
                }
            }

            if let Some(row_ids) = &job.row_ids {
                // If this is a job triggered from notification (new row inserted or row was updated)
                // Then we need to remove the entries from lock table for this rows
                // As we are using table ctid for lock key, after table VACUUM the ctids may repeat
                // And if new row will be inserted with previously locked ctid
                // it won't be taken by daemon
                let lock_table_name = get_full_table_name(&internal_schema, EMB_LOCK_TABLE_NAME);
                unlock_rows(
                    client_ref,
                    &lock_table_name,
                    logger.clone(),
                    job.id,
                    row_ids,
                )
                .await;
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

async fn collect_pending_jobs(
    client: Arc<Client>,
    update_notification_tx: UnboundedSender<JobUpdateNotification>,
    table: String,
) -> AnyhowVoidResult {
    // Get all pending jobs and set them in queue
    let rows = client
        .query(
            &format!("SELECT id FROM {table} WHERE init_failed_at IS NULL and canceled_at IS NULL ORDER BY id"),
            &[],
        )
        .await?;

    for row in rows {
        // TODO This can be optimized
        update_notification_tx.send(JobUpdateNotification {
            id: row.get::<usize, i32>(0).to_owned(),
            generate_missing: true,
        })?;
    }

    Ok(())
}

async fn job_insert_processor(
    db_uri: String,
    mut notifications_rx: UnboundedReceiver<JobInsertNotification>,
    notifications_tx: UnboundedSender<JobInsertNotification>,
    job_tx: Sender<EmbeddingJob>,
    schema: String,
    lock_table_schema: String,
    table: String,
    data_path: &'static str,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    // This function will have 2 running tasks
    // The first task will receive notification which can come from 4 sources
    // 1 - a new job is inserted into jobs table
    // 2 - after starting daemon job table scan was happened and we should check for any inserts we
    // missed while daemon was offline
    // 3 - job's canceled_at was changed to NULL, which means we should reactivate the job and
    //   generate embeddings for the rows which were inserted while job had canceled_at
    // 4 - a new row was inserted into client database and we should generate an embedding for that
    //   row
    // 1-3 cases works similiar we just send a new job record to embedding worker receiver which
    // will start generating embeddings for that job
    // for the 4th case we will lock the row (insert a record in our lock table) and set that row
    // in a hashmap item for that job (e.g { [job_id] => Vec<row_id1, row_id2> })
    // each 10 sconds the second running task the collector task will drain the hashmap and create
    // batch jobs for the rows. This will optimize embedding generation as if there will be lots of
    // inserts to the table between 10 seconds all that rows will be batched.
    let full_table_name = Arc::new(get_full_table_name(&schema, &table));
    // TODO:: Select pk here
    let job_query_sql = Arc::new(format!("SELECT id, db_connection as db_uri, src_column as \"column\", dst_column, \"table\", \"schema\", embedding_model as model, runtime, runtime_params::text FROM {0}", &full_table_name));

    let db_uri_r1 = db_uri.clone();
    let full_table_name_r1 = full_table_name.clone();
    let job_query_sql_r1 = job_query_sql.clone();
    let job_tx_r1 = job_tx.clone();
    let logger_r1 = logger.clone();
    let lock_table_name = Arc::new(get_full_table_name(&lock_table_schema, EMB_LOCK_TABLE_NAME));
    let job_batching_hashmap_r1 = JOB_BATCHING_HASHMAP.clone();

    let insert_processor_task = tokio::spawn(async move {
        let (insert_client, connection) = tokio_postgres::connect(&db_uri_r1, NoTls).await?;
        let insert_client = Arc::new(insert_client);
        tokio::spawn(async move { connection.await.unwrap() });
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            if let Some(row_id) = notification.row_id {
                // Do this in a non-blocking way to not block collecting of updates while locking
                let client_r1 = insert_client.clone();
                let logger_r1 = logger_r1.clone();
                let job_batching_hashmap_r1 = job_batching_hashmap_r1.clone();
                let lock_table_name = lock_table_name.clone();
                tokio::spawn(async move {
                    // Single row update received from client job, lock row and add to batching map
                    let status = lock_row(
                        client_r1.clone(),
                        &lock_table_name,
                        logger_r1.clone(),
                        id,
                        &row_id,
                    )
                    .await;

                    if status {
                        // this means locking was successfull and row will be processed
                        // from this daemon
                        let mut jobs = job_batching_hashmap_r1.lock().await;
                        let job = jobs.get_mut(&id);

                        if let Some(job_vec) = job {
                            job_vec.push(row_id.to_owned());
                        } else {
                            jobs.insert(id, vec![row_id.to_owned()]);
                        }

                        drop(jobs);
                    }
                });

                continue;
            }

            if notification.init && !notification.generate_missing {
                // Only update init time if this is the first time job is being executed
                let updated_count = insert_client.execute(&format!("UPDATE {0} SET init_started_at=NOW() WHERE init_started_at IS NULL AND id=$1", &full_table_name_r1), &[&id]).await?;
                if updated_count == 0 {
                    continue;
                }
            }

            let job_result = insert_client
                .query_one(
                    &format!("{job_query_sql_r1} WHERE id=$1 AND canceled_at IS NULL"),
                    &[&id],
                )
                .await;

            if let Ok(row) = job_result {
                let job = EmbeddingJob::new(row, data_path);

                if let Err(e) = &job {
                    logger_r1.error(&format!("Error while creating job {id}: {e}",));
                    continue;
                }
                let mut job = job.unwrap();
                job.set_is_init(notification.init);
                if let Some(filter) = notification.filter {
                    job.set_filter(&filter);
                }
                // TODO:: Check if passing insert_client does not make deadlocks
                tokio::spawn(stream_job(
                    logger_r1.clone(),
                    insert_client.clone(),
                    job_tx_r1.clone(),
                    notifications_tx.clone(),
                    full_table_name_r1.to_string(),
                    lock_table_name.to_string(),
                    job,
                ));
            } else {
                logger_r1.error(&format!(
                    "Error while getting job {id}: {}",
                    job_result.err().unwrap()
                ));
            }
        }
        Ok(()) as AnyhowVoidResult
    });

    let job_batching_hashmap = JOB_BATCHING_HASHMAP.clone();
    let batch_collector_task = tokio::spawn(async move {
        let (batch_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
        tokio::spawn(async move { connection.await.unwrap() });
        loop {
            let mut job_map = job_batching_hashmap.lock().await;
            let jobs: Vec<(i32, Vec<String>)> = job_map.drain().collect();
            // release lock
            drop(job_map);

            for (job_id, row_ids) in jobs {
                let job_result = batch_client
                    .query_one(
                        &format!("{job_query_sql} WHERE id=$1 AND canceled_at IS NULL"),
                        &[&job_id],
                    )
                    .await;
                if let Err(e) = job_result {
                    logger.error(&format!("Error while getting job {job_id}: {}", e));
                    continue;
                }
                let row = job_result.unwrap();
                let job = EmbeddingJob::new(row, data_path);

                if let Err(e) = &job {
                    logger.error(&format!("Error while creating job {job_id}: {e}"));
                    continue;
                }
                let mut job = job.unwrap();
                job.set_is_init(false);
                let rows_len = row_ids.len();
                job.set_id_filter(&row_ids);
                job.set_row_ids(row_ids);
                logger.debug(&format!(
                    "Sending batch job {job_id} (len: {rows_len}) to embedding_worker"
                ));

                // Send in new tokio task to avoid blocking loop
                let logger = logger.clone();
                let job_tx = job_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = job_tx.send(job).await {
                        logger.error(&format!("Failed to send batch job: {job_id}: {e}"));
                    }
                });
            }

            // collect jobs every 10 seconds
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    let (r1, _) = tokio::try_join!(insert_processor_task, batch_collector_task)?;
    r1?;

    Ok(())
}

async fn job_update_processor(
    db_uri: String,
    mut update_queue_rx: UnboundedReceiver<JobUpdateNotification>,
    job_insert_queue_tx: UnboundedSender<JobInsertNotification>,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    tokio::spawn(async move {
        while let Some(notification) = update_queue_rx.recv().await {
            let (client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
            tokio::spawn(async move { connection.await.unwrap() });
            let full_table_name =  get_full_table_name(&schema, &table);
            let id = notification.id;
            // TODO:: Select pk here
            let row = client.query_one(&format!("SELECT db_connection as db_uri, dst_column, src_column as \"column\", \"table\", \"schema\", canceled_at, init_finished_at FROM {0} WHERE id=$1", &full_table_name), &[&id]).await?;
            let src_column = row.get::<&str, String>("column").to_owned();
            let out_column = row.get::<&str, String>("dst_column").to_owned();

            let canceled_at: Option<SystemTime> = row.get("canceled_at");
            let init_finished_at: Option<SystemTime> = row.get("init_finished_at");

            if !notification.generate_missing {
              logger.debug(&format!("Update job {id}: is_canceled: {}", canceled_at.is_some()));
            }

            if init_finished_at.is_some() {
              toggle_client_job(id, "id".to_owned(), row.get::<&str, String>("db_uri").to_owned(), row.get::<&str, String>("column").to_owned(), row.get::<&str, String>("dst_column").to_owned(), row.get::<&str, String>("table").to_owned(), row.get::<&str, String>("schema").to_owned(), logger.level.clone(), Some(job_insert_queue_tx.clone()), canceled_at.is_none()).await?;
            } else if canceled_at.is_some() {
                // Cancel ongoing job
                let jobs = JOBS.read().await;
                let job = jobs.get(&id);

                if let Some(tx) = job {
                   tx.send(JOB_CANCELLED_MESSAGE.to_owned()).await?;
                }
                drop(jobs);

                // Cancel collected jobs
                let mut job_map = JOB_BATCHING_HASHMAP.lock().await;
                job_map.remove(&id);
                drop(job_map);
            }

            if canceled_at.is_none() && notification.generate_missing {
                // this will be on startup to generate embeddings for rows that might be inserted
                // while daemon is offline
                job_insert_queue_tx.send(JobInsertNotification { id, init: init_finished_at.is_none(), generate_missing: true, filter: Some(get_missing_rows_filter(&src_column, &out_column)), limit: None, row_id: None })?;
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

async fn create_data_path(logger: Arc<Logger>) -> &'static str {
    let tmp_path = "/tmp/lantern-daemon";
    let data_path = if cfg!(target_os = "macos") {
        "/usr/local/var/lantern-daemon"
    } else {
        "/var/lib/lantern-daemon"
    };

    let data_path_obj = Path::new(data_path);
    if data_path_obj.exists() {
        return data_path;
    }

    if fs::create_dir(data_path).await.is_ok() {
        return data_path;
    }

    logger.warn(&format!(
        "No write permission in directory {data_path}. Writing data to temp directory"
    ));
    let tmp_path_obj = Path::new(tmp_path);

    if tmp_path_obj.exists() {
        return tmp_path;
    }

    fs::create_dir(tmp_path).await.unwrap();
    tmp_path
}

#[tokio::main]
pub async fn start(args: cli::DaemonArgs, logger: Arc<Logger>) -> AnyhowVoidResult {
    logger.info("Starting Embedding Jobs");

    let (main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    tokio::spawn(async move { connection.await.unwrap() });

    let main_db_client = Arc::new(main_db_client);
    let notification_channel = "lantern_cloud_embedding_jobs";
    let data_path = create_data_path(logger.clone()).await;

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        UnboundedSender<JobInsertNotification>,
        UnboundedReceiver<JobInsertNotification>,
    ) = mpsc::unbounded_channel();
    let (update_notification_queue_tx, update_notification_queue_rx): (
        UnboundedSender<JobUpdateNotification>,
        UnboundedReceiver<JobUpdateNotification>,
    ) = mpsc::unbounded_channel();
    let (job_queue_tx, job_queue_rx): (Sender<EmbeddingJob>, Receiver<EmbeddingJob>) =
        mpsc::channel(1);
    let table = args.embedding_table.unwrap();

    startup_hook(
        main_db_client.clone(),
        &table,
        &args.schema,
        Some(&args.internal_schema),
        Some(EMB_LOCK_TABLE_NAME),
        &notification_channel,
        logger.clone(),
    )
    .await?;

    let handles = vec![
        Box::pin(db_notification_listener(
            args.uri.clone(),
            &notification_channel,
            insert_notification_queue_tx.clone(),
            Some(update_notification_queue_tx.clone()),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(job_insert_processor(
            args.uri.clone(),
            insert_notification_queue_rx,
            insert_notification_queue_tx.clone(),
            job_queue_tx.clone(),
            args.schema.clone(),
            args.internal_schema.clone(),
            table.clone(),
            data_path,
            logger.clone(),
        )) as VoidFuture,
        Box::pin(job_update_processor(
            args.uri.clone(),
            update_notification_queue_rx,
            insert_notification_queue_tx.clone(),
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(embedding_worker(
            job_queue_rx,
            job_queue_tx.clone(),
            args.uri.clone(),
            args.schema.clone(),
            args.internal_schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(collect_pending_jobs(
            main_db_client.clone(),
            update_notification_queue_tx.clone(),
            get_full_table_name(&args.schema, &table),
        )) as VoidFuture,
    ];

    drop(main_db_client);

    if let Err(e) = future::try_join_all(handles).await {
        logger.error(&e.to_string());
        logger.error("Fatal error exiting process");
        process::exit(1);
    }

    Ok(())
}
