use super::types::{
    EmbeddingJob, JobEvent, JobEventHandlersMap, JobInsertNotification, JobTaskEventTx,
    JobUpdateNotification,
};
use crate::logger::Logger;
use crate::types::{AnyhowVoidResult, JOB_CANCELLED_MESSAGE};
use crate::utils::{get_common_embedding_ignore_filters, get_full_table_name, quote_ident};
use futures::StreamExt;
use postgres::tls::MakeTlsConnect;
use postgres::{IsolationLevel, Socket};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tokio_postgres::{AsyncMessage, Connection};
use tokio_util::sync::CancellationToken;

pub async fn check_table_exists(client: Arc<Client>, table: &str) -> AnyhowVoidResult {
    // verify that table exists
    if let Err(_) = client
        .execute(&format!("SELECT ctid FROM {} LIMIT 1", table), &[])
        .await
    {
        anyhow::bail!("Table {table} does not exist");
    }

    Ok(())
}

pub async fn db_notification_listener(
    db_uri: String,
    notification_channel: &'static str,
    insert_queue_tx: UnboundedSender<JobInsertNotification>,
    update_queue_tx: Option<UnboundedSender<JobUpdateNotification>>,
    cancel_token: CancellationToken,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let (client, mut connection) = tokio_postgres::connect(&db_uri, tokio_postgres::NoTls).await?;

    let client = Arc::new(client);
    let client_ref = client.clone();
    // spawn new task to handle notifications
    let task = tokio::spawn(async move {
        let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        logger.info("Lisening for notifications");

        loop {
            tokio::select! {
               notification = stream.next() => {
                 if notification.is_none() {
                     return;
                 }

                 let message = notification.unwrap();

                 if let Err(e) = &message {
                     logger.error(&format!("Failed to get message from db: {}", e));
                     return;
                 }

                 let message = message.unwrap();

                 if let AsyncMessage::Notification(not) = message {
                     let parts: Vec<&str> = not.payload().split(':').collect();

                     if parts.len() < 2 {
                         logger.error(&format!("Invalid notification received {}", not.payload()));
                         return;
                     }

                     let action: &str = parts[0];
                     let id = i32::from_str_radix(parts[1], 10).unwrap();

                     match action {
                         "insert" => {
                             insert_queue_tx
                                 .send(JobInsertNotification {
                                     id,
                                     generate_missing: false,
                                     row_id: None,
                                     filter: None,
                                     limit: None,
                                 })
                                 .unwrap();
                         }
                         "update" => {
                             if let Some(update_tx) = &update_queue_tx {
                                 update_tx
                                     .send(JobUpdateNotification {
                                         id,
                                         generate_missing: true,
                                     })
                                     .unwrap();
                             }
                         }
                         _ => logger.error(&format!("Invalid notification received {}", not.payload())),
                     }
                 }
               },
               _ = cancel_token.cancelled() => {
                    break;
               }
            }
        }
        drop(client_ref);
    });

    client
        .batch_execute(&format!("LISTEN {notification_channel};"))
        .await?;

    task.await?;

    Ok(())
}

pub async fn startup_hook(
    client: &mut Client,
    table: &str,
    table_def: &str,
    schema: &str,
    lock_table_name: Option<&str>,
    results_table_name: Option<&str>,
    results_table_def: Option<&str>,
    usage_table_name: Option<&str>,
    usage_table_def: Option<&str>,
    channel: &str,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    logger.info("Setting up environment");
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(false)
        .start()
        .await?;
    // lock to not have conflict among other daemon instances
    transaction
        .execute("SELECT pg_advisory_lock(1337);", &[])
        .await?;
    // create schema and table if not exists
    transaction
        .execute(
            &format!(
                "CREATE SCHEMA IF NOT EXISTS {schema}",
                schema = quote_ident(schema)
            ),
            &[],
        )
        .await?;
    let full_table_name = get_full_table_name(schema, table);
    transaction
        .execute(
            &format!("CREATE TABLE IF NOT EXISTS {full_table_name} ({table_def})"),
            &[],
        )
        .await?;

    let insert_function_name = &get_full_table_name(schema, &format!("notify_insert_{table}"));
    let update_function_name = &get_full_table_name(schema, &format!("notify_update_{table}"));
    let insert_trigger_name = quote_ident(&format!("trigger_insert_{table}"));
    let update_trigger_name = quote_ident(&format!("trigger_update_{table}"));
    // Set up trigger on table insert
    transaction
        .batch_execute(&format!(
            "
            CREATE OR REPLACE FUNCTION {insert_function_name}() RETURNS TRIGGER AS $$
              BEGIN
                PERFORM pg_notify('{channel}', 'insert:' || NEW.id::TEXT);
                RETURN NULL;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION {update_function_name}() RETURNS TRIGGER AS $$
              BEGIN
                IF (NEW.canceled_at IS NULL AND OLD.canceled_at IS NOT NULL) 
                OR (NEW.canceled_at IS NOT NULL AND OLD.canceled_at IS NULL)
                THEN
                     PERFORM pg_notify('{channel}', 'update:' || NEW.id::TEXT);
	            END IF;
                RETURN NEW;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE TRIGGER {insert_trigger_name}
            AFTER INSERT 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {insert_function_name}();

            CREATE OR REPLACE TRIGGER {update_trigger_name}
            AFTER UPDATE 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {update_function_name}();
        "
        ))
        .await?;

    if lock_table_name.is_some() {
        let lock_table_name = get_full_table_name(schema, lock_table_name.unwrap());
        transaction
            .batch_execute(&format!(
                "
            -- Create Lock Table
            CREATE UNLOGGED TABLE IF NOT EXISTS {lock_table_name} (
              job_id INTEGER NOT NULL,
              row_id TEXT NOT NULL,
              CONSTRAINT ldb_lock_jobid_rowid UNIQUE (job_id, row_id)
            );
        ",
            ))
            .await?;
    }

    if results_table_name.is_some() && results_table_def.is_some() {
        let results_table_name = get_full_table_name(schema, results_table_name.unwrap());
        let results_table_def = results_table_def.unwrap();

        transaction
            .execute(
                &format!("CREATE TABLE IF NOT EXISTS {results_table_name} ({results_table_def})"),
                &[],
            )
            .await?;
    }

    if usage_table_name.is_some() && usage_table_def.is_some() {
        let usage_table_name = get_full_table_name(schema, usage_table_name.unwrap());
        let usage_table_def = usage_table_def.unwrap();
        transaction
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {usage_table_name} ({usage_table_def});
                 CREATE INDEX IF NOT EXISTS embedding_usage_date_idx ON {usage_table_name}(created_at);"
            ))
            .await?;
    }

    transaction
        .execute("SELECT pg_advisory_unlock(1337);", &[])
        .await?;
    transaction.commit().await?;

    Ok(())
}

pub async fn collect_pending_index_jobs(
    client: Arc<Client>,
    insert_notification_tx: UnboundedSender<JobInsertNotification>,
    table: String,
) -> AnyhowVoidResult {
    // Get all pending jobs and set them in queue
    let rows = client
        .query(
            &format!("SELECT id, started_at FROM {table} WHERE failed_at IS NULL and finished_at IS NULL ORDER BY id"),
            &[],
        )
        .await?;

    for row in rows {
        insert_notification_tx.send(JobInsertNotification {
            id: row.get::<usize, i32>(0).to_owned(),
            row_id: None,
            filter: None,
            limit: None,
            // if we do not provide this
            // and some job will be terminated while running
            // on next start of daemon the job will not be picked as
            // it will already have started_at set
            generate_missing: row.get::<usize, Option<SystemTime>>(1).is_some(),
        })?;
    }

    Ok(())
}

pub async fn index_job_update_processor(
    client: Arc<Client>,
    mut update_queue_rx: UnboundedReceiver<JobUpdateNotification>,
    schema: String,
    table: String,
    job_cancelleation_handlers: Arc<JobEventHandlersMap>,
) -> AnyhowVoidResult {
    tokio::spawn(async move {
        while let Some(notification) = update_queue_rx.recv().await {
            let full_table_name = get_full_table_name(&schema, &table);
            let id = notification.id;
            let row = client
                .query_one(
                    &format!("SELECT canceled_at FROM {0} WHERE id=$1", &full_table_name),
                    &[&id],
                )
                .await?;

            let canceled_at: Option<SystemTime> = row.get("canceled_at");

            if canceled_at.is_some() {
                // Cancel ongoing job
                let jobs = job_cancelleation_handlers.read().await;
                let job = jobs.get(&id);

                if let Some(tx) = job {
                    tx.send(JobEvent::Errored(JOB_CANCELLED_MESSAGE.to_owned()))
                        .await?;
                }
                drop(jobs);
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

pub async fn cancel_all_jobs(map: Arc<JobEventHandlersMap>) -> AnyhowVoidResult {
    let mut jobs_map = map.write().await;
    let jobs: Vec<(i32, JobTaskEventTx)> = jobs_map.drain().collect();

    for (_, tx) in jobs {
        tx.send(JobEvent::Errored(JOB_CANCELLED_MESSAGE.to_owned()))
            .await?;
    }

    Ok(())
}

pub async fn set_job_handle(
    map: &JobEventHandlersMap,
    job_id: i32,
    handle: JobTaskEventTx,
) -> AnyhowVoidResult {
    let mut jobs = map.write().await;
    jobs.insert(job_id, handle);
    Ok(())
}

pub async fn remove_job_handle(map: &JobEventHandlersMap, job_id: i32) -> AnyhowVoidResult {
    let mut jobs = map.write().await;
    jobs.remove(&job_id);
    Ok(())
}

pub fn get_missing_rows_filter(src_column: &str, out_column: &str) -> String {
    format!(
        "({common_filter}) AND {out_column} IS NULL",
        common_filter = get_common_embedding_ignore_filters(&quote_ident(&src_column)),
        out_column = quote_ident(&out_column)
    )
}

pub async fn schedule_job_retry(
    logger: Arc<Logger>,
    job: EmbeddingJob,
    tx: Sender<EmbeddingJob>,
    retry_after: Duration,
) {
    tokio::spawn(async move {
        let job_id = job.id;
        let batch_len = if let Some(row_ids) = &job.row_ids {
            row_ids.len()
        } else {
            0
        };
        logger.info(&format!(
            "Scheduling retry after {}s for job {job_id} with batch len ({batch_len})",
            retry_after.as_secs(),
        ));
        tokio::time::sleep(retry_after).await;
        match tx.send(job).await {
            Ok(_) => {}
            Err(e) => logger.error(&format!(
                "Sending retry for failed job: {job_id} with batch len ({batch_len}) failed with error: {e}",
            )),
        };
    });
}

pub async fn cancellation_handler<F, Fut>(
    cancel_token: CancellationToken,
    cleanup_fn: Option<F>,
) -> AnyhowVoidResult
where
    F: FnOnce() -> Fut,
    Fut: futures::Future<Output = AnyhowVoidResult>,
{
    cancel_token.cancelled().await;

    if let Some(cleanup_fn) = cleanup_fn {
        cleanup_fn().await?;
    }

    Ok(())
}

pub async fn notify_job(jobs_map: Arc<JobEventHandlersMap>, job_id: i32, msg: JobEvent) {
    tokio::spawn(async move {
        let jobs = jobs_map.read().await;
        if let Some(tx) = jobs.get(&job_id) {
            tx.send(msg).await?;
        }
        Ok::<(), anyhow::Error>(())
    });
}

pub async fn anyhow_wrap_connection<T>(
    connection: Connection<Socket, T::Stream>,
) -> AnyhowVoidResult
where
    T: MakeTlsConnect<Socket>,
{
    if let Err(e) = connection.await {
        anyhow::bail!(e);
    }
    Ok(())
}
