use super::helpers::{cancel_all_jobs, cancellation_handler, collect_pending_index_jobs, db_notification_listener, index_job_update_processor, startup_hook};
use crate::daemon::helpers::anyhow_wrap_connection;
use crate::types::*;
use super::types::{ExternalIndexJob, JobEvent, JobEventHandlersMap, JobInsertNotification, JobRunArgs, JobTaskEventTx, JobUpdateNotification};
use crate::external_index::cli::{CreateIndexArgs, UMetricKind};
use crate::logger::{Logger, LogLevel};
use crate::utils::get_full_table_name;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_postgres::{Client, NoTls};

pub const JOB_TABLE_DEFINITION: &'static str = r#"
"id" SERIAL PRIMARY KEY,
"schema" text NOT NULL DEFAULT 'public',
"table" text NOT NULL,
"column" text NOT NULL,
"index" text,
"operator" text NOT NULL,
"efc" INT NOT NULL,
"ef" INT NOT NULL,
"m" INT NOT NULL,
"created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
"updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
"canceled_at" timestamp,
"started_at" timestamp,
"finished_at" timestamp,
"failed_at" timestamp,
"failure_reason" text,
"progress" INT2 DEFAULT 0
"#;

async fn set_job_handle(jobs_map: Arc<JobEventHandlersMap>, job_id: i32, handle: JobTaskEventTx) -> AnyhowVoidResult {
    let mut jobs = jobs_map.write().await;
    jobs.insert(job_id, handle);
    Ok(())
}

async fn remove_job_handle(jobs_map: Arc<JobEventHandlersMap>, job_id: i32) -> AnyhowVoidResult {
    let mut jobs = jobs_map.write().await;
    jobs.remove(&job_id);
    Ok(())
}

async fn external_index_worker(
    mut job_queue_rx: UnboundedReceiver<ExternalIndexJob>,
    client: Arc<Client>,
    schema: String,
    table: String,
    jobs_map: Arc<JobEventHandlersMap>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);
    let jobs_table_name = Arc::new(get_full_table_name(&schema, &table));

    tokio::spawn(async move {
        logger.info("External index worker started");
        while let Some(job) = job_queue_rx.recv().await {
            logger.info(&format!("Starting execution of index creation job {}", job.id));
            let client_ref = client.clone();
            let client_ref2 = client.clone();
            let logger_ref = logger.clone();
            let job = Arc::new(job);
            let job_ref = job.clone();
            let jobs_table_name_r1 = jobs_table_name.clone();

            let progress_callback = move |progress: u8| {
                // Passed progress in a format string to avoid type casts between
                // different int types
                let res = futures::executor::block_on(client_ref2.execute(
                    &format!(
                        "UPDATE {jobs_table_name_r1} SET progress={progress} WHERE id=$1"
                    ),
                    &[&job_ref.id],
                ));

                if let Err(e) = res {
                    logger_ref.error(&format!(
                        "Error while updating progress for job {job_id}: {e}",
                        job_id = job_ref.id
                    ));
                }
            };

            let progress_callback = 
                Some(Box::new(progress_callback) as ProgressCbFn);

            let task_logger =
                Logger::new(&format!("{}|{}", logger.label, job.id), LogLevel::Info);
            let job_clone = job.clone();
            
            let (event_tx, mut event_rx) = mpsc::channel(1);
            let event_tx_clone = event_tx.clone();

            // We will spawn 2 tasks
            // The first one will run index creation job and as soon as it finish
            // It will send the result via job_tx channel
            // The second task will listen to cancel_rx channel, so if someone send a message
            // via cancel_tx channel it will change is_canceled to true
            // And index job will be cancelled on next cycle
            // We will keep the cancel_tx in static hashmap, so we can cancel the job if
            // canceled_at will be changed to true

            let task_handle = tokio::spawn(async move {
                let is_canceled = Arc::new(std::sync::RwLock::new(false));
                let is_canceled_clone = is_canceled.clone();
                let metric_kind = UMetricKind::from_ops(&job_clone.operator_class)?;
                let task = tokio::task::spawn_blocking(move || {
                    let val: u32  = rand::random();
                    let index_path = format!("/tmp/daemon-index-{val}.usearch");
                    let result = crate::external_index::create_usearch_index(&CreateIndexArgs {
                        schema: job_clone.schema.clone(),
                        uri: job_clone.db_uri.clone(),
                        table: job_clone.table.clone(),
                        column: job_clone.column.clone(),
                        metric_kind,
                        index_name: job_clone.index_name.clone(),
                        m: job_clone.m,
                        ef: job_clone.ef,
                        efc: job_clone.efc,
                        import: true,
                        dims: 0,
                        out: index_path,
                        remote_database: true,
                        pq: false,
                    }, progress_callback, Some(is_canceled_clone), Some(task_logger));
                    futures::executor::block_on(event_tx_clone.send(JobEvent::Done))?;
                    result
                });

                while let Some(event) = event_rx.recv().await {
                    if let JobEvent::Errored(msg) = event {
                        if msg == JOB_CANCELLED_MESSAGE.to_owned() {
                            *is_canceled.write().unwrap() = true;
                        }
                    }
                    break;
                }

                task.await?
            });

            set_job_handle(jobs_map.clone(), job.id, event_tx).await?;
      
            match task_handle.await? {
                Ok(_) => {
                    remove_job_handle(jobs_map.clone(), job.id).await?;
                    // mark success
                    client_ref.execute(&format!("UPDATE {jobs_table_name} SET finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job.id]).await?;
                },
                Err(e) => {
                    logger.error(&format!("Error while executing job {job_id}: {e}", job_id=job.id));
                    remove_job_handle(jobs_map.clone(), job.id).await?;
                    // update failure reason
                    client_ref.execute(&format!("UPDATE {jobs_table_name} SET failed_at=NOW(), updated_at=NOW(), failure_reason=$1 WHERE id=$2"), &[&e.to_string(), &job.id]).await?;
                }
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}


async fn job_insert_processor(
    client: Arc<Client>,
    mut notifications_rx: UnboundedReceiver<JobInsertNotification>,
    job_tx: UnboundedSender<ExternalIndexJob>,
    db_uri: String,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    // This function will handle newcoming jobs
    // It will update started_at create external index job from the row
    // And pass to external_index_worker
    // On startup this function will also be called for unfinished jobs

    tokio::spawn(async move {
        let full_table_name = Arc::new(get_full_table_name(&schema, &table));
        let job_query_sql = Arc::new(format!("SELECT id, \"column\", \"table\", \"schema\", operator, efc, ef, m, \"index\", finished_at FROM {0}", &full_table_name));
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            let job_result = client
                .query_one(
                    &format!("{job_query_sql} WHERE id=$1 AND canceled_at IS NULL"),
                    &[&id],
                )
                .await;

            if let Ok(row) = job_result {
                let is_init = row
                    .get::<&str, Option<SystemTime>>("finished_at")
                    .is_none();

                if is_init {
                    // Only update init time if this is the first time job is being executed
                    let updated_count = client.execute(&format!("UPDATE {0} SET started_at=NOW() WHERE started_at IS NULL AND id=$1", &full_table_name), &[&id]).await?;
                    if updated_count == 0 && !notification.generate_missing {
                        continue;
                    }
                }
                job_tx.send(ExternalIndexJob::new(row, &db_uri.clone()))?;
            } else {
                logger.error(&format!(
                    "Error while getting job {id}: {}",
                    job_result.err().unwrap()
                ));
            }
        }
        Ok(()) as AnyhowVoidResult
    }).await??;

    Ok(())
}

pub async fn start(args: JobRunArgs, logger: Arc<Logger>, cancel_token: CancellationToken) -> AnyhowVoidResult {
    logger.info("Starting External Index Jobs");

    let (mut main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    let connection_task = tokio::spawn(async move { connection.await });

    let notification_channel = "lantern_cloud_index_jobs_v2";

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        UnboundedSender<JobInsertNotification>,
        UnboundedReceiver<JobInsertNotification>,
    ) = mpsc::unbounded_channel();
    let (update_notification_queue_tx, update_notification_queue_rx): (
        UnboundedSender<JobUpdateNotification>,
        UnboundedReceiver<JobUpdateNotification>,
    ) = mpsc::unbounded_channel();

    let (job_queue_tx, job_queue_rx): (UnboundedSender<ExternalIndexJob>, UnboundedReceiver<ExternalIndexJob>) =
        mpsc::unbounded_channel();

    let table = args.table_name;

    startup_hook(
        &mut main_db_client,
        &table,
        JOB_TABLE_DEFINITION,
        &args.schema,
        None,
        None,
        None,
        false,
        &notification_channel,
        logger.clone(),
    )
    .await?;

    connection_task.abort();
    let (main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;
    let main_db_client = Arc::new(main_db_client);

    let jobs_map: Arc<JobEventHandlersMap> = Arc::new(RwLock::new(HashMap::new()));
    let jobs_map_clone = jobs_map.clone();

    tokio::try_join!(
        anyhow_wrap_connection::<NoTls>(connection),
        db_notification_listener(
            args.uri.clone(),
            &notification_channel,
            insert_notification_queue_tx.clone(),
            Some(update_notification_queue_tx.clone()),
            cancel_token.clone(),
            logger.clone(),
        ),
        job_insert_processor(
            main_db_client.clone(),
            insert_notification_queue_rx,
            job_queue_tx,
            args.uri.clone(),
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        ),
        index_job_update_processor(
            main_db_client.clone(),
            update_notification_queue_rx,
            args.schema.clone(),
            table.clone(),
            jobs_map.clone()
        ),
        external_index_worker(
            job_queue_rx,
            main_db_client.clone(),
            args.schema.clone(),
            table.clone(),
            jobs_map.clone(),
            logger.clone(),
        ),
        collect_pending_index_jobs(
            main_db_client.clone(),
            insert_notification_queue_tx.clone(),
            get_full_table_name(&args.schema, &table),
        ),
        cancellation_handler(
            cancel_token.clone(),
            Some(move || async {
                cancel_all_jobs(
                    jobs_map_clone,
                )
                .await?;

                Ok::<(), anyhow::Error>(())
            })
        )
    )?;

    Ok(())
}
