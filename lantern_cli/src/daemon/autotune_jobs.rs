use super::helpers::{
    cancel_all_jobs, cancellation_handler, collect_pending_index_jobs, db_notification_listener,
    index_job_update_processor, remove_job_handle, set_job_handle, startup_hook,
};
use super::types::{
    AutotuneJob, AutotuneProcessorArgs, JobEvent, JobEventHandlersMap, JobInsertNotification,
    JobRunArgs, JobUpdateNotification,
};
use crate::daemon::helpers::anyhow_wrap_connection;
use crate::external_index::cli::UMetricKind;
use crate::index_autotune::cli::IndexAutotuneArgs;
use crate::logger::Logger;
use crate::types::*;
use crate::utils::get_full_table_name;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
};
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

pub const JOB_TABLE_DEFINITION: &'static str = r#"
"id" SERIAL PRIMARY KEY,
"schema" text NOT NULL DEFAULT 'public',
"table" text NOT NULL,
"column" text NOT NULL,
"operator" text NOT NULL,
"target_recall" DOUBLE PRECISION NOT NULL,
"embedding_model" text NULL,
"k" int NOT NULL,
"n" int NOT NULL,
"create_index" bool NOT NULL,
"created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
"updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
"canceled_at" timestamp,
"started_at" timestamp,
"progress" INT2 DEFAULT 0,
"finished_at" timestamp,
"failed_at" timestamp,
"failure_reason" text
"#;

const RESULT_TABLE_DEFINITION: &'static str = r#"
id SERIAL PRIMARY KEY, 
experiment_id INT NOT NULL,
ef INT NOT NULL, 
efc INT  NOT NULL, 
m INT  NOT NULL, 
recall DOUBLE PRECISION NOT NULL,
latency DOUBLE PRECISION NOT NULL,
build_time DOUBLE PRECISION NULL
"#;

pub async fn autotune_job_processor(
    mut rx: Receiver<AutotuneProcessorArgs>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                if msg.is_none() {
                    break
                }

                let (autotune_args, response_tx, event_tx, progress_callback, is_canceled, task_logger) = msg.unwrap();
                    let result = tokio::task::spawn_blocking(move || crate::index_autotune::autotune_index(&autotune_args,
                     progress_callback, Some(is_canceled), Some(task_logger))).await?;
                    event_tx.send(JobEvent::Done).await?;
                    response_tx.send(result).await?;
            },
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }

    Ok(())
}

async fn autotune_worker(
    mut job_queue_rx: UnboundedReceiver<AutotuneJob>,
    autotune_processor_tx: Sender<AutotuneProcessorArgs>,
    client: Arc<Client>,
    export_db_uri: String,
    schema: String,
    table: String,
    autotune_results_table: String,
    jobs_map: Arc<JobEventHandlersMap>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);
    let autotune_results_table = Arc::new(autotune_results_table);
    let jobs_table_name = Arc::new(get_full_table_name(&schema, &table));

    tokio::spawn(async move {
        logger.info("Autotune worker started");
        while let Some(job) = job_queue_rx.recv().await {
            logger.info(&format!("Starting execution of autotune job {}", job.id));
            let client_ref = client.clone();
            let client_ref2 = client.clone();
            let logger_ref = logger.clone();
            let job = Arc::new(job);
            let job_ref = job.clone();
            let jobs_table_name_r1 = jobs_table_name.clone();
            let export_db_uri = export_db_uri.clone();

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

            let progress_callback = if job.is_init {
                Some(Box::new(progress_callback) as ProgressCbFn)
            } else {
                None
            };

            let task_logger =
                Logger::new(&format!("{}|{}", logger.label, job.id), logger.level.clone());
            let job_clone = job.clone();

            let (event_tx, mut event_rx) = mpsc::channel(1);
            // We will spawn 2 tasks
            // The first one will run autotune job and as soon as it finish
            // It will send the result via job_tx channel
            // The second task will listen to cancel_rx channel, so if someone send a message
            // via cancel_tx channel it will change is_canceled to true
            // And autotune job will be cancelled on next cycle
            // We will keep the cancel_tx in static hashmap, so we can cancel the job if
            // canceled_at will be changed to true

            let schema = schema.clone();
            let table = table.clone();
            let results_table = autotune_results_table.clone();
            let is_canceled = Arc::new(std::sync::RwLock::new(false));
            let is_canceled_clone = is_canceled.clone();
            let (tx, mut rx) = mpsc::channel(1);
            autotune_processor_tx.send((
                IndexAutotuneArgs {
                    job_id: Some(job_clone.id),
                    model_name: job_clone.model_name.clone(),
                    schema: job_clone.schema.clone(),
                    uri: job_clone.db_uri.clone(),
                    export_db_uri: Some(export_db_uri),
                    export_schema_name: schema.to_string(),
                    job_schema_name: schema.to_string(),
                    export_table_name: Some(results_table.to_string()),
                    job_table_name: Some(table.to_string()),
                    table: job_clone.table.clone(),
                    column: job_clone.column.clone(),
                    test_data_size: job_clone.sample_size,
                    create_index: job_clone.create_index,
                    k: job_clone.k,
                    recall: job_clone.recall,
                    metric_kind: UMetricKind::from_ops(&job_clone.metric_kind)?
                },
                tx,
                event_tx.clone(),
                progress_callback,
                is_canceled_clone,
                task_logger
            )).await?;

            set_job_handle(&jobs_map, job.id, event_tx).await?;

            while let Some(event) = event_rx.recv().await {
                if let JobEvent::Errored(msg) = event {
                    if msg == JOB_CANCELLED_MESSAGE.to_owned() {
                        *is_canceled.write().unwrap() = true;
                    }
                }
                break;
            }

            let result = rx.recv().await;

            if result.is_none() {
                logger.error(&format!("No result received for job {}", job.id));
            }

            match result.unwrap() {
                Ok(_) => {
                    remove_job_handle(&jobs_map, job.id).await?;
                    // mark success
                    client_ref.execute(&format!("UPDATE {jobs_table_name} SET finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job.id]).await?;
                },
                Err(e) => {
                    logger.error(&format!("Error while executing job {job_id}: {e}", job_id=job.id));
                    remove_job_handle(&jobs_map, job.id).await?;
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
    job_tx: UnboundedSender<AutotuneJob>,
    db_uri: String,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    // This function will handle newcoming jobs
    // It will update started_at create autotune job from the row
    // And pass to autotune_worker
    // On startup this function will also be called for unfinished jobs

    tokio::spawn(async move {
        let full_table_name = Arc::new(get_full_table_name(&schema, &table));
        let job_query_sql = Arc::new(format!("SELECT id, \"column\",  \"table\", \"schema\", embedding_model as model, target_recall, k, n as sample_size, create_index, operator as metric_kind, finished_at  FROM {0}", &full_table_name));
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
                job_tx.send(AutotuneJob::new(row, &db_uri))?;
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

pub async fn start(
    args: JobRunArgs,
    autotune_processor_tx: Sender<AutotuneProcessorArgs>,
    logger: Arc<Logger>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    logger.info("Starting Autotune Jobs");

    let autotune_results_table = String::from("autotune_results");

    let (mut main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    let connection_task = tokio::spawn(async move { connection.await });

    let notification_channel = "lantern_cloud_autotune_jobs_v2";

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        UnboundedSender<JobInsertNotification>,
        UnboundedReceiver<JobInsertNotification>,
    ) = mpsc::unbounded_channel();
    let (update_notification_queue_tx, update_notification_queue_rx): (
        UnboundedSender<JobUpdateNotification>,
        UnboundedReceiver<JobUpdateNotification>,
    ) = mpsc::unbounded_channel();

    let (job_queue_tx, job_queue_rx): (
        UnboundedSender<AutotuneJob>,
        UnboundedReceiver<AutotuneJob>,
    ) = mpsc::unbounded_channel();

    let table = args.table_name;

    startup_hook(
        &mut main_db_client,
        &table,
        JOB_TABLE_DEFINITION,
        &args.schema,
        None,
        Some(&autotune_results_table),
        Some(RESULT_TABLE_DEFINITION),
        None,
        None,
        None,
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
        autotune_worker(
            job_queue_rx,
            autotune_processor_tx,
            main_db_client.clone(),
            args.uri.clone(),
            args.schema.clone(),
            table.clone(),
            autotune_results_table.clone(),
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
                cancel_all_jobs(jobs_map_clone).await?;

                Ok::<(), anyhow::Error>(())
            })
        )
    )?;

    Ok(())
}
