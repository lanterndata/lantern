/*
    External Index Jobs Table should have the following structure:
    CREATE TABLE "public"."index_jobs" (
        "id" SERIAL PRIMARY KEY,
        "database_id" text NOT NULL,
        "db_connection" text NOT NULL,
        "schema" text NOT NULL,
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
    );
*/

use crate::cli;
use crate::helpers::{db_notification_listener, startup_hook, collect_pending_index_jobs, index_job_update_processor};
use crate::types::{AnyhowVoidResult, ExternalIndexJob, JobInsertNotification, VoidFuture, JobUpdateNotification, JobTaskCancelTx, JobCancellationHandlersMap};
use futures::future;
use lantern_external_index::cli::CreateIndexArgs;
use lantern_logger::{Logger, LogLevel};
use lantern_utils::get_full_table_name;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};
use tokio_postgres::{Client, NoTls};

lazy_static! {
    static ref JOBS: JobCancellationHandlersMap = RwLock::new(HashMap::new());
}

async fn set_job_handle(job_id: i32, handle: JobTaskCancelTx) -> AnyhowVoidResult {
    let mut jobs = JOBS.write().await;
    jobs.insert(job_id, handle);
    Ok(())
}

async fn remove_job_handle(job_id: i32) -> AnyhowVoidResult {
    let mut jobs = JOBS.write().await;
    jobs.remove(&job_id);
    Ok(())
}

async fn external_index_worker(
    mut job_queue_rx: Receiver<ExternalIndexJob>,
    client: Arc<Client>,
    schema: String,
    table: String,
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
                Some(Box::new(progress_callback) as lantern_external_index::ProgressCbFn);

            let task_logger =
                Logger::new(&format!("External Index Job {}", job.id), LogLevel::Info);
            let job_clone = job.clone();
            
            let (cancel_tx, mut cancel_rx) = mpsc::channel(1);
            let cancel_tx_clone = cancel_tx.clone();

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
                let task = tokio::task::spawn_blocking(move || {
                    let val: u32  = rand::random();
                    let index_path = format!("/tmp/daemon-index-{val}.usearch");
                    let result = lantern_external_index::create_usearch_index(&CreateIndexArgs {
                        schema: job_clone.schema.clone(),
                        uri: job_clone.db_uri.clone(),
                        table: job_clone.table.clone(),
                        column: job_clone.column.clone(),
                        metric_kind: job_clone.metric_kind.clone(),
                        index_name: job_clone.index_name.clone(),
                        m: job_clone.m,
                        ef: job_clone.ef,
                        efc: job_clone.efc,
                        import: true,
                        dims: 0,
                        out: index_path,
                        remote_database: true,
                    }, progress_callback, Some(is_canceled_clone), Some(task_logger));
                    futures::executor::block_on(cancel_tx_clone.send(false))?;
                    result
                });

                while let Some(should_cancel) = cancel_rx.recv().await {
                    if should_cancel {
                        *is_canceled.write().unwrap() = true;
                    }
                    break;
                }

                task.await?
            });

            set_job_handle(job.id, cancel_tx).await?;
      
            match task_handle.await? {
                Ok(_) => {
                    remove_job_handle(job.id).await?;
                    // mark success
                    client_ref.execute(&format!("UPDATE {jobs_table_name} SET finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job.id]).await?;
                },
                Err(e) => {
                    logger.error(&format!("Error while executing job {job_id}: {e}", job_id=job.id));
                    remove_job_handle(job.id).await?;
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
    mut notifications_rx: Receiver<JobInsertNotification>,
    job_tx: Sender<ExternalIndexJob>,
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
        let job_query_sql = Arc::new(format!("SELECT id, db_connection as db_uri, \"column\", \"table\", \"schema\", operator, efc, ef, m, \"index\"  FROM {0}", &full_table_name));
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            if notification.init && !notification.generate_missing {
                // Only update init time if this is the first time job is being executed
                let updated_count = client.execute(&format!("UPDATE {0} SET started_at=NOW() WHERE started_at IS NULL AND id=$1", &full_table_name), &[&id]).await?;
                if updated_count == 0 {
                    continue;
                }
            }

            let job_result = client
                .query_one(
                    &format!("{job_query_sql} WHERE id=$1 AND canceled_at IS NULL"),
                    &[&id],
                )
                .await;

            if let Ok(row) = job_result {
                job_tx.send(ExternalIndexJob::new(row)?).await?;
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


#[tokio::main]
pub async fn start(args: cli::DaemonArgs, logger: Arc<Logger>) -> AnyhowVoidResult {
    logger.info("Starting External Index Jobs");

    let (main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    tokio::spawn(async move { connection.await.unwrap() });

    let main_db_client = Arc::new(main_db_client);
    let notification_channel = "lantern_cloud_index_jobs";

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        Sender<JobInsertNotification>,
        Receiver<JobInsertNotification>,
    ) = mpsc::channel(args.queue_size);
    let (update_notification_queue_tx, update_notification_queue_rx): (
        Sender<JobUpdateNotification>,
        Receiver<JobUpdateNotification>,
    ) = mpsc::channel(args.queue_size);

    let (job_queue_tx, job_queue_rx): (Sender<ExternalIndexJob>, Receiver<ExternalIndexJob>) =
        mpsc::channel(args.queue_size);

    let table = args.external_index_table.unwrap();

    startup_hook(
        main_db_client.clone(),
        &table,
        &args.schema,
        None,
        None,
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
            main_db_client.clone(),
            insert_notification_queue_rx,
            job_queue_tx,
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(index_job_update_processor(
            main_db_client.clone(),
            update_notification_queue_rx,
            args.schema.clone(),
            table.clone(),
            &JOBS
        )) as VoidFuture,
        Box::pin(external_index_worker(
            job_queue_rx,
            main_db_client.clone(),
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(collect_pending_index_jobs(
            main_db_client.clone(),
            insert_notification_queue_tx.clone(),
            get_full_table_name(&args.schema, &table),
        )) as VoidFuture,
    ];

    if let Err(e) = future::try_join_all(handles).await {
        logger.error(&e.to_string());
        logger.error("Fatal error exiting process");
        process::exit(1);
    }

    Ok(())
}
