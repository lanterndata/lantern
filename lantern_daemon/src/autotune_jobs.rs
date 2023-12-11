/*
    Autotune Jobs Table should have the following structure:
    CREATE TABLE "public"."index_autotune_jobs" (
        "id" SERIAL PRIMARY KEY,
        "database_id" text NOT NULL,
        "db_connection" text NOT NULL,
        "schema" text NOT NULL,
        "table" text NOT NULL,
        "src_column" text NOT NULL,
        "metric_kind" text NOT NULL,
        "target_recall" int NOT NULL,
        "k" int NOT NULL,
        "create_index" bool NOT NULL,
        "embedding_model" text NULL,
        "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "canceled_at" timestamp,
        "init_started_at" timestamp,
        "init_finished_at" timestamp,
        "init_failed_at" timestamp,
        "init_failure_reason" text,
        "init_progress" INT2 DEFAULT 0
    );
*/

use crate::cli;
use crate::helpers::{db_notification_listener, startup_hook};
use crate::types::{AnyhowVoidResult, AutotuneJob, JobInsertNotification, VoidFuture};
use futures::future;
use lantern_create_index::cli::UMetricKind;
use lantern_index_autotune::cli::IndexAutotuneArgs;
use lantern_logger::Logger;
use lantern_utils::get_full_table_name;
use std::process;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};
use tokio_postgres::{Client, NoTls};

async fn autotune_worker(
    mut job_queue_rx: Receiver<AutotuneJob>,
    client: Arc<Client>,
    export_db_uri: String,
    internal_schema: String,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);
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
            let internal_schema = internal_schema.clone();
            let export_db_uri = export_db_uri.clone();

            let progress_callback = move |progress: u8| {
                // Passed progress in a format string to avoid type casts between
                // different int types
                let res = futures::executor::block_on(client_ref2.execute(
                    &format!(
                        "UPDATE {jobs_table_name_r1} SET init_progress={progress} WHERE id=$1"
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
                Some(Box::new(progress_callback) as lantern_index_autotune::ProgressCbFn)
            } else {
                None
            };

            let task_logger =
                Logger::new(&format!("Autotune Job {}", job.id), logger.level.clone());
            let is_init = job.is_init;
            let job_id = job.id;
            
            let result = tokio::task::spawn_blocking(move || {
                lantern_index_autotune::autotune_index(&IndexAutotuneArgs {
                    pk: "id".to_owned(),
                    job_id: Some(job.id.to_string()),
                    model_name: job.model_name.clone(),
                    schema: job.schema.clone(),
                    uri: job.db_uri.clone(),
                    export_db_uri: Some(export_db_uri),
                    export_schema_name: internal_schema,
                    export_table_name: "lantern_autotune_results".to_owned(),
                    table: job.table.clone(),
                    column: job.column.clone(),
                    test_data_size: 10000,
                    create_index: job.create_index,
                    export: true,
                    k: job.k,
                    recall: job.recall,
                    metric_kind: UMetricKind::from(&job.metric_kind)?
                }, progress_callback, Some(task_logger))
            }).await;

            match result {
                Ok(_) => {
                    if is_init {
                        // mark success
                        client_ref.execute(&format!("UPDATE {jobs_table_name} SET init_finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job_id]).await?;
                    }
                },
                Err(e) => {
                    if is_init {
                        // update failure reason
                        client_ref.execute(&format!("UPDATE {jobs_table_name} SET init_failed_at=NOW(), updated_at=NOW(), init_failure_reason=$1 WHERE id=$2"), &[&e.to_string(), &job_id]).await?;
                    }
                }
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

async fn collect_pending_jobs(
    client: Arc<Client>,
    insert_notification_tx: Sender<JobInsertNotification>,
    table: String,
) -> AnyhowVoidResult {
    // Get all pending jobs and set them in queue
    let rows = client
        .query(
            &format!("SELECT id, init_started_at FROM {table} WHERE init_failed_at IS NULL and init_finished_at IS NULL ORDER BY id"),
            &[],
        )
        .await?;

    for row in rows {
        // TODO This can be optimized
        insert_notification_tx
            .send(JobInsertNotification {
                id: row.get::<usize, i32>(0).to_owned(),
                init: true,
                row_id: None,
                filter: None,
                limit: None,
                // if we do not provide this
                // and some job will be terminated while running
                // on next start of daemon the job will not be picked as
                // it will already have init_started_at set
                generate_missing: row.get::<usize, Option<SystemTime>>(1).is_some(),
            })
            .await?;
    }

    Ok(())
}

async fn job_insert_processor(
    client: Arc<Client>,
    mut notifications_rx: Receiver<JobInsertNotification>,
    job_tx: Sender<AutotuneJob>,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    // This function will handle newcoming jobs
    // It will update init_started_at create autotune job from the row
    // And pass to autotune_worker
    // On startup this function will also be called for unfinished jobs

    tokio::spawn(async move {
        let full_table_name = Arc::new(get_full_table_name(&schema, &table));
        let job_query_sql = Arc::new(format!("SELECT id, db_connection as db_uri, src_column as \"column\",  \"table\", \"schema\", embedding_model as model, target_recall, k, create_index, metric_kind  FROM {0}", &full_table_name));
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            if notification.init && !notification.generate_missing {
                // Only update init time if this is the first time job is being executed
                let updated_count = client.execute(&format!("UPDATE {0} SET init_started_at=NOW() WHERE init_started_at IS NULL AND id=$1", &full_table_name), &[&id]).await?;
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
                let mut job = AutotuneJob::new(row);
                job.set_is_init(notification.init);
                job_tx.send(job).await?;
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
    logger.info("Starting Autotune Jobs");

    let (main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    tokio::spawn(async move { connection.await.unwrap() });

    let main_db_client = Arc::new(main_db_client);
    let notification_channel = "lantern_cloud_autotune_jobs";

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        Sender<JobInsertNotification>,
        Receiver<JobInsertNotification>,
    ) = mpsc::channel(args.queue_size);

    let (job_queue_tx, job_queue_rx): (Sender<AutotuneJob>, Receiver<AutotuneJob>) =
        mpsc::channel(args.queue_size);

    let table = args.autotune_table.unwrap();

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
            None,
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
        Box::pin(autotune_worker(
            job_queue_rx,
            main_db_client.clone(),
            args.uri.clone(),
            args.internal_schema.clone(),
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(collect_pending_jobs(
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
