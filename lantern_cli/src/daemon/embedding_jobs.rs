/*
    Embedding Jobs Table should have the following structure:
    CREATE TABLE "public"."embedding_generation_jobs" (
        "id" SERIAL PRIMARY KEY,
        "database_id" text NOT NULL,
        "db_connection" text NOT NULL,
        "schema" text NOT NULL,
        "table" text NOT NULL,
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
        "init_progress" int2 DEFAULT 0
    );
*/

use super::cli;
use super::client_embedding_jobs::toggle_client_job;
use super::helpers::{db_notification_listener, startup_hook, set_job_handle, remove_job_handle, get_missing_rows_filter, schedule_job_retry};
use crate::types::*;
use super::types::{
     EmbeddingJob, JobInsertNotification, JobUpdateNotification, VoidFuture,  JobCancellationHandlersMap, 
};
use futures::future;
use itertools::Itertools;
use crate::embeddings::cli::EmbeddingArgs;
use crate::logger::Logger;
use crate::utils::get_full_table_name;
use tokio_postgres::types::ToSql;
use std::collections::HashMap;
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::{
    Mutex,
    mpsc,
    mpsc::{Receiver, Sender},
    RwLock
};
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
            let comma = if idx < row_ids.len() - 1 {  "," } else { "" };
            row_ids_query = format!("{row_ids_query}${}{comma}", idx+1);
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
                "Error while unlocking rows: {:?} for job: {job_id} : {e}",row_ids
            ));
    }
}

async fn embedding_worker(
    mut job_queue_rx: Receiver<EmbeddingJob>,
    job_queue_tx: Sender<EmbeddingJob>,
    notifications_tx: Sender<JobInsertNotification>,
    client: Arc<Client>,
    schema: String,
    internal_schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);
    let jobs_table_name = Arc::new(get_full_table_name(&schema, &table));

    tokio::spawn(async move {
        logger.info("Embedding worker started");
        while let Some(job) = job_queue_rx.recv().await {
            logger.info(&format!("Starting execution of embedding job {}", job.id));
            let client_ref = client.clone();
            let client_ref2 = client.clone();
            let logger_ref = logger.clone();
            let orig_job_clone = job.clone();
            let job = Arc::new(job);
            let job_ref = job.clone();
            let jobs_table_name_r1 = jobs_table_name.clone();
            let schema_ref = schema.clone();

            let progress_callback = move |progress: u8| {
                // Passed progress in a format string to avoid type casts between 
                // different int types
                let res = futures::executor::block_on(client_ref2.execute(&format!("UPDATE {jobs_table_name_r1} SET init_progress={progress} WHERE id=$1"), &[&job_ref.id]));

                if let Err(e) = res {
                    logger_ref.error(&format!("Error while updating progress for job {job_id}: {e}", job_id=job_ref.id));
                }
            };

            let progress_callback = if job.is_init {
                Some(Box::new(progress_callback) as ProgressCbFn)
            } else {
                None
            };

            let task_logger = Logger::new(&format!("Embedding Job {}|{:?}", job.id, job.runtime), logger.level.clone());
            let job_clone = job.clone();
            
            let (cancel_tx, mut cancel_rx) = mpsc::channel(1);
            let cancel_tx_clone = cancel_tx.clone();

            // We will spawn 2 tasks
            // The first one will run embedding generation job and as soon as it finish
            // It will send the result via job_tx channel
            // The second task will listen to cancel_rx channel, so if someone send a message
            // via cancel_tx channel it will change is_canceled to true
            // And embedding job will be cancelled on next cycle
            // We will keep the cancel_tx in static hashmap, so we can cancel the job if
            // canceled_at will be changed to true

            // Enable triggers for job
            if job.is_init {
              toggle_client_job(job.id.clone(), job.db_uri.clone(), job.column.clone(), job.out_column.clone(), job.table.clone(), job.schema.clone(), logger.level.clone(), Some(notifications_tx.clone()), true ).await?;
            }

            let task_handle = tokio::spawn(async move {
                let is_canceled = Arc::new(std::sync::RwLock::new(false));
                let is_canceled_clone = is_canceled.clone();
                let embedding_task = tokio::spawn(async move {
                    let result = crate::embeddings::create_embeddings_from_db(EmbeddingArgs {
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
                            limit: None
                        }, progress_callback.is_some(), progress_callback, Some(is_canceled_clone), Some(task_logger));
                    cancel_tx_clone.send(false).await?;
                    result
                });

                while let Some(should_cancel) = cancel_rx.recv().await {
                    if should_cancel {
                        *is_canceled.write().unwrap() = true;
                    }
                    break;
                }

                embedding_task.await?
            });

            set_job_handle(&JOBS, job.id, cancel_tx).await?;
     
            match task_handle.await? {
                Ok((processed_rows, processed_tokens)) => {
                    remove_job_handle(&JOBS, job.id).await?;
                    if job.is_init {
                        // mark success
                        client_ref.execute(&format!("UPDATE {jobs_table_name} SET init_finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job.id]).await?;
                    }

                    if processed_tokens > 0 {
                        let fn_name = get_full_table_name(&schema_ref, "increment_embedding_usage_and_tokens");
                        let res = client_ref.execute(&format!("SELECT {fn_name}({job_id},{usage},{tokens}::bigint)", job_id=job.id, usage=processed_rows, tokens=processed_tokens), &[]).await;

                        if let Err(e) = res {
                            logger.error(&format!("Error while updating usage for {job_id}: {e}", job_id=job.id));
                        }
                    }
                },
                Err(e) => {
                    logger.error(&format!("Error while executing job {job_id}: {e}", job_id=job.id));
                    remove_job_handle(&JOBS, job.id).await?;
                    if job.is_init {
                        // update failure reason
                        client_ref.execute(&format!("UPDATE {jobs_table_name} SET init_failed_at=NOW(), updated_at=NOW(), init_failure_reason=$1 WHERE id=$2"), &[&e.to_string(), &job.id]).await?;
                        toggle_client_job(job.id.clone(), job.db_uri.clone(), job.column.clone(), job.out_column.clone(), job.table.clone(), job.schema.clone(), logger.level.clone(), Some(notifications_tx.clone()), false).await?;
                    } else {
                        schedule_job_retry(logger.clone(), orig_job_clone, job_queue_tx.clone(), Duration::from_secs(300)).await?;
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
               unlock_rows(client_ref, &lock_table_name, logger.clone(), job.id, row_ids).await;
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

async fn collect_pending_jobs(
    client: Arc<Client>,
    update_notification_tx: Sender<JobUpdateNotification>,
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
        update_notification_tx
            .send(JobUpdateNotification {
                id: row.get::<usize, i32>(0).to_owned(),
                generate_missing: true,
            })
            .await?;
    }

    Ok(())
}

async fn job_insert_processor(
    db_uri: String,
    mut notifications_rx: Receiver<JobInsertNotification>,
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

    let (client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
    tokio::spawn(async move { connection.await.unwrap() });

    let client = Arc::new(client);

    let full_table_name = Arc::new(get_full_table_name(&schema, &table));
    let job_query_sql = Arc::new(format!("SELECT id, db_connection as db_uri, src_column as \"column\", dst_column, \"table\", \"schema\", embedding_model as model, runtime, runtime_params::text FROM {0}", &full_table_name));

    let full_table_name_r1 = full_table_name.clone();
    let job_query_sql_r1 = job_query_sql.clone();
    let client_r1 = client.clone();
    let job_tx_r1 = job_tx.clone();
    let logger_r1 = logger.clone();
    let lock_table_name = Arc::new(get_full_table_name(&lock_table_schema, EMB_LOCK_TABLE_NAME));
    let job_batching_hashmap_r1 = JOB_BATCHING_HASHMAP.clone();

    let insert_processor_task = tokio::spawn(async move {
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            if let Some(row_id) = notification.row_id {
                // Do this in a non-blocking way to not block collecting of updates while locking
                let client_r1 = client_r1.clone();
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

            // TODO
            // when we are checking !notification.generate_missing we are excluding job "locking"
            // so in case if we have more than one daemons running and job will change
            // `canceled_at` to NULL (e.g. resume job) all daemons will try to generate embeddings
            // for the missing rows. This will be okay if we every time reset init_start/finish
            // times when changing canceled_at to NULL and remove this generate_missing check.
            // This case is also about the startup pending job collector. As there might be a case
            // when job was failed on init and if 2 daemons will start-up at the same time both
            // will pick that job and try to generate embeddings (though this is very rare case)
            if notification.init && !notification.generate_missing {
                // Only update init time if this is the first time job is being executed
                let updated_count = client_r1.execute(&format!("UPDATE {0} SET init_started_at=NOW() WHERE init_started_at IS NULL AND id=$1", &full_table_name_r1), &[&id]).await?;
                if updated_count == 0 {
                    continue;
                }
            }

            let job_result = client_r1
                .query_one(
                    &format!("{job_query_sql_r1} WHERE id=$1 AND canceled_at IS NULL"),
                    &[&id],
                )
                .await;

            if let Ok(row) = job_result {
                let  job = EmbeddingJob::new(row, data_path);

                if let Err(e) = &job {
                    logger_r1.error(&format!(
                        "Error while creating job {id}: {e}",
                    ));
                    continue;
                }
                let mut job = job.unwrap();
                job.set_is_init(notification.init);
                if let Some(filter) = notification.filter {
                    job.set_filter(&filter);
                }
                job_tx_r1.send(job).await?;
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
        loop {
            let mut job_map = job_batching_hashmap.lock().await;
            let jobs: Vec<(i32, Vec<String>)> = job_map.drain().collect();
            // release lock
            drop(job_map);

            for (job_id, row_ids) in jobs {
                let job_result = client
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
                let row_ctids_str = row_ids.iter().map(|r| format!("'{r}'::tid")).join(",");
                let rows_len = row_ids.len();
                job.set_row_ids(row_ids);
                job.set_filter(&format!("ctid IN ({row_ctids_str})"));
                logger.debug(&format!("Sending batch job {job_id} (len: {rows_len}) to embedding_worker"));
                let _ = job_tx.send(job).await;
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
    client: Arc<Client>,
    mut update_queue_rx: Receiver<JobUpdateNotification>,
    job_insert_queue_tx: Sender<JobInsertNotification>,
    schema: String,
    table: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    tokio::spawn(async move {
        while let Some(notification) = update_queue_rx.recv().await {
            let full_table_name =  get_full_table_name(&schema, &table);
            let id = notification.id;
            let row = client.query_one(&format!("SELECT db_connection as db_uri, dst_column, src_column as \"column\", \"table\", \"schema\", canceled_at, init_finished_at FROM {0} WHERE id=$1", &full_table_name), &[&id]).await?;
            let src_column = row.get::<&str, String>("column").to_owned();
            let out_column = row.get::<&str, String>("dst_column").to_owned();

            let canceled_at: Option<SystemTime> = row.get("canceled_at");
            let init_finished_at: Option<SystemTime> = row.get("init_finished_at");

            if !notification.generate_missing {
              logger.debug(&format!("Update job {id}: is_canceled: {}", canceled_at.is_some()));
            }

            if init_finished_at.is_some() {
              toggle_client_job(id, row.get::<&str, String>("db_uri").to_owned(), row.get::<&str, String>("column").to_owned(), row.get::<&str, String>("dst_column").to_owned(), row.get::<&str, String>("table").to_owned(), row.get::<&str, String>("schema").to_owned(), logger.level.clone(), Some(job_insert_queue_tx.clone()), canceled_at.is_none()).await?;
            } else if canceled_at.is_some() {
                // Cancel ongoing job
                let jobs = JOBS.read().await;
                let job = jobs.get(&id);

                if let Some(tx) = job {
                   tx.send(true).await?;
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
                job_insert_queue_tx.send(JobInsertNotification { id, init: init_finished_at.is_none(), generate_missing: true, filter: Some(get_missing_rows_filter(&src_column, &out_column)), limit: None, row_id: None }).await?;
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
        Sender<JobInsertNotification>,
        Receiver<JobInsertNotification>,
    ) = mpsc::channel(args.queue_size);
    let (update_notification_queue_tx, update_notification_queue_rx): (
        Sender<JobUpdateNotification>,
        Receiver<JobUpdateNotification>,
    ) = mpsc::channel(args.queue_size);
    let (job_queue_tx, job_queue_rx): (Sender<EmbeddingJob>, Receiver<EmbeddingJob>) =
        mpsc::channel(args.queue_size);
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
            job_queue_tx.clone(),
            args.schema.clone(),
            args.internal_schema.clone(),
            table.clone(),
            data_path,
            logger.clone(),
        )) as VoidFuture,
        Box::pin(job_update_processor(
            main_db_client.clone(),
            update_notification_queue_rx,
            insert_notification_queue_tx.clone(),
            args.schema.clone(),
            table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(embedding_worker(
            job_queue_rx,
            job_queue_tx.clone(),
            insert_notification_queue_tx.clone(),
            main_db_client.clone(),
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

    if let Err(e) = future::try_join_all(handles).await {
        logger.error(&e.to_string());
        logger.error("Fatal error exiting process");
        process::exit(1);
    }

    Ok(())
}
