pub mod cli;
mod client_jobs;
mod helpers;
mod types;

use client_jobs::toggle_client_job;
use futures::{future, StreamExt};
use helpers::check_table_exists;
use itertools::Itertools;
use lantern_embeddings::cli::EmbeddingArgs;
use lantern_logger::Logger;
use lantern_utils::{get_full_table_name, quote_ident};
use std::collections::HashMap;
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use std::{ops::Deref, time::SystemTime};
use tokio::fs;
use tokio::sync::Mutex;
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};
use tokio_postgres::{AsyncMessage, Client, NoTls};
use types::{AnyhowVoidResult, Job, JobInsertNotification, JobUpdateNotification, VoidFuture};

#[macro_use]
extern crate lazy_static;

const EMB_LOCK_TABLE_NAME: &'static str = "_lantern_emb_job_locks";

async fn db_notification_listener(
    db_uri: String,
    notification_channel: &'static str,
    insert_queue_tx: Sender<JobInsertNotification>,
    update_queue_tx: Sender<JobUpdateNotification>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let (client, mut connection) = tokio_postgres::connect(&db_uri, tokio_postgres::NoTls).await?;

    let client = Arc::new(client);
    let client_ref = client.clone();
    // spawn new task to handle notifications
    let task = tokio::spawn(async move {
        let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        logger.info("Lisening for notifications");

        while let Some(message) = stream.next().await {
            if let Err(e) = &message {
                logger.error(&format!("Failed to get message from db: {}", e));
            }

            let message = message.unwrap();

            if let AsyncMessage::Notification(not) = message {
                let parts: Vec<&str> = not.payload().split(':').collect();

                if parts.len() < 2 {
                    logger.error(&format!("Invalid notification received {}", not.payload()));
                    continue;
                }

                let action: &str = parts[0];
                let id = i32::from_str_radix(parts[1], 10).unwrap();

                match action {
                    "insert" => {
                        insert_queue_tx
                            .send(JobInsertNotification {
                                id,
                                init: true,
                                generate_missing: false,
                                row_id: None,
                                filter: None,
                                limit: None,
                            })
                            .await
                            .unwrap();
                    }
                    "update" => {
                        update_queue_tx
                            .send(JobUpdateNotification {
                                id,
                                generate_missing: true,
                            })
                            .await
                            .unwrap();
                    }
                    _ => logger.error(&format!("Invalid notification received {}", not.payload())),
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

async fn lock_row(client: Arc<Client>, logger: Arc<Logger>, job_id: i32, row_id: &str) -> bool {
    let res = client
        .execute(
            &format!("INSERT INTO {EMB_LOCK_TABLE_NAME} (job_id, row_id) VALUES ($1, $2)"),
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

async fn embedding_worker(
    mut job_queue_rx: Receiver<Job>,
    notifications_tx: Sender<JobInsertNotification>,
    client: Arc<Client>,
    schema: String,
    table: String,
    data_path: String,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let schema = Arc::new(schema);
    let table = Arc::new(table);

    tokio::spawn(async move {
        logger.info("Embedding worker started");
        while let Some(job) = job_queue_rx.recv().await {
            logger.info(&format!("Starting execution of job {}", job.id));
            let client_ref = client.clone();
            let schema_ref = schema.clone();
            let table_ref = table.clone();
            let data_path = data_path.clone();

            let task_logger = Logger::new(&format!("Job {}", job.id), logger.level.clone());
            let result = lantern_embeddings::create_embeddings_from_db(EmbeddingArgs {
                pk: String::from("id"),
                model: job.model,
                schema: job.schema.clone(),
                uri: job.db_uri.clone(),
                out_uri: Some(job.db_uri.clone()),
                table: job.table.clone(),
                out_table: Some(job.table.clone()),
                column: job.column.clone(),
                out_column: job.out_column.clone(),
                batch_size: job.batch_size,
                data_path: Some(data_path),
                visual: false,
                stream: false,
                create_column: false,
                out_csv: None,
                filter: job.filter,
                limit: None
            }, Some(task_logger));

            if job.is_init {
                let full_table_name = get_full_table_name(schema_ref.deref(), table_ref.deref());
                if let Err(e) = result {
                    // update failure reason
                    client_ref.execute(&format!("UPDATE {full_table_name} SET init_failed_at=NOW(), updated_at=NOW(), init_failure_reason=$1 WHERE id=$2"), &[&e.to_string(), &job.id]).await?;
                } else {
                    // mark success
                    client_ref.execute(&format!("UPDATE {full_table_name} SET init_finished_at=NOW(), updated_at=NOW() WHERE id=$1"), &[&job.id]).await?;
                    toggle_client_job(job.id.clone(), job.db_uri.clone(), job.column.clone(), job.out_column.clone(), job.table.clone(), job.schema.clone(), logger.level.clone(), Some(notifications_tx.clone()), true ).await?;
                }
            }
        }
        Ok(()) as AnyhowVoidResult
    })
    .await??;
    Ok(())
}

async fn startup_hook(
    client: Arc<Client>,
    table: &str,
    schema: &str,
    channel: &str,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    logger.info("Setting up environment");
    // verify that table exists
    let full_table_name = get_full_table_name(schema, table);
    check_table_exists(client.clone(), &full_table_name).await?;

    // Set up trigger on table insert
    client
        .batch_execute(&format!(
            "
            CREATE OR REPLACE FUNCTION notify_insert_lantern_daemon() RETURNS TRIGGER AS $$
              BEGIN
                PERFORM pg_notify('{channel}', 'insert:' || NEW.id::TEXT);
                RETURN NULL;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION notify_update_lantern_daemon() RETURNS TRIGGER AS $$
              BEGIN
                IF (NEW.canceled_at IS NULL AND OLD.canceled_at IS NOT NULL) 
                OR (NEW.canceled_at IS NOT NULL AND OLD.canceled_at IS NULL)
                THEN
                     PERFORM pg_notify('{channel}', 'update:' || NEW.id::TEXT);
	            END IF;
                RETURN NEW;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE TRIGGER trigger_lantern_jobs_insert
            AFTER INSERT 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE notify_insert_lantern_daemon();

            CREATE OR REPLACE TRIGGER trigger_lantern_jobs_update
            AFTER UPDATE 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE notify_update_lantern_daemon();

            -- Create Lock Table
            CREATE UNLOGGED TABLE IF NOT EXISTS {EMB_LOCK_TABLE_NAME} (
              job_id INTEGER NOT NULL,
              row_id TEXT NOT NULL,
              CONSTRAINT ldb_lock_jobid_rowid UNIQUE (job_id, row_id)
            );
        ",
        ))
        .await?;

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
            &format!("SELECT id FROM {table} WHERE init_failed_at IS NULL ORDER BY id"),
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
    client: Arc<Client>,
    mut notifications_rx: Receiver<JobInsertNotification>,
    job_tx: Sender<Job>,
    schema: String,
    table: String,
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

    let job_batching_hashmap: Arc<Mutex<HashMap<i32, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let full_table_name = Arc::new(get_full_table_name(&schema, &table));
    let job_query_sql = Arc::new(format!("SELECT id, db_connection as db_uri, src_column as \"column\", dst_column, \"table\", \"schema\", embedding_model as model FROM {0}", &full_table_name));

    let full_table_name_r1 = full_table_name.clone();
    let job_query_sql_r1 = job_query_sql.clone();
    let client_r1 = client.clone();
    let job_tx_r1 = job_tx.clone();
    let logger_r1 = logger.clone();
    let job_batching_hashmap_r1 = job_batching_hashmap.clone();

    let insert_processor_task = tokio::spawn(async move {
        while let Some(notification) = notifications_rx.recv().await {
            let id = notification.id;

            if let Some(row_id) = notification.row_id {
                // Single row update received from client job, lock row and add to batching map
                let status = lock_row(client_r1.clone(), logger_r1.clone(), id, &row_id).await;

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
                let mut job = Job::new(row);
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
                let mut job = Job::new(row);
                // TODO take from job
                let pk = "id";
                job.set_is_init(false);
                let row_ids_str = row_ids.iter().map(|r| format!("'{r}'")).join(",");
                job.set_filter(&format!("{} IN ({row_ids_str})", quote_ident(pk)));
                let _ = job_tx.send(job).await;
            }

            // collect jobs every 10 seconds
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
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
              toggle_client_job(id, row.get::<&str, String>("db_uri").to_owned(), src_column.clone(), out_column.clone(), row.get::<&str, String>("table").to_owned(), row.get::<&str, String>("schema").to_owned(), logger.level.clone(), Some(job_insert_queue_tx.clone()), canceled_at.is_none()).await?;
            }

            if canceled_at.is_none() && notification.generate_missing {
                // this will be on startup to generate embeddings for rows that might be inserted
                // while daemon is offline
                job_insert_queue_tx.send(JobInsertNotification { id, init: init_finished_at.is_none(), generate_missing: true, filter: Some(format!("\"{src_column}\" IS NOT NULL AND \"{out_column}\" IS NULL")), limit: None, row_id: None }).await?;
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
pub async fn start(args: cli::DaemonArgs, logger: Option<Logger>) -> Result<(), anyhow::Error> {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Daemon", args.log_level.value())));
    logger.info("Staring Daemon");

    let (main_db_client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    tokio::spawn(async move { connection.await.unwrap() });

    let main_db_client = Arc::new(main_db_client);
    let notification_channel = "lantern_cloud_jobs";
    let data_path = create_data_path(logger.clone()).await;

    let (insert_notification_queue_tx, insert_notification_queue_rx): (
        Sender<JobInsertNotification>,
        Receiver<JobInsertNotification>,
    ) = mpsc::channel(args.queue_size);
    let (update_notification_queue_tx, update_notification_queue_rx): (
        Sender<JobUpdateNotification>,
        Receiver<JobUpdateNotification>,
    ) = mpsc::channel(args.queue_size);
    let (job_queue_tx, job_queue_rx): (Sender<Job>, Receiver<Job>) = mpsc::channel(args.queue_size);

    startup_hook(
        main_db_client.clone(),
        &args.table,
        &args.schema,
        &notification_channel,
        logger.clone(),
    )
    .await?;

    let handles = vec![
        Box::pin(db_notification_listener(
            args.uri.clone(),
            &notification_channel,
            insert_notification_queue_tx.clone(),
            update_notification_queue_tx.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(job_insert_processor(
            main_db_client.clone(),
            insert_notification_queue_rx,
            job_queue_tx,
            args.schema.clone(),
            args.table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(job_update_processor(
            main_db_client.clone(),
            update_notification_queue_rx,
            insert_notification_queue_tx.clone(),
            args.schema.clone(),
            args.table.clone(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(embedding_worker(
            job_queue_rx,
            insert_notification_queue_tx.clone(),
            main_db_client.clone(),
            args.schema.clone(),
            args.table.clone(),
            data_path.to_owned(),
            logger.clone(),
        )) as VoidFuture,
        Box::pin(collect_pending_jobs(
            main_db_client.clone(),
            update_notification_queue_tx.clone(),
            args.table.clone(),
        )) as VoidFuture,
    ];

    if let Err(e) = future::try_join_all(handles).await {
        logger.error(&e.to_string());
        logger.error("Fatal error exiting process");
        process::exit(1);
    }

    Ok(())
}
