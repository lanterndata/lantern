use super::helpers::{check_table_exists, get_missing_rows_filter};
use super::types::{ClientJobSignal, ClientJobsMap, JobInsertNotification};
use crate::logger::{LogLevel, Logger};
use crate::types::AnyhowVoidResult;
use crate::utils::{
    append_params_to_uri, get_common_embedding_ignore_filters, get_full_table_name, quote_ident,
};
use futures::StreamExt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_postgres::{AsyncMessage, Client, NoTls};

pub async fn toggle_client_job(
    client_jobs_map: Arc<ClientJobsMap>,
    job_id: i32,
    db_uri: String,
    pk: String,
    column: String,
    out_column: String,
    table: String,
    schema: String,
    log_level: LogLevel,
    log_label: &str,
    job_insert_queue_tx: Option<UnboundedSender<JobInsertNotification>>,
    enable: bool,
) -> AnyhowVoidResult {
    let logger = Arc::new(Logger::new(&format!("{log_label}|{job_id}"), log_level));
    let job_logger = logger.clone();
    if enable {
        let job_insert_queue_tx = job_insert_queue_tx.unwrap();
        let task_logger = logger.clone();
        tokio::spawn(async move {
            let res = start_client_job(
                job_logger,
                client_jobs_map,
                job_id,
                pk,
                db_uri,
                column,
                out_column,
                table,
                schema,
                job_insert_queue_tx,
            )
            .await;

            if let Err(e) = res {
                task_logger.error(&format!("Error while starting job {}", e.to_string()));
            }
        });
    } else {
        let res = stop_client_job(
            job_logger,
            client_jobs_map,
            &db_uri,
            job_id,
            &table,
            &schema,
            true,
        )
        .await;
        if let Err(e) = res {
            logger.error(&format!("Error while stopping job {}", e.to_string()));
        }
    }

    Ok(())
}

fn get_trigger_name(job_id: i32, operation: &str) -> String {
    return format!("trigger_lantern_jobs_v2_{operation}_{job_id}");
}

fn get_function_name(job_id: i32) -> String {
    return format!("notify_insert_lantern_daemon_v2_{job_id}");
}

fn get_notification_channel_name(job_id: i32) -> String {
    return format!("lantern_client_notifications_v2_{job_id}");
}

async fn setup_client_triggers(
    job_id: i32,
    pk: &str,
    client: Arc<Client>,
    column: Arc<String>,
    table: Arc<String>,
    schema: Arc<String>,
    channel: Arc<String>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    logger.info("Setting Up Client Triggers");
    // verify that table exists
    let full_table_name = get_full_table_name(schema.deref(), table.deref());
    check_table_exists(client.clone(), &full_table_name).await?;

    let column_name = quote_ident(&column);
    let pk = quote_ident(pk);
    let function_name = get_function_name(job_id);
    let insert_trigger_name = get_trigger_name(job_id, "insert");
    let update_trigger_name = get_trigger_name(job_id, "update");
    let function_name = get_full_table_name(schema.deref(), &function_name);

    // Set up trigger on table insert
    client
        .batch_execute(&format!(
            "
            CREATE OR REPLACE FUNCTION {function_name}() RETURNS TRIGGER AS $$
              BEGIN
                IF ({common_filter})
                THEN
                    PERFORM pg_notify('{channel}', NEW.{pk}::TEXT || ':' || '{job_id}');
                END IF;
                RETURN NULL;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE TRIGGER {insert_trigger_name}
            AFTER INSERT 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {function_name}();

            CREATE OR REPLACE TRIGGER {update_trigger_name}
            AFTER UPDATE OF {column_name}
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {function_name}();
        ",
            common_filter = get_common_embedding_ignore_filters(&format!("NEW.{column_name}"))
        ))
        .await?;

    Ok(())
}

async fn remove_client_triggers(
    db_uri: &str,
    job_id: i32,
    table: &str,
    schema: &str,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    logger.info("Removing Client Triggers");
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
    let db_connection_logger = logger.clone();
    let db_connection_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            db_connection_logger.error(&e.to_string());
        }
    });
    let full_table_name = get_full_table_name(schema, table);

    let function_name = get_function_name(job_id);
    let function_name = get_full_table_name(schema, &function_name);
    let insert_trigger_name = get_trigger_name(job_id, "insert");
    let update_trigger_name = get_trigger_name(job_id, "update");
    // Set up trigger on table insert
    db_client
        .batch_execute(&format!(
            "
            DROP TRIGGER IF EXISTS {insert_trigger_name} ON {full_table_name};
            DROP TRIGGER IF EXISTS {update_trigger_name} ON {full_table_name};
            DROP FUNCTION IF EXISTS {function_name};
        ",
        ))
        .await?;

    db_connection_task.abort();
    Ok(())
}

async fn client_notification_listener(
    db_uri: Arc<String>,
    notification_channel: Arc<String>,
    job_signal_tx: UnboundedSender<ClientJobSignal>,
    job_insert_queue_tx: UnboundedSender<JobInsertNotification>,
    logger: Arc<Logger>,
) -> Result<UnboundedSender<()>, anyhow::Error> {
    let uri = append_params_to_uri(&db_uri, "connect_timeout=10");
    let (client, mut connection) = tokio_postgres::connect(&uri.as_str(), NoTls).await?;

    let client = Arc::new(client);

    logger.info("Listening for notifications");

    let ping_logger = logger.clone();
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap().as_millis();
    let since_the_epoch_clone = since_the_epoch.clone();
    let client_ref = client.clone();
    let task = tokio::spawn(async move {
        // Poll messages from connection and forward it to stream
        let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(message) = stream.next().await {
            if let Err(e) = &message {
                logger.error(&format!(
                    "Error receiving message from DB: {}",
                    &e.to_string()
                ));
                let _ = job_signal_tx.send(ClientJobSignal::Restart);
                break;
            }

            let message = message.unwrap();

            if let AsyncMessage::Notification(not) = message {
                let parts: Vec<&str> = not.payload().split(':').collect();

                if parts.len() < 2 {
                    logger.error(&format!("Invalid notification received {}", not.payload()));
                    continue;
                }
                let pk: &str = parts[0];
                let job_id = i32::from_str_radix(parts[1], 10).unwrap();
                let result = job_insert_queue_tx.send(JobInsertNotification {
                    id: job_id,
                    generate_missing: false,
                    row_id: Some(pk.to_owned()),
                    filter: None,
                    limit: None,
                });

                if let Err(e) = result {
                    logger.error(&e.to_string());
                }
            }
        }
        drop(client_ref);
        logger.debug(&format!(
            "Database connection stream finished, task_id: {since_the_epoch}"
        ));
    });

    client
        .batch_execute(&format!("LISTEN {notification_channel};"))
        .await?;

    // Task cancellation handler
    let (tx, mut rx): (UnboundedSender<()>, UnboundedReceiver<()>) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(_) = rx.recv().await {
            task.abort();
            break;
        }
    });

    let cancel_tx = tx.clone();
    tokio::spawn(async move {
        ping_logger.debug(&format!(
            "Sending ping queries each 30s, task_id: {since_the_epoch_clone}"
        ));
        loop {
            match client.query_one("SELECT 1", &[]).await {
                Ok(_) => {}
                Err(e) => {
                    ping_logger.error(&format!(
                        "Ping query failed with {e}. Sending restart, task_id: {since_the_epoch_clone}"
                    ));
                    match cancel_tx.send(()) {
                        Ok(_) => {}
                        Err(e) => {
                            ping_logger.error(&format!("Sending cancel event failed with {e}, task_id: {since_the_epoch_clone}"));
                        }
                    }
                    break;
                }
            };
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    Ok(tx)
}

async fn start_client_job(
    logger: Arc<Logger>,
    client_jobs_map: Arc<ClientJobsMap>,
    job_id: i32,
    pk: String,
    db_uri: String,
    column: String,
    out_column: String,
    table: String,
    schema: String,
    job_insert_queue_tx: UnboundedSender<JobInsertNotification>,
) -> AnyhowVoidResult {
    let jobs = client_jobs_map.read().await;
    if jobs.get(&job_id).is_some() {
        logger.warn("Job is active, cancelling before running again");
        drop(jobs);
        stop_client_job(
            logger.clone(),
            client_jobs_map.clone(),
            &db_uri,
            job_id,
            &table,
            &schema,
            false,
        )
        .await?;
    } else {
        drop(jobs);
    }

    let (job_signal_tx, mut job_signal_rx): (
        UnboundedSender<ClientJobSignal>,
        UnboundedReceiver<ClientJobSignal>,
    ) = mpsc::unbounded_channel();
    logger.info("Starting Client Listener");

    // Connect to client database
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
    let db_client = Arc::new(db_client);
    let db_connection_logger = logger.clone();
    let db_connection_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            db_connection_logger.error(&e.to_string());
        }
    });

    let notification_channel = Arc::new(get_notification_channel_name(job_id));
    // Wrap variables into Arc to share between tasks
    let db_uri = Arc::new(db_uri);
    let column = Arc::new(column);
    let table = Arc::new(table);
    let schema = Arc::new(schema);

    // Setup triggers on client database table, to get new inserts
    setup_client_triggers(
        job_id,
        &pk,
        db_client,
        column.clone(),
        table.clone(),
        schema.clone(),
        notification_channel.clone(),
        logger.clone(),
    )
    .await?;
    // close the database connection as we will create a new one for notification listener
    db_connection_task.abort();
    drop(db_connection_task);

    let client_task_logger = logger.clone();
    let job_signal_tx_clone = job_signal_tx.clone();

    // Save job tx into shared hashmap, so we will be able to stop the job later
    let mut jobs = client_jobs_map.write().await;
    jobs.insert(job_id, job_signal_tx);
    drop(jobs);

    let mut cancel_listener_task = client_notification_listener(
        db_uri.clone(),
        notification_channel.clone(),
        job_signal_tx_clone.clone(),
        job_insert_queue_tx.clone(),
        client_task_logger.clone(),
    )
    .await?;

    let signal_listener_logger = logger.clone();
    // Listen for signals and abort/restart jobs
    tokio::spawn(async move {
        let mut restart_interval = 10;
        while let Some(signal) = job_signal_rx.recv().await {
            match signal {
                ClientJobSignal::Stop => {
                    // remove client listener
                    if let Err(e) = cancel_listener_task.send(()) {
                        signal_listener_logger
                            .error(&format!("Failed to cancel client listener: {e}"));
                    }
                    // close channel
                    signal_listener_logger.info("Job stopped");
                    break;
                }
                ClientJobSignal::Restart => loop {
                    signal_listener_logger.info("Restarting job");

                    let res = client_notification_listener(
                        db_uri.clone(),
                        notification_channel.clone(),
                        job_signal_tx_clone.clone(),
                        job_insert_queue_tx.clone(),
                        client_task_logger.clone(),
                    )
                    .await;

                    if let Ok(tx) = res {
                        restart_interval = 10;
                        cancel_listener_task = tx;
                        job_insert_queue_tx.send(JobInsertNotification {
                            id: job_id,
                            generate_missing: true,
                            row_id: None,
                            filter: Some(get_missing_rows_filter(&column, &out_column)),
                            limit: None,
                        })?;
                        break;
                    } else {
                        tokio::time::sleep(Duration::from_secs(restart_interval)).await;
                        restart_interval *= 2;
                    }
                },
            }
        }
        Ok(()) as AnyhowVoidResult
    });

    Ok(())
}

pub async fn stop_client_job(
    logger: Arc<Logger>,
    client_jobs_map: Arc<ClientJobsMap>,
    db_uri: &str,
    job_id: i32,
    table: &str,
    schema: &str,
    remove: bool,
) -> AnyhowVoidResult {
    if remove {
        // remove client triggers
        let res = remove_client_triggers(db_uri, job_id, table, schema, logger.clone()).await;

        if let Err(e) = res {
            logger.error(&format!("Error while removing triggers: {}", e))
        }
    }

    // Cancel job and remove from hashmap
    let mut jobs = client_jobs_map.write().await;
    let job = jobs.remove(&job_id);
    drop(jobs);

    match job {
        None => {
            logger.error(&format!("Job {job_id} not found in job list"));
        }

        Some(job) => job.send(ClientJobSignal::Stop)?,
    }

    Ok(())
}
