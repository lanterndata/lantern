use crate::helpers::check_table_exists;
use crate::types::{AnyhowVoidResult, JobInsertNotification};
use futures::StreamExt;
use lantern_logger::{LogLevel, Logger};
use lantern_utils::{append_params_to_uri, get_full_table_name, quote_ident};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};
use tokio_postgres::{AsyncMessage, Client, NoTls};

enum Signal {
    Stop,
    Restart,
}

lazy_static! {
    static ref CLIENT_JOBS: RwLock<HashMap<i32, Sender<Signal>>> = RwLock::new(HashMap::new());
}

pub async fn toggle_client_job(
    job_id: i32,
    db_uri: String,
    src_column: String,
    table: String,
    schema: String,
    log_level: LogLevel,
    job_insert_queue_tx: Option<Sender<JobInsertNotification>>,
    enable: bool,
) -> AnyhowVoidResult {
    let logger = Arc::new(Logger::new(&format!("Job {job_id}"), log_level));
    let job_logger = logger.clone();
    if enable {
        let job_insert_queue_tx = job_insert_queue_tx.unwrap();
        let task_logger = logger.clone();
        tokio::spawn(async move {
            let res = start_client_job(
                job_logger,
                job_id,
                db_uri,
                src_column,
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
            &db_uri,
            job_id,
            &src_column,
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

async fn setup_client_triggers(
    job_id: i32,
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

    // Set up trigger on table insert
    let function_name = quote_ident(&format!("notify_insert_lantern_daemon_{table}_{column}"));
    let trigger_name = quote_ident(&format!("trigger_lantern_jobs_insert_{column}"));

    client
        .batch_execute(&format!(
            "
            CREATE OR REPLACE FUNCTION {function_name}() RETURNS TRIGGER AS $$
              BEGIN
                PERFORM pg_notify('{channel}', NEW.id::TEXT || ':' || '{job_id}');
                RETURN NULL;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE TRIGGER {trigger_name}
            AFTER INSERT 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {function_name}();
        ",
        ))
        .await?;

    Ok(())
}

async fn remove_client_triggers(
    db_uri: &str,
    column: &str,
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

    let function_name = quote_ident(&format!("notify_insert_lantern_daemon_{table}_{column}"));
    let trigger_name = quote_ident(&format!("trigger_lantern_jobs_insert_{column}"));
    // Set up trigger on table insert
    db_client
        .batch_execute(&format!(
            "
            DROP FUNCTION IF EXISTS {function_name};
            DROP TRIGGER IF EXISTS {trigger_name} ON {full_table_name};
        ",
        ))
        .await?;

    db_connection_task.abort();
    Ok(())
}

async fn client_notification_listener(
    db_uri: Arc<String>,
    notification_channel: Arc<String>,
    job_signal_tx: Sender<Signal>,
    job_insert_queue_tx: Sender<JobInsertNotification>,
    logger: Arc<Logger>,
) -> Result<Sender<()>, anyhow::Error> {
    let uri = append_params_to_uri(&db_uri, "connect_timeout=10");
    let (client, mut connection) = tokio_postgres::connect(&uri.as_str(), NoTls).await?;

    let client = Arc::new(client);

    logger.info("Listening for notifications");

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
                let _ = job_signal_tx.send(Signal::Restart).await;
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
                let result = job_insert_queue_tx
                    .send(JobInsertNotification {
                        id: job_id,
                        init: false,
                        generate_missing: false,
                        row_id: Some(pk.to_owned()),
                        filter: None,
                        limit: None,
                    })
                    .await;

                if let Err(e) = result {
                    logger.error(&e.to_string());
                }
            }
        }
        drop(client_ref);
    });

    client
        .batch_execute(&format!("LISTEN {notification_channel};"))
        .await?;

    // Task cancellation handler
    let (tx, mut rx): (Sender<()>, Receiver<()>) = mpsc::channel(1);

    tokio::spawn(async move {
        while let Some(_) = rx.recv().await {
            task.abort();
            break;
        }
    });

    Ok(tx)
}

async fn start_client_job(
    logger: Arc<Logger>,
    job_id: i32,
    db_uri: String,
    src_column: String,
    table: String,
    schema: String,
    job_insert_queue_tx: Sender<JobInsertNotification>,
) -> AnyhowVoidResult {
    let jobs = CLIENT_JOBS.read().await;
    if jobs.get(&job_id).is_some() {
        logger.warn("Job is active, cancelling before running again");
        drop(jobs);
        stop_client_job(
            logger.clone(),
            &db_uri,
            job_id,
            &src_column,
            &table,
            &schema,
            false,
        )
        .await?;
    } else {
        drop(jobs);
    }

    let (job_signal_tx, mut job_signal_rx): (Sender<Signal>, Receiver<Signal>) = mpsc::channel(1);
    logger.info("Staring Client Listener");

    // Connect to client database
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await?;
    let db_client = Arc::new(db_client);
    let db_connection_logger = logger.clone();
    let db_connection_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            db_connection_logger.error(&e.to_string());
        }
    });

    let notification_channel = Arc::new(quote_ident(&format!(
        "lantern_client_notifications_{table}_{src_column}"
    )));

    // Wrap variables into Arc to share between tasks
    let db_uri = Arc::new(db_uri);
    let src_column = Arc::new(src_column);
    let table = Arc::new(table);
    let schema = Arc::new(schema);

    // Setup triggers on client database table, to get new inserts
    setup_client_triggers(
        job_id,
        db_client,
        src_column.clone(),
        table.clone(),
        schema.clone(),
        notification_channel.clone(),
        logger.clone(),
    )
    .await?;
    // close the database connection as we will create a new one for notification listener
    db_connection_task.abort();

    let client_task_logger = logger.clone();
    let job_signal_tx_clone = job_signal_tx.clone();

    // Save job tx into shared hashmap, so we will be able to stop the job later
    let mut jobs = CLIENT_JOBS.write().await;
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
        while let Some(signal) = job_signal_rx.recv().await {
            match signal {
                Signal::Stop => {
                    // remove client listener
                    if let Err(e) = cancel_listener_task.send(()).await {
                        signal_listener_logger
                            .error(&format!("Failed to cancel client listener: {e}"));
                    }
                    // close channel
                    signal_listener_logger.info("Job stopped");
                    break;
                }
                Signal::Restart => loop {
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
                        cancel_listener_task = tx;
                        break;
                    } else {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                },
            }
        }
        Ok(()) as AnyhowVoidResult
    });

    Ok(())
}

async fn stop_client_job(
    logger: Arc<Logger>,
    db_uri: &str,
    job_id: i32,
    src_column: &str,
    table: &str,
    schema: &str,
    remove: bool,
) -> AnyhowVoidResult {
    if remove {
        // remove client triggers
        let res = remove_client_triggers(db_uri, src_column, table, schema, logger.clone()).await;

        if let Err(e) = res {
            logger.error(&format!("Error while removing triggers: {}", e))
        }
    }

    // Cancel job and remove from hashmap
    let mut jobs = CLIENT_JOBS.write().await;
    let job = jobs.remove(&job_id);
    drop(jobs);

    match job {
        None => {
            logger.error(&format!("Job {job_id} not found in job list"));
        }

        Some(job) => job.send(Signal::Stop).await?,
    }

    Ok(())
}
