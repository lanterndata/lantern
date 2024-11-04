#[cfg(feature = "autotune")]
pub mod autotune_jobs;
pub mod cli;
#[cfg(feature = "embeddings")]
mod client_embedding_jobs;
#[cfg(feature = "embeddings")]
pub mod embedding_jobs;
mod helpers;
mod types;

use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};
use tokio_postgres::{AsyncMessage, NoTls};
use tokio_util::sync::CancellationToken;

use crate::types::AnyhowVoidResult;
use crate::{logger::Logger, utils::get_full_table_name};
use types::{DaemonJobHandlerMap, JobRunArgs, TargetDB};

use types::{AutotuneProcessorArgs, EmbeddingProcessorArgs, JobType};

lazy_static! {
    static ref JOBS: DaemonJobHandlerMap = RwLock::new(HashMap::new());
}

static NOTIFICATION_CHANNEL: &'static str = "_lantern_daemon_updates";

async fn get_target_databases(
    args: &cli::DaemonArgs,
    logger: Arc<Logger>,
) -> Result<Vec<TargetDB>, anyhow::Error> {
    if args.target_db.is_none() && args.master_db.is_none() {
        anyhow::bail!("Please pass `--master-db` or `--target-db` to operate");
    }

    if let Some(target_db) = args.target_db.as_ref() {
        return target_db
            .iter()
            .map(|db_str| -> Result<TargetDB, anyhow::Error> { Ok(TargetDB::from_uri(db_str)?) })
            .collect::<Result<Vec<TargetDB>, anyhow::Error>>();
    }

    let db_uri = args.master_db.as_ref().unwrap();
    let (client, connection) = tokio_postgres::connect(db_uri, NoTls).await?;
    tokio::spawn(async move { connection.await.unwrap() });

    Ok(client
        .query(
            &format!(
                "SELECT db_uri FROM {databases_table}",
                databases_table = args.databases_table
            ),
            &[],
        )
        .await?
        .iter()
        .filter_map(
            |r| match TargetDB::from_uri(r.get::<&str, &str>("db_uri")) {
                Err(e) => {
                    logger.error(&e.to_string());
                    None
                }
                Ok(uri) => Some(uri),
            },
        )
        .collect::<Vec<TargetDB>>())
}

async fn destroy_jobs(target_db: &TargetDB, logger: Arc<Logger>) {
    logger.info(&format!("Destroying tasks for {}", target_db.name));
    let mut jobs = JOBS.write().await;
    let cancel_token = jobs.remove(&target_db.name);

    if cancel_token.is_none() {
        return;
    }

    let cancel_token = cancel_token.unwrap();

    cancel_token.cancel();
}

async fn spawn_job(
    target_db: Arc<TargetDB>,
    args: Arc<cli::DaemonArgs>,
    job_type: JobType,
    parent_cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    let mut retry_interval = 5;

    let log_label = match job_type {
        JobType::Embeddings(_) => "embeddings",
        JobType::Autotune(_) => "autotune",
    };

    let logger = Arc::new(Logger::new(
        &format!("{} - {log_label}", &target_db.name),
        args.log_level.value(),
    ));

    let mut last_retry = Instant::now();

    loop {
        // If run inside postgres, in case of error
        // We will cancel the parent task and let bgworker
        // handle the task restart
        let cancel_token = if args.inside_postgres {
            parent_cancel_token.clone()
        } else {
            parent_cancel_token.child_token()
        };

        let mut jobs = JOBS.write().await;
        jobs.insert(target_db.name.clone(), cancel_token.clone());
        drop(jobs);

        let result: Result<(), anyhow::Error> = match &job_type {
            #[cfg(feature = "embeddings")]
            JobType::Embeddings(processor_tx) => {
                embedding_jobs::start(
                    JobRunArgs {
                        label: args.label.clone(),
                        uri: target_db.uri.clone(),
                        schema: args.schema.clone(),
                        log_level: args.log_level.value(),
                        data_path: args.data_path.clone(),
                        table_name: "embedding_generation_jobs".to_owned(),
                    },
                    processor_tx.clone(),
                    logger.clone(),
                    cancel_token.clone(),
                )
                .await
            }
            #[cfg(not(feature = "embeddings"))]
            JobType::Embeddings(_) => {
                anyhow::bail!("Embedding jobs are not enabled");
            }
            #[cfg(feature = "autotune")]
            JobType::Autotune(processor_tx) => {
                autotune_jobs::start(
                    JobRunArgs {
                        label: args.label.clone(),
                        uri: target_db.uri.clone(),
                        schema: args.schema.clone(),
                        log_level: args.log_level.value(),
                        data_path: None,
                        table_name: "autotune_jobs".to_owned(),
                    },
                    processor_tx.clone(),
                    logger.clone(),
                    cancel_token.clone(),
                )
                .await
            }
            #[cfg(not(feature = "autotune"))]
            JobType::Autotune(_) => {
                anyhow::bail!("Autotune jobs are not enabled");
            }
        };

        cancel_token.cancel();
        if let Err(e) = result {
            if last_retry.elapsed().as_secs() > 30 {
                // reset retry exponential backoff time if job was not failing constantly
                retry_interval = 10;
            }
            logger.error(&format!(
                "Error from job: {e} (retry after {retry_interval}s)"
            ));
            tokio::time::sleep(Duration::from_secs(retry_interval)).await;
            retry_interval *= 2;
            last_retry = Instant::now();
            continue;
        }

        break;
    }

    Ok(())
}

async fn spawn_jobs(
    target_db: TargetDB,
    args: Arc<cli::DaemonArgs>,
    embedding_tx: Sender<EmbeddingProcessorArgs>,
    autotune_tx: Sender<AutotuneProcessorArgs>,
    cancel_token: CancellationToken,
) {
    let target_db = Arc::new(target_db);

    if args.embeddings {
        tokio::spawn(spawn_job(
            target_db.clone(),
            args.clone(),
            JobType::Embeddings(embedding_tx),
            cancel_token.clone(),
        ));
    }

    if args.autotune {
        tokio::spawn(spawn_job(
            target_db.clone(),
            args.clone(),
            JobType::Autotune(autotune_tx),
            cancel_token.clone(),
        ));
    }
}

async fn db_change_listener(
    args: Arc<cli::DaemonArgs>,
    embedding_tx: Sender<EmbeddingProcessorArgs>,
    autotune_tx: Sender<AutotuneProcessorArgs>,
    logger: Arc<Logger>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    logger.info("Setting up triggers");
    let (client, mut connection) =
        tokio_postgres::connect(args.master_db.as_ref().unwrap(), NoTls).await?;

    let client = Arc::new(client);
    let client_ref = client.clone();
    let cancel_token_clone = cancel_token.clone();
    let logger_clone = logger.clone();
    let ping_logger = logger.clone();

    let full_table_name = get_full_table_name(&args.master_db_schema, &args.databases_table);
    let insert_trigger_name = "_lantern_daemon_insert_trigger";
    let delete_trigger_name = "_lantern_daemon_delete_trigger";
    let insert_function_name = "_lantern_daemon_insert_trigger_notify";
    let delete_function_name = "_lantern_daemon_delete_trigger_notify";

    let healthcheck_task = tokio::spawn(async move {
        ping_logger.debug(&format!("Sending ping queries to master db each 30s"));

        loop {
            match client_ref.query_one("SELECT 1", &[]).await {
                Ok(_) => {}
                Err(e) => {
                    ping_logger.error(&format!("Ping query failed with {e}. Exitting"));
                    cancel_token_clone.cancel();
                    break;
                }
            };
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let task = tokio::spawn(async move {
        // Poll messages from connection and forward it to stream
        let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
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
                    let parts: Vec<&str> = not.payload().split("::").collect();

                    if parts.len() < 2 {
                        logger.error(&format!("Invalid notification received {}", not.payload()));
                        return;
                    }
                    let action: &str = parts[0];
                    let db_uri: &str = parts[1];
                    let target_db = TargetDB::from_uri(db_uri);

                    if let Err(e) = &target_db {
                         logger.error(&format!("Failed to parse db uri: {e}"));
                         return;
                        }
                        let target_db = target_db.unwrap();

                    match action {
                        "delete" => {
                            destroy_jobs(&target_db, logger.clone()).await;
                        }
                        "insert" => {
                            spawn_jobs(target_db, args.clone(), embedding_tx.clone(), autotune_tx.clone(), cancel_token.clone()).await;
                        }
                        _ => logger.error(&format!("Invalid action received: {action}")),
                    }
                }
               },
               _ = cancel_token.cancelled() => {
                    break;
               }
            }
        }
        logger.debug(&format!(
            "db_change_listener: Database connection stream finished"
        ));
    });

    logger_clone.info("Listening for master db changes");
    client.batch_execute(&format!("
      DROP TRIGGER IF EXISTS {insert_trigger_name} ON {full_table_name};
      DROP TRIGGER IF EXISTS {delete_trigger_name} ON {full_table_name};

      CREATE OR REPLACE FUNCTION {insert_function_name}() RETURNS TRIGGER AS $$
        BEGIN
          PERFORM pg_notify('{channel}', 'insert::' || NEW.db_uri);
          RETURN NULL;
        END;
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE FUNCTION {delete_function_name}() RETURNS TRIGGER AS $$
        BEGIN
          PERFORM pg_notify('{channel}', 'delete::' || OLD.db_uri);
          RETURN NULL;
        END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER {insert_trigger_name} AFTER INSERT ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {insert_function_name}();
      CREATE TRIGGER {delete_trigger_name} AFTER DELETE ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {delete_function_name}();

      LISTEN {NOTIFICATION_CHANNEL};
    ", channel = NOTIFICATION_CHANNEL)).await?;

    tokio::select! {
        _ = healthcheck_task => {}
        _ = task => {},
    }

    Ok(())
}

pub async fn start(
    args: cli::DaemonArgs,
    logger: Option<Logger>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Daemon", args.log_level.value())));

    let target_databases: Vec<TargetDB> = get_target_databases(&args, logger.clone()).await?;

    let args_arc = Arc::new(args);
    let args_arc_clone = args_arc.clone();
    let embedding_channel: (
        Sender<EmbeddingProcessorArgs>,
        Receiver<EmbeddingProcessorArgs>,
    ) = mpsc::channel(1);
    let autotune_channel: (
        Sender<AutotuneProcessorArgs>,
        Receiver<AutotuneProcessorArgs>,
    ) = mpsc::channel(1);

    #[cfg(feature = "embeddings")]
    if args_arc.embeddings {
        tokio::spawn(embedding_jobs::embedding_job_processor(
            embedding_channel.1,
            cancel_token.clone(),
        ));
    }

    #[cfg(feature = "autotune")]
    if args_arc.autotune {
        tokio::spawn(autotune_jobs::autotune_job_processor(
            autotune_channel.1,
            cancel_token.clone(),
        ));
    }

    for target_db in target_databases {
        spawn_jobs(
            target_db,
            args_arc_clone.clone(),
            embedding_channel.0.clone(),
            autotune_channel.0.clone(),
            cancel_token.clone(),
        )
        .await;
    }

    if args_arc.master_db.is_some() {
        db_change_listener(
            args_arc.clone(),
            embedding_channel.0.clone(),
            autotune_channel.0.clone(),
            logger.clone(),
            cancel_token.clone(),
        )
        .await?;
    }

    Ok(())
}
