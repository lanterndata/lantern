use tokio::sync::RwLock;
mod autotune_jobs;
use tokio_postgres::{AsyncMessage, NoTls};
pub mod cli;
mod client_embedding_jobs;
mod embedding_jobs;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
mod external_index_jobs;
mod helpers;
mod types;

use crate::types::AnyhowVoidResult;
use crate::{logger::Logger, utils::get_full_table_name};
use std::collections::HashMap;
use std::sync::Arc;
use types::{DaemonJobHandlerMap, JobRunArgs, TargetDB};

lazy_static! {
    static ref JOBS: DaemonJobHandlerMap = RwLock::new(HashMap::new());
}

static NOTIFICATION_CHANNEL: &'static str = "_lantern_daemon_updates";

async fn get_target_databases(args: &cli::DaemonArgs) -> Result<Vec<TargetDB>, anyhow::Error> {
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
        .map(|r| TargetDB::from_uri(r.get::<&str, &str>("db_uri")).unwrap())
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

async fn spawn_jobs(target_db: &TargetDB, args: Arc<cli::DaemonArgs>) {
    let mut jobs = JOBS.write().await;
    let cancel_token = CancellationToken::new();
    jobs.insert(target_db.name.clone(), cancel_token.clone());

    if args.embeddings {
        let logger = Arc::new(Logger::new(
            &format!("{} - embeddings", &target_db.name),
            args.log_level.value(),
        ));
        let args = args.clone();
        let uri = target_db.uri.clone();
        let cancel_token = cancel_token.clone();

        tokio::spawn(async move {
            let result = embedding_jobs::start(
                JobRunArgs {
                    uri,
                    schema: args.schema.clone(),
                    log_level: args.log_level.value(),
                    table_name: "embedding_generation_jobs".to_owned(),
                },
                logger.clone(),
                cancel_token,
            )
            .await;

            if let Err(e) = result {
                logger.error(&format!("Error from embedding job: {e}"));
            }
        });
    }

    if args.external_index {
        let logger = Arc::new(Logger::new(
            &format!("{} - indexing", &target_db.name),
            args.log_level.value(),
        ));
        let args = args.clone();
        let uri = target_db.uri.clone();
        let cancel_token = cancel_token.clone();

        tokio::spawn(async move {
            let result = external_index_jobs::start(
                JobRunArgs {
                    uri,
                    schema: args.schema.clone(),
                    log_level: args.log_level.value(),
                    table_name: "external_index_jobs".to_owned(),
                },
                logger.clone(),
                cancel_token,
            )
            .await;

            if let Err(e) = result {
                logger.error(&format!("Error from indexing job: {e}"));
            }
        });
    }

    if args.autotune {
        let logger = Arc::new(Logger::new(
            &format!("{} - autotune", &target_db.name),
            args.log_level.value(),
        ));

        let args = args.clone();
        let uri = target_db.uri.clone();
        let cancel_token = cancel_token.clone();

        tokio::spawn(async move {
            let result = autotune_jobs::start(
                JobRunArgs {
                    uri,
                    schema: args.schema.clone(),
                    log_level: args.log_level.value(),
                    table_name: "autotune_jobs".to_owned(),
                },
                logger.clone(),
                cancel_token,
            )
            .await;

            if let Err(e) = result {
                logger.error(&format!("Error from autotune job: {e}"));
            }
        });
    }
}

async fn db_change_listener(
    args: Arc<cli::DaemonArgs>,
    logger: Arc<Logger>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    logger.info("Setting up triggers");
    let (client, mut connection) =
        tokio_postgres::connect(args.master_db.as_ref().unwrap(), NoTls).await?;

    let logger_clone = logger.clone();
    let full_table_name = get_full_table_name(&args.master_db_schema, &args.databases_table);
    let insert_trigger_name = "_lantern_daemon_insert_trigger";
    let delete_trigger_name = "_lantern_daemon_delete_trigger";
    let insert_function_name = "_lantern_daemon_insert_trigger_notify";
    let delete_function_name = "_lantern_daemon_delete_trigger_notify";

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
                            spawn_jobs(&target_db, args.clone()).await;
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

    task.await?;
    Ok(())
}

#[tokio::main]
pub async fn start(
    args: cli::DaemonArgs,
    logger: Option<Logger>,
    cancel_token: CancellationToken,
) -> AnyhowVoidResult {
    let logger = Arc::new(logger.unwrap_or(Logger::new("Lantern Daemon", args.log_level.value())));

    let target_databases: Vec<TargetDB> = get_target_databases(&args).await?;

    logger.info(&format!("Target Databases -> {:?}", target_databases));

    let args_arc = Arc::new(args);
    let args_arc_clone = args_arc.clone();
    for target_db in &target_databases {
        spawn_jobs(target_db, args_arc_clone.clone()).await;
    }

    if args_arc.master_db.is_some() {
        db_change_listener(args_arc.clone(), logger.clone(), cancel_token.clone()).await?;
    }

    cancel_token.cancelled().await;

    Ok(())
}
