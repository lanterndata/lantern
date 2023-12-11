use std::sync::Arc;

use lantern_logger::Logger;
use lantern_utils::get_full_table_name;
use tokio_postgres::{Client, NoTls};

use crate::{cli, types::AnyhowVoidResult};

pub async fn drop_old_triggers_and_functions(
    client: Arc<Client>,
    emb_jobs_full_table_name: &str,
) -> AnyhowVoidResult {
    client
        .batch_execute(&format!(
            "
       DROP FUNCTION IF EXISTS notify_insert_lantern_daemon CASCADE;
       DROP FUNCTION IF EXISTS notify_update_lantern_daemon CASCADE;
       DROP TRIGGER IF EXISTS trigger_lantern_jobs_insert ON {emb_jobs_full_table_name} CASCADE;
       DROP TRIGGER IF EXISTS trigger_lantern_jobs_update ON {emb_jobs_full_table_name} CASCADE;
    ",
        ))
        .await?;
    Ok(())
}

#[tokio::main]
pub async fn run_migrations(args: &cli::DaemonArgs, logger: Arc<Logger>) -> AnyhowVoidResult {
    logger.info("Running migrations");
    let (client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    let connection_task = tokio::spawn(async move { connection.await.unwrap() });

    let client = Arc::new(client);

    if args.embedding_table.is_some() {
        drop_old_triggers_and_functions(
            client,
            &get_full_table_name(&args.schema, args.embedding_table.as_deref().unwrap()),
        )
        .await?;
        logger.info("migration: drop_old_triggers_and_functions [OK]");
    }

    connection_task.abort();
    logger.info("All migrations run successfully");
    Ok(())
}
