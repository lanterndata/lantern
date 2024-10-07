use super::{COLLECTION_SCHEMA_NAME, COLLECTION_TABLE_NAME};
use crate::types::AnyhowVoidResult;
use deadpool_postgres::Pool;

pub async fn setup_tables(pool: &Pool) -> AnyhowVoidResult {
    let client = pool.get().await?;

    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {COLLECTION_SCHEMA_NAME};
             CREATE TABLE IF NOT EXISTS {COLLECTION_TABLE_NAME} (
         id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
         name NAME
        );",
        ))
        .await?;
    Ok(())
}
