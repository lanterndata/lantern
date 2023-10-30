use crate::types::AnyhowVoidResult;
use std::sync::Arc;
use tokio_postgres::Client;

pub fn get_full_table_name(schema: &str, table: &str) -> String {
    format!("\"{schema}\".\"{table}\"")
}

pub async fn check_table_exists(client: Arc<Client>, table: &str) -> AnyhowVoidResult {
    // verify that table exists
    if let Err(_) = client
        .execute(&format!("SELECT ctid FROM {} LIMIT 1", table), &[])
        .await
    {
        anyhow::bail!("Table {table} does not exist");
    }

    Ok(())
}
