pub mod daemon_test_utils {
    use crate::types::AnyhowVoidResult;
    use std::{env, time::Duration};
    use tokio_postgres::{Client, NoTls};

    pub static CLIENT_TABLE_NAME: &'static str = "_lantern_cloud_client1";
    pub static CLIENT_TABLE_NAME_2: &'static str = "_lantern_cloud_client2";

    #[cfg(not(feature = "autotune"))]
    pub static AUTOTUNE_JOB_TABLE_DEF: &'static str = "(id INT)";
    #[cfg(feature = "autotune")]
    pub static AUTOTUNE_JOB_TABLE_DEF: &'static str =
        crate::daemon::autotune_jobs::JOB_TABLE_DEFINITION;
    #[cfg(not(feature = "embeddings"))]
    pub static EMBEDDING_JOB_TABLE_DEF: &'static str = "(id INT)";
    #[cfg(feature = "embeddings")]
    pub static EMBEDDING_JOB_TABLE_DEF: &'static str =
        crate::daemon::embedding_jobs::JOB_TABLE_DEFINITION;

    async fn drop_db(client: &mut Client, name: &str) -> AnyhowVoidResult {
        client
            .execute(
                &format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{name}';"),
                &[],
            )
            .await?;
        client
            .execute(&format!("DROP DATABASE IF EXISTS {name}"), &[])
            .await?;

        Ok(())
    }

    async fn create_db(client: &mut Client, name: &str) -> AnyhowVoidResult {
        client
            .execute(&format!("CREATE DATABASE {name}"), &[])
            .await?;

        Ok(())
    }

    pub async fn setup_test(test_name: &str) -> Result<(String, Client), anyhow::Error> {
        let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
        let test_db_name = format!("_{test_name}");
        let (mut client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        drop_db(&mut client, &test_db_name).await?;
        create_db(&mut client, &test_db_name).await?;
        let new_connection_uri = format!("{db_uri}?dbname={test_db_name}");
        let (new_db_client, connection) =
            tokio_postgres::connect(&new_connection_uri, NoTls).await?;

        tokio::spawn(async move { connection.await.unwrap() });
        new_db_client
            .batch_execute(&format!(
                r#"
    CREATE EXTENSION IF NOT EXISTS lantern;
    CREATE SCHEMA IF NOT EXISTS _lantern_extras_internal;

    CREATE TABLE {CLIENT_TABLE_NAME} (
       id SERIAL PRIMARY KEY,
       title TEXT,
       num INT,
       title_embedding REAL[]
    );

    CREATE TABLE _lantern_extras_internal.embedding_generation_jobs ({embedding_job_table_def});
    CREATE TABLE _lantern_extras_internal.autotune_jobs ({autotune_job_table_def});
    
     "#,
                embedding_job_table_def = EMBEDDING_JOB_TABLE_DEF,
                autotune_job_table_def = AUTOTUNE_JOB_TABLE_DEF,
            ))
            .await?;

        Ok((new_connection_uri, new_db_client))
    }

    pub async fn wait_for_completion(
        client: &mut Client,
        query_condition: &str,
        timeout: u32,
    ) -> AnyhowVoidResult {
        let mut check_cnt = 0;
        loop {
            let client_data = client.query(query_condition, &[]).await.unwrap();
            let mut exists = false;

            if client_data.len() != 0 {
                exists = client_data[0].get::<usize, bool>(0);
            }

            if !exists {
                if check_cnt >= timeout {
                    anyhow::bail!("Force exit after {check_cnt} seconds");
                }
                check_cnt += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                break;
            }
        }

        Ok(())
    }
}
