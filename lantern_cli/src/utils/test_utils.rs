pub mod daemon_test_utils {
    use crate::{daemon, types::AnyhowVoidResult};
    use std::{env, time::Duration};
    use tokio_postgres::{Client, NoTls};

    pub static CLIENT_TABLE_NAME: &'static str = "_lantern_cloud_client1";
    pub static CLIENT_TABLE_NAME_2: &'static str = "_lantern_cloud_client2";
    pub static EMBEDDING_USAGE_TABLE_NAME: &'static str = "_daemon_embedding_usage";

    async fn drop_db(client: &mut Client, name: &str) -> AnyhowVoidResult {
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

    CREATE TABLE {CLIENT_TABLE_NAME} (
       id SERIAL PRIMARY KEY,
       title TEXT,
       title_embedding REAL[]
    );

    CREATE TABLE {EMBEDDING_USAGE_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        job_id INT NOT NULL UNIQUE,
        usage INT NOT NULL DEFAULT 0,
        tokens BIGINT NOT NULL DEFAULT 0
   );

    CREATE OR REPLACE FUNCTION _lantern_internal.increment_embedding_usage_and_tokens(v_job_id integer, v_usage integer, v_tokens BIGINT DEFAULT 0)
     RETURNS VOID
     LANGUAGE plpgsql
    AS $function$
    BEGIN
    INSERT INTO {EMBEDDING_USAGE_TABLE_NAME} (job_id, usage, tokens)
      VALUES (v_job_id, v_usage, v_tokens)
      ON CONFLICT (job_id)
      DO UPDATE SET
        usage = {EMBEDDING_USAGE_TABLE_NAME}.usage + v_usage,
        tokens = {EMBEDDING_USAGE_TABLE_NAME}.tokens + v_tokens;
    END;
    $function$;

    CREATE TABLE _lantern_internal.embedding_generation_jobs ({embedding_job_table_def});
    CREATE TABLE _lantern_internal.autotune_jobs ({autotune_job_table_def});
    CREATE TABLE _lantern_internal.external_index_jobs ({indexing_job_table_def});
    
     "#, 
        embedding_job_table_def = daemon::embedding_jobs::JOB_TABLE_DEFINITION,
        autotune_job_table_def = daemon::autotune_jobs::JOB_TABLE_DEFINITION,
        indexing_job_table_def = daemon::external_index_jobs::JOB_TABLE_DEFINITION,
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
            let client_data = client.query_one(query_condition, &[]).await.unwrap();
            let exists: bool = client_data.get::<usize, bool>(0);

            if !exists {
                if check_cnt >= timeout {
                    anyhow::bail!("Force exit after 30 seconds");
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
