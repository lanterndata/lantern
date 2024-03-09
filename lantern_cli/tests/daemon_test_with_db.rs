use std::{
    env,
    sync::mpsc::{self, Sender, TryRecvError},
    time::Duration,
};

use lantern_cli::daemon::{
    self,
    cli::{DaemonArgs, LogLevel},
};
use tokio_postgres::{Client, NoTls};

static EMB_LOCK_TABLE_NAME: &'static str = "_lantern_emb_job_locks";
static EMBEDDING_JOBS_TABLE_NAME: &'static str = "_daemon_embedding_jobs";
static AUTOTUNE_JOBS_TABLE_NAME: &'static str = "_daemon_autotune_jobs";
static INDEX_JOBS_TABLE_NAME: &'static str = "_daemon_index_jobs";
static CLIENT_TABLE_NAME: &'static str = "_lantern_cloud_client1";
static AUTOTUNE_RESULTS_TABLE_NAME: &'static str = "_daemon_autotune_results";
static EMBEDDING_USAGE_TABLE_NAME: &'static str = "_daemon_embedding_usage";

async fn setup_db_tables(client: &mut Client) {
    client
        .batch_execute(&format!(
            "
    DROP TABLE IF EXISTS {INDEX_JOBS_TABLE_NAME};
    DROP TABLE IF EXISTS {EMBEDDING_JOBS_TABLE_NAME};
    DROP TABLE IF EXISTS {AUTOTUNE_JOBS_TABLE_NAME};
    DROP TABLE IF EXISTS {AUTOTUNE_RESULTS_TABLE_NAME};
    DROP TABLE IF EXISTS {CLIENT_TABLE_NAME};
    CREATE TABLE {INDEX_JOBS_TABLE_NAME} (
        \"id\" SERIAL PRIMARY KEY,
        \"database_id\" text NOT NULL,
        \"db_connection\" text NOT NULL,
        \"schema\" text NOT NULL,
        \"table\" text NOT NULL,
        \"column\" text NOT NULL,
        \"index\" text,
        \"operator\" text NOT NULL,
        \"efc\" INT NOT NULL,
        \"ef\" INT NOT NULL,
        \"m\" INT NOT NULL,
        \"created_at\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \"updated_at\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \"canceled_at\" timestamp,
        \"started_at\" timestamp,
        \"finished_at\" timestamp,
        \"failed_at\" timestamp,
        \"failure_reason\" text,
        \"progress\" INT2 DEFAULT 0
    );

    CREATE TABLE {AUTOTUNE_JOBS_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        database_id text NOT NULL,
        db_connection text NOT NULL,
        \"schema\" text NOT NULL,
        \"table\" text NOT NULL,
        \"column\" text NOT NULL,
        \"operator\" text NOT NULL,
        target_recall DOUBLE PRECISION NOT NULL,
        embedding_model text NULL,
        k int NOT NULL,
        n int NOT NULL,
        create_index bool NOT NULL,
        created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        canceled_at timestamp,
        started_at timestamp,
        progress INT2 DEFAULT 0,
        finished_at timestamp,
        failed_at timestamp,
        failure_reason text
    );

    CREATE TABLE {EMBEDDING_JOBS_TABLE_NAME} (
        \"id\" SERIAL PRIMARY KEY,
        \"database_id\" text NOT NULL,
        \"db_connection\" text NOT NULL,
        \"schema\" text NOT NULL,
        \"table\" text NOT NULL,
        \"runtime\" text NOT NULL DEFAULT 'ort',
        \"runtime_params\" jsonb,
        \"src_column\" text NOT NULL,
        \"dst_column\" text NOT NULL,
        \"embedding_model\" text NOT NULL,
        \"created_at\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \"updated_at\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \"canceled_at\" timestamp,
        \"init_started_at\" timestamp,
        \"init_finished_at\" timestamp,
        \"init_failed_at\" timestamp,
        \"init_failure_reason\" text,
        \"init_progress\" int2 DEFAULT 0
    );
    CREATE TABLE {CLIENT_TABLE_NAME} (
       id SERIAL PRIMARY KEY,
       title TEXT,
       title_embedding REAL[]
    );

    CREATE TABLE {AUTOTUNE_RESULTS_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        experiment_id INT NOT NULL,
        ef INT NOT NULL,
        efc INT  NOT NULL,
        m INT  NOT NULL,
        recall DOUBLE PRECISION NOT NULL,
        latency DOUBLE PRECISION NOT NULL,
        build_time DOUBLE PRECISION NULL
   );
            
    CREATE TABLE {EMBEDDING_USAGE_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        job_id INT NOT NULL UNIQUE,
        usage INT NOT NULL DEFAULT 0,
        tokens BIGINT NOT NULL DEFAULT 0
   );


    CREATE OR REPLACE FUNCTION public.increment_embedding_usage_and_tokens(v_job_id integer, v_usage integer, v_tokens BIGINT DEFAULT 0)
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
    $function$
"
        ))
        .await
        .expect("Could not create necessarry tables");
}

async fn drop_db_tables(client: &mut Client) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS {INDEX_JOBS_TABLE_NAME};
        DROP TABLE IF EXISTS {EMBEDDING_JOBS_TABLE_NAME};
        DROP TABLE IF EXISTS {AUTOTUNE_JOBS_TABLE_NAME};
        DROP TABLE IF EXISTS {CLIENT_TABLE_NAME};
        DROP TABLE IF EXISTS {AUTOTUNE_RESULTS_TABLE_NAME};
        DROP TABLE IF EXISTS {EMBEDDING_USAGE_TABLE_NAME};
    "
        ))
        .await
        .expect("Could not drop tables");
}

async fn test_setup() {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (mut db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    setup_db_tables(&mut db_client).await;

    db_client
        .batch_execute(&format!(
            "
       INSERT INTO {CLIENT_TABLE_NAME} (title) VALUES 
       ('Test1'),
       ('Test2'),
       ('Test3'),
       ('Test4'),
       ('Test5')
"
        ))
        .await
        .unwrap();
}

fn start_daemon(
    db_uri: String,
    embedding_table: Option<String>,
    autotune_table: Option<String>,
    external_index_table: Option<String>,
) -> Sender<()> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        std::thread::spawn(move || {
            daemon::start(
                DaemonArgs {
                    uri: db_uri,
                    schema: "public".to_owned(),
                    internal_schema: "lantern_test".to_owned(),
                    embedding_table,
                    autotune_table,
                    autotune_results_table: Some(AUTOTUNE_RESULTS_TABLE_NAME.to_owned()),
                    external_index_table,
                    queue_size: 1,
                    log_level: LogLevel::Debug,
                },
                None,
            )
            .expect("Failed to start daemon");
        });

        loop {
            std::thread::sleep(Duration::from_millis(500));
            match rx.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
        }
    });

    return tx;
}

async fn test_embedding_generation_runtime(
    runtime: &str,
    model: &str,
    dimensions: usize,
    token_env_var: &str,
) {
    let mut runtime_params = "{}".to_owned();
    if runtime != "ort" {
        let api_token = env::var(token_env_var);
        let err_msg =
            format!("'{token_env_var}' not provided: skipping {runtime} embedding generation test");

        if api_token.is_err() {
            println!("{}", err_msg);
            return;
        }
        let token = api_token.unwrap();
        let token = token.trim();

        if token == "" {
            println!("{}", err_msg);
            return;
        }
        runtime_params = format!(r#"{{ "api_token": "{token}", "dimensions": {dimensions} }}"#);
    }

    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    db_client
        .batch_execute(&format!(
            "
            DROP SCHEMA IF EXISTS lantern_test CASCADE;
            TRUNCATE TABLE {EMBEDDING_JOBS_TABLE_NAME};
            UPDATE {CLIENT_TABLE_NAME} SET title_embedding=NULL;
         "
        ))
        .await
        .unwrap();
    let db_uri_clone = db_uri.clone();
    let stop_tx = start_daemon(
        db_uri_clone,
        Some(EMBEDDING_JOBS_TABLE_NAME.to_owned()),
        None,
        None,
    );

    // Verify that inserting job with invalid runtime does not crash process
    db_client
        .execute(&format!(
            "
       INSERT INTO {EMBEDDING_JOBS_TABLE_NAME} (database_id, db_connection, \"schema\", \"table\", \"src_column\", \"dst_column\", \"embedding_model\", runtime, runtime_params) 
        VALUES ('client1', $1, 'public', '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', '{model}', 'test_runtime', '{runtime_params}');
"
        ), &[&db_uri])
        .await.unwrap();

    db_client
        .execute(&format!(
            "
       INSERT INTO {EMBEDDING_JOBS_TABLE_NAME} (database_id, db_connection, \"schema\", \"table\", \"src_column\", \"dst_column\", \"embedding_model\", runtime, runtime_params) 
        VALUES ('client1', $1, 'public', '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', '{model}', '{runtime}', '{runtime_params}');
"
        ), &[&db_uri])
        .await.unwrap();

    let mut check_cnt = 0;

    loop {
        let job = db_client.query_one(&format!("SELECT init_progress, init_failed_at, init_failure_reason FROM {EMBEDDING_JOBS_TABLE_NAME} ORDER BY id DESC LIMIT 1"), &[]).await.unwrap();
        let progress: i16 = job.get::<&str, i16>("init_progress");
        let init_failure_reason: Option<String> =
            job.get::<&str, Option<String>>("init_failure_reason");

        if let Some(err) = init_failure_reason {
            eprintln!("{err}");
            assert!(false);
        }

        if progress != 100 {
            if check_cnt >= 30 {
                eprintln!("Force exit after 30 seconds");
                break;
            }
            check_cnt += 1;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        break;
    }
    let client_data = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NULL"),
            &[],
        )
        .await
        .unwrap();
    let cnt: i64 = client_data.get::<usize, i64>(0);
    let client_data = db_client
        .query(
            &format!("SELECT ARRAY_LENGTH(title_embedding, 1) FROM {CLIENT_TABLE_NAME} WHERE ARRAY_LENGTH(title_embedding, 1) != {dimensions}"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(cnt, 0);
    assert_eq!(client_data.len(), 0);
    // Check usage
    let client_data = db_client
        .query_one(
            &format!("SELECT usage FROM {EMBEDDING_USAGE_TABLE_NAME} e WHERE e.job_id=2"),
            &[],
        )
        .await
        .unwrap();
    let usage = client_data.get::<usize, i32>(0);

    assert_eq!(usage, 5);

    // Test embedding inserts
    let mut check_cnt = 0;
    db_client
        .execute(
            &format!("INSERT INTO {CLIENT_TABLE_NAME} (id, title) VALUES (7777, 'Test inesert'), (8888, ''), (9999, ' ')"),
            &[],
        )
        .await
        .unwrap();
    loop {
        let client_data = db_client
            .query_one(
                &format!("SELECT COUNT(*) FROM {CLIENT_TABLE_NAME} WHERE id=7777 AND title_embedding IS NULL"),
                &[],
            )
            .await
            .unwrap();
        let cnt: i64 = client_data.get::<usize, i64>(0);

        if cnt != 0 {
            if check_cnt >= 30 {
                eprintln!("Force exit after 30 seconds");
                assert!(false);
                break;
            }
            check_cnt += 1;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        break;
    }
    let old_embedding = db_client
        .query_one(
            &format!("SELECT title_embedding FROM {CLIENT_TABLE_NAME} WHERE id=7777"),
            &[],
        )
        .await
        .unwrap();
    let old_embedding = old_embedding.get::<usize, Vec<f32>>(0);
    // Check usage
    let client_data = db_client
        .query_one(
            &format!("SELECT usage FROM {EMBEDDING_USAGE_TABLE_NAME} e WHERE e.job_id=2"),
            &[],
        )
        .await
        .unwrap();
    let usage = client_data.get::<usize, i32>(0);

    assert_eq!(usage, 6);
    // Test embedding updates
    let mut check_cnt = 0;
    db_client
        .execute(
            &format!("UPDATE {CLIENT_TABLE_NAME} SET title=NULL, title_embedding=NULL WHERE id in (1, 2)"),
            &[],
        )
        .await
        .unwrap();
    let client_data = db_client
        .query_one(
            &format!("SELECT COUNT(*) FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NULL AND title IS NULL"),
            &[],
        )
        .await
        .unwrap();
    let cnt: i64 = client_data.get::<usize, i64>(0);
    assert_eq!(cnt, 2);
    db_client
        .execute(
            &format!(
                "
                UPDATE {CLIENT_TABLE_NAME} SET title='Test' || id WHERE id in (1, 2, 7777);
            "
            ),
            &[],
        )
        .await
        .unwrap();

    loop {
        let client_data = db_client
            .query_one(
                &format!("SELECT COUNT(*) FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NULL"),
                &[],
            )
            .await
            .unwrap();
        let cnt: i64 = client_data.get::<usize, i64>(0);

        if cnt != 0 {
            if check_cnt >= 30 {
                eprintln!("Force exit after 30 seconds");
                break;
            }
            check_cnt += 1;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        break;
    }
    let client_data = db_client
        .query(
            &format!("SELECT * FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NULL OR ARRAY_LENGTH(title_embedding, 1) != {dimensions}"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(client_data.len(), 2);

    db_client
        .execute(
            &format!("DELETE FROM {CLIENT_TABLE_NAME} WHERE id in (8888, 9999)"),
            &[],
        )
        .await
        .unwrap();

    let updated_embedding = db_client
        .query_one(
            &format!("SELECT title_embedding, title FROM {CLIENT_TABLE_NAME} WHERE id=7777"),
            &[],
        )
        .await
        .unwrap();
    let updated_embedding = updated_embedding.get::<usize, Vec<f32>>(0);
    assert_ne!(old_embedding, updated_embedding);

    // Check usage
    let client_data = db_client
        .query_one(
            &format!("SELECT usage FROM {EMBEDDING_USAGE_TABLE_NAME} e WHERE e.job_id=2"),
            &[],
        )
        .await
        .unwrap();
    let usage = client_data.get::<usize, i32>(0);

    assert_eq!(usage, 9);
    // Check that all row locks are removed
    let locks = db_client
        .query(
            &format!("SELECT * FROM lantern_test.{EMB_LOCK_TABLE_NAME}"),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(locks.len(), 0);
    stop_tx.send(()).unwrap();
}

async fn test_index_creation() {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    let db_uri_clone = db_uri.clone();
    let stop_tx = start_daemon(
        db_uri_clone,
        None,
        None,
        Some(INDEX_JOBS_TABLE_NAME.to_owned()),
    );
    // Verify that NULL values do not crash index creation
    db_client
        .execute(
            &format!("UPDATE {CLIENT_TABLE_NAME} SET title_embedding=NULL WHERE id=1;"),
            &[],
        )
        .await
        .unwrap();
    db_client
        .execute(&format!(
            "
       INSERT INTO {INDEX_JOBS_TABLE_NAME} (database_id, db_connection, \"schema\", \"table\", \"column\", \"operator\", m, ef, efc, \"index\") 
        VALUES ('client1', $1, 'public', '{CLIENT_TABLE_NAME}', 'title_embedding', 'dist_l2sq_ops', 12, 64, 64, 'daemon_idx');
"
        ), &[&db_uri])
        .await.unwrap();

    let mut check_cnt = 0;

    loop {
        let job = db_client.query_one(&format!("SELECT progress, failed_at, failure_reason FROM {INDEX_JOBS_TABLE_NAME} LIMIT 1"), &[]).await.unwrap();
        let progress: i16 = job.get::<&str, i16>("progress");
        let init_failure_reason: Option<String> = job.get::<&str, Option<String>>("failure_reason");

        if let Some(err) = init_failure_reason {
            eprintln!("{err}");
            assert!(false);
        }

        if progress != 100 {
            if check_cnt >= 30 {
                eprintln!("Force exit after 30 seconds");
                break;
            }
            check_cnt += 1;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        break;
    }
    db_client
        .query_one(
            &format!("SELECT _lantern_internal.validate_index('daemon_idx', false)"),
            &[],
        )
        .await
        .unwrap();
    stop_tx.send(()).unwrap();
}

async fn test_index_autotune() {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    let db_uri_clone = db_uri.clone();
    let stop_tx = start_daemon(
        db_uri_clone,
        None,
        Some(AUTOTUNE_JOBS_TABLE_NAME.to_owned()),
        None,
    );
    db_client
        .execute(&format!(
            "
       INSERT INTO {AUTOTUNE_JOBS_TABLE_NAME} (database_id, db_connection, \"schema\", \"table\", \"column\", \"operator\", target_recall, k, n, create_index, created_at, updated_at) 
        VALUES ('client1', $1, 'public', '{CLIENT_TABLE_NAME}', 'title_embedding', 'dist_l2sq_ops', 98, 10, 100000, true, NOW(), NOW());
"
        ), &[&db_uri])
        .await.unwrap();

    let mut check_cnt = 0;

    loop {
        let job = db_client.query_one(&format!("SELECT progress, failed_at, failure_reason FROM {AUTOTUNE_JOBS_TABLE_NAME} LIMIT 1"), &[]).await.unwrap();
        let progress: i16 = job.get::<&str, i16>("progress");
        let init_failure_reason: Option<String> = job.get::<&str, Option<String>>("failure_reason");

        if let Some(err) = init_failure_reason {
            eprintln!("{err}");
            assert!(false);
        }

        if progress != 100 {
            if check_cnt >= 30 {
                eprintln!("Force exit after 30 seconds");
                break;
            }
            check_cnt += 1;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        break;
    }
    db_client
        .query_one(
            &format!("SELECT _lantern_internal.validate_index('{CLIENT_TABLE_NAME}_title_embedding_idx', false)"),
            &[],
        )
        .await
        .unwrap();
    stop_tx.send(()).unwrap();
}

async fn test_cleanup() {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (mut db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    drop_db_tables(&mut db_client).await;
}

#[tokio::test]
async fn test_daemon() {
    let runtimes = [
        (
            "openai",
            "openai/text-embedding-ada-002",
            1536,
            "OPENAI_TOKEN",
        ),
        (
            "openai",
            "openai/text-embedding-3-small",
            768,
            "OPENAI_TOKEN",
        ),
        (
            "openai",
            "openai/text-embedding-3-large",
            3072,
            "OPENAI_TOKEN",
        ),
        (
            "cohere",
            "cohere/embed-multilingual-v2.0",
            768,
            "COHERE_TOKEN",
        ),
        ("ort", "microsoft/all-MiniLM-L12-v2", 384, ""),
    ];

    for (runtime, model, dim, params) in runtimes {
        test_cleanup().await;
        test_setup().await;
        test_embedding_generation_runtime(runtime, model, dim, params).await;
    }
    test_index_creation().await;
    test_index_autotune().await;
    test_cleanup().await;
}
