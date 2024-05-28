use lantern_cli::{
    daemon::{
        self,
        cli::{DaemonArgs, LogLevel},
    },
    utils::test_utils::daemon_test_utils::{setup_test, wait_for_completion},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

static CLIENT_TABLE_NAME: &'static str = "_lantern_cloud_client1";

#[tokio::test]
async fn test_daemon_embedding_init_job() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_init_job").await.unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title)
    VALUES ('Test1'),
           ('Test2'),
           ('Test3'),
           ('Test4'),
           ('Test5');

    INSERT INTO _lantern_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (1, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=5 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_job_client_insert_listener() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_job_client_insert_listener")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (2, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title) 
    VALUES ('Test1'),
           ('Test2'),
           ('Test3'),
           ('Test4'),
           ('Test5');
     "#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=5 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_job_client_update_listener() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_job_client_update_listener")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (3, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');

    INSERT INTO {CLIENT_TABLE_NAME} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test4');
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    new_db_client
        .batch_execute(&format!(
            r#"UPDATE {CLIENT_TABLE_NAME} SET title='updated title' WHERE id=4"#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=5 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        30,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT c1.title_embedding != c2.title_embedding FROM {CLIENT_TABLE_NAME} c1 JOIN {CLIENT_TABLE_NAME} c2 ON c2.id=5 WHERE c1.id=4"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_job_resume() {
    let (new_connection_uri, mut new_db_client) = setup_test("test_daemon_embedding_job_updates")
        .await
        .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title)
    VALUES ('Test1'),
           ('Test2'),
           ('Test3'),
           ('Test4'),
           ('Test5');

    INSERT INTO _lantern_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, canceled_at)
    VALUES (4, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en', NOW());
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    new_db_client
        .execute(
            "UPDATE _lantern_internal.embedding_generation_jobs SET canceled_at=NULL",
            &[],
        )
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=5 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_finished_job_listener() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_finished_job_listener")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, init_finished_at)
    VALUES (5, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en', NOW());
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    new_db_client
        .batch_execute(&format!(
            r#"INSERT INTO {CLIENT_TABLE_NAME} (title) VALUES ('Test6');"#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=1 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

// TODO:: Test failure cases, Test different runtimes, Test usage and failure info tracking
