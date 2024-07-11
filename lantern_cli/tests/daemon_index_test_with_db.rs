use lantern_cli::{
    daemon::{
        self,
        cli::{DaemonArgs, LogLevel},
    },
    utils::test_utils::daemon_test_utils::{setup_test, wait_for_completion},
};
use tokio_util::sync::CancellationToken;

static CLIENT_TABLE_NAME: &'static str = "_lantern_cloud_client1";

#[tokio::test]
async fn test_daemon_external_index_create_on_small_table() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_external_index_create_on_small_table")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title, title_embedding)
    VALUES ('Test1', '{{0,0,0}}'),
           ('Test2', '{{0,0,1}}'),
           ('Test5', '{{0,0,4}}');

    INSERT INTO _lantern_extras_internal.external_index_jobs ("id", "table", "column", "operator", "index", efc, ef, m)
    VALUES (1, '{CLIENT_TABLE_NAME}', 'title_embedding', 'dist_cos_ops', 'test_idx1', 32, 32, 10);
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                label: None,
                master_db: None,
                master_db_schema: String::new(),
                embeddings: false,
                autotune: false,
                external_index: true,
                databases_table: String::new(),
                schema: "_lantern_extras_internal".to_owned(),
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
        &format!("SELECT COUNT(*)=1 FROM pg_indexes WHERE indexname='test_idx1'"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}
#[tokio::test]
async fn test_daemon_external_index_create() {
    let (new_connection_uri, mut new_db_client) = setup_test("test_daemon_external_index_create")
        .await
        .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title, title_embedding)
    VALUES ('Test1', '{{0,0,0}}'),
           ('Test2', '{{0,0,1}}'),
           ('Test3', '{{0,0,2}}'),
           ('Test4', '{{0,0,3}}'),
           ('Test5', '{{0,0,4}}');

    INSERT INTO _lantern_extras_internal.external_index_jobs ("id", "table", "column", "operator", "index", efc, ef, m)
    VALUES (2, '{CLIENT_TABLE_NAME}', 'title_embedding', 'dist_cos_ops', 'test_idx1', 32, 32, 10);
     "#
        ))
        .await
        .unwrap();
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                label: None,
                master_db: None,
                master_db_schema: String::new(),
                embeddings: false,
                autotune: false,
                external_index: true,
                databases_table: String::new(),
                schema: "_lantern_extras_internal".to_owned(),
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
        &format!("SELECT COUNT(*)=1 FROM pg_indexes WHERE indexname='test_idx1'"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

// TODO:: Test failure cases
