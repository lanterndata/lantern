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
async fn test_daemon_autotune_with_create_index() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_autotune_with_create_index")
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

    INSERT INTO _lantern_internal.autotune_jobs ("id", "table", "column", "operator", target_recall, k, n, create_index)
    VALUES (1, '{CLIENT_TABLE_NAME}', 'title_embedding', 'dist_cos_ops', 95.0, 10, 1000, true);
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
                embeddings: false,
                autotune: true,
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
        &format!("SELECT COUNT(*)=1 FROM pg_indexes WHERE indexdef LIKE '%lantern_hnsw%'"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

// TODO:: Test failure cases, Validate target recall
