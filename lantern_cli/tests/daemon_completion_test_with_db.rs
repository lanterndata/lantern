use lantern_cli::{
    daemon::{
        self,
        cli::{DaemonArgs, LogLevel},
    },
    utils::test_utils::daemon_test_utils::{setup_test, wait_for_completion, CLIENT_TABLE_NAME},
};
use std::env;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_daemon_completion_init_job() {
    let api_token = env::var("OPENAI_TOKEN").unwrap_or("".to_owned());
    if api_token == "" {
        return;
    }

    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_completion_init_job").await.unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO {CLIENT_TABLE_NAME} (title)
    VALUES ('Test1'),
           ('Test2'),
           ('Test3'),
           ('Test4'),
           ('Test5');

    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, runtime, runtime_params, job_type, column_type)
    VALUES (1, '{CLIENT_TABLE_NAME}', 'title', 'num', 'gpt-4o', 'openai', '{{"api_token": "{api_token}", "context": "Given text testN, return the N as number without any quotes, so for Test1 you should return 1, Test105 you should return 105" }}', 'completion', 'INT');
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
                embeddings: true,
                autotune: false,
                external_index: false,
                databases_table: String::new(),
                schema: "_lantern_extras_internal".to_owned(),
                target_db: Some(vec![new_connection_uri]),
                log_level: LogLevel::Debug,
                data_path: None,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=5 FROM {CLIENT_TABLE_NAME} WHERE num = substring(title, 5)::INT"),
        30,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}
