use lantern_cli::{
    daemon::{
        self,
        cli::{DaemonArgs, LogLevel},
    },
    utils::test_utils::daemon_test_utils::{
        setup_test, wait_for_completion, CLIENT_TABLE_NAME, CLIENT_TABLE_NAME_2,
    },
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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

    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
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
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
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
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
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

    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, canceled_at)
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
            "UPDATE _lantern_extras_internal.embedding_generation_jobs SET canceled_at=NULL",
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
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, init_finished_at)
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

#[tokio::test]
async fn test_daemon_embedding_multiple_jobs_listener() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_multiple_jobs_listener")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    CREATE TABLE {CLIENT_TABLE_NAME_2} AS TABLE {CLIENT_TABLE_NAME};
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, init_finished_at)
    VALUES (6, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en', NOW());
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model, init_finished_at)
    VALUES (7, '{CLIENT_TABLE_NAME_2}', 'title', 'title_embedding', 'BAAI/bge-small-en', NOW());

    INSERT INTO {CLIENT_TABLE_NAME} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');

    INSERT INTO {CLIENT_TABLE_NAME_2} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');
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
            r#"
            INSERT INTO {CLIENT_TABLE_NAME} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            INSERT INTO {CLIENT_TABLE_NAME_2} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            "#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=8 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=8 FROM {CLIENT_TABLE_NAME_2} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_multiple_new_jobs_streaming() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_multiple_new_jobs_streaming")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    CREATE TABLE {CLIENT_TABLE_NAME_2} AS TABLE {CLIENT_TABLE_NAME};
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (8, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (9, '{CLIENT_TABLE_NAME_2}', 'title', 'title_embedding', 'BAAI/bge-small-en');

    INSERT INTO {CLIENT_TABLE_NAME} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');

    INSERT INTO {CLIENT_TABLE_NAME_2} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');
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
            r#"
            INSERT INTO {CLIENT_TABLE_NAME} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            INSERT INTO {CLIENT_TABLE_NAME_2} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            "#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=8 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=8 FROM {CLIENT_TABLE_NAME_2} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=2 FROM _lantern_extras_internal.embedding_generation_jobs WHERE id IN (8, 9) AND init_started_at IS NOT NULL AND init_finished_at IS NOT NULL AND init_progress=100"),
        60,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_multiple_new_jobs_with_failure() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_multiple_new_jobs_with_failure")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    CREATE TABLE {CLIENT_TABLE_NAME_2} AS TABLE {CLIENT_TABLE_NAME};
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (10, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (11, '{CLIENT_TABLE_NAME_2}', 'title', 'title_embedding', 'BAAI/bge-small-en2');

    INSERT INTO {CLIENT_TABLE_NAME} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');

    INSERT INTO {CLIENT_TABLE_NAME_2} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');
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
            r#"
            INSERT INTO {CLIENT_TABLE_NAME} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            INSERT INTO {CLIENT_TABLE_NAME_2} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
            "#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=8 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT SUM(tokens)=32 FROM _lantern_extras_internal.embedding_usage_info WHERE job_id=10 AND failed=FALSE GROUP BY job_id"),
        1,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT SUM(rows)=8 FROM _lantern_extras_internal.embedding_usage_info WHERE job_id=10 AND failed=FALSE GROUP BY job_id"),
        1,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=1 FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=11 AND init_failure_reason IS NOT NULL"),
        60,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_jobs_streaming_with_failure() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_jobs_streaming_with_failure")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (10, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');

    INSERT INTO {CLIENT_TABLE_NAME} (id, title)
    VALUES (1, 'Test1'),
           (2, 'Test2'),
           (3, 'Test3'),
           (4, 'Test4'),
           (5, 'Test5');
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
            r#"
            UPDATE _lantern_extras_internal.embedding_generation_jobs SET embedding_model='test';
            INSERT INTO {CLIENT_TABLE_NAME} (id, title) VALUES (6, 'Test6'), (7, 'Test7'), (8, 'Test8');
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

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT SUM(rows)=5 FROM _lantern_extras_internal.embedding_usage_info WHERE job_id=10 AND failed=FALSE GROUP BY job_id"),
        1,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT SUM(tokens)=20 FROM _lantern_extras_internal.embedding_usage_info WHERE job_id=10 AND failed=FALSE GROUP BY job_id"),
        1,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT SUM(rows)=3 FROM _lantern_extras_internal.embedding_usage_info WHERE job_id=10 AND failed=TRUE GROUP BY job_id"),
        20,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_job_labels() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_job_labels").await.unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "label", "table", src_column, dst_column, embedding_model)
    VALUES (13, 'test-label', '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');
     "#
        ))
        .await
        .unwrap();

    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    let connection_uri = new_connection_uri.clone();
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
                target_db: Some(vec![connection_uri]),
                log_level: LogLevel::Debug,
            },
            None,
            cancel_token_clone,
        )
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_secs(5)).await;

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=1 FROM _lantern_extras_internal.embedding_generation_jobs WHERE init_started_at IS NULL"),
        1,
    )
    .await
    .unwrap();

    cancel_token.cancel();

    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    tokio::spawn(async {
        daemon::start(
            DaemonArgs {
                label: Some("test-label".to_owned()),
                master_db: None,
                master_db_schema: String::new(),
                embeddings: true,
                autotune: false,
                external_index: false,
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
        &format!("SELECT COUNT(*)=1 FROM _lantern_extras_internal.embedding_generation_jobs WHERE init_started_at IS NOT NULL AND init_finished_at IS NOT NULL"),
        20,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}

#[tokio::test]
async fn test_daemon_embedding_init_job_streaming_large() {
    let (new_connection_uri, mut new_db_client) =
        setup_test("test_daemon_embedding_init_job_streaming_large")
            .await
            .unwrap();
    new_db_client
        .batch_execute(&format!(
            r#"
    INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("id", "table", src_column, dst_column, embedding_model)
    VALUES (14, '{CLIENT_TABLE_NAME}', 'title', 'title_embedding', 'BAAI/bge-small-en');

    INSERT INTO {CLIENT_TABLE_NAME} (title) SELECT 'Test' ||  n as title FROM generate_series(1, 2000) as n;
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
            r#"
            INSERT INTO {CLIENT_TABLE_NAME} (title) SELECT 'Test' ||  n as title FROM generate_series(2001, 2500) as n;
            "#
        ))
        .await
        .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=1 FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=14 AND init_started_at IS NOT NULL AND init_finished_at IS NOT NULL AND init_progress=100"),
        60,
    )
    .await
    .unwrap();

    wait_for_completion(
        &mut new_db_client,
        &format!("SELECT COUNT(*)=2500 FROM {CLIENT_TABLE_NAME} WHERE title_embedding IS NOT NULL"),
        120,
    )
    .await
    .unwrap();

    cancel_token.cancel();
}
