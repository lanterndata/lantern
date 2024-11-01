use std::{
    env,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use lantern_cli::embeddings::{self, cli::EmbeddingJobType};
use lantern_cli::embeddings::{core::Runtime, get_try_cast_fn_sql};
use lantern_cli::{daemon::embedding_jobs::FAILURE_TABLE_DEFINITION, embeddings::cli};
use tokio_postgres::IsolationLevel;
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

async fn setup_db_tables(client: &mut Client, table_name: &str) {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(false)
        .start()
        .await
        .unwrap();
    transaction
        .batch_execute(&format!(
            "
    SELECT pg_advisory_xact_lock(3337);
    DROP TABLE IF EXISTS {table_name};
    DROP TABLE IF EXISTS {table_name}_failure_info;
    CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, content TEXT);
    CREATE TABLE {table_name}_failure_info ({FAILURE_TABLE_DEFINITION});
    INSERT INTO {table_name} SELECT generate_series(1,4000), 'Hello world!';
    {try_cast_fn}
",
            try_cast_fn = get_try_cast_fn_sql("public")
        ))
        .await
        .expect("Could not create necessarry tables");
    transaction.commit().await.unwrap();
}

async fn drop_db_tables(client: &mut Client, table_name: &str) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS {table_name};
        DROP TABLE IF EXISTS {table_name}_failure_info;
    "
        ))
        .await
        .expect("Could not drop tables");
}

#[tokio::test]
async fn test_embedding_generation_from_db() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let table_name = String::from("_embeddings_test");
    let (mut db_client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Can not connect to database");
    tokio::spawn(async move { connection.await.unwrap() });
    setup_db_tables(&mut db_client, &table_name).await;

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    let (processed_rows, processed_tokens) = embeddings::create_embeddings_from_db(
        cli::EmbeddingArgs {
            model: "BAAI/bge-small-en".to_owned(),
            uri: db_url.clone(),
            pk: "id".to_owned(),
            column: "content".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            out_uri: None,
            out_column: "emb".to_owned(),
            batch_size: None,
            visual: false,
            out_table: None,
            limit: None,
            filter: None,
            runtime: Runtime::Ort,
            runtime_params: "{\"data_path\": \"/tmp/lantern-embeddings-core-test\"}".to_owned(),
            create_column: true,
            stream: true,
            job_type: None,
            column_type: None,
            check_column_type: false,
            create_cast_fn: false,
            internal_schema: "".to_owned(),
            failed_rows_table: None,
            job_id: 0,
        },
        true,
        Some(Box::new(callback)),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(processed_rows, 4000);
    assert_eq!(processed_tokens, 20000);

    let cnt = db_client
        .query_one(
            &format!(
            "SELECT COUNT(id) FROM {table_name} WHERE emb IS NULL OR array_length(emb, 1) != 384"
        ),
            &[],
        )
        .await
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    drop_db_tables(&mut db_client, &table_name).await;

    assert_eq!(cnt, 0);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_openai_completion_from_db() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let api_token = env::var("OPENAI_TOKEN").unwrap_or("".to_owned());

    if api_token == "" {
        return;
    }

    let table_name = String::from("_completion_test_openai");
    let failure_table_name = format!("{table_name}_failure_info");
    let (mut db_client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Can not connect to database");
    tokio::spawn(async move { connection.await.unwrap() });
    setup_db_tables(&mut db_client, &table_name).await;

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    let (processed_rows, _) = embeddings::create_embeddings_from_db(
        cli::EmbeddingArgs {
            model: "gpt-4o".to_owned(),
            uri: db_url.clone(),
            pk: "id".to_owned(),
            column: "content".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            out_uri: None,
            out_column: "chars".to_owned(),
            batch_size: None,
            visual: false,
            out_table: None,
            limit: Some(10),
            filter: None,
            runtime: Runtime::OpenAi,
            runtime_params: format!(r#"{{"api_token": "{api_token}", "system_prompt": "you will be given text, return postgres array of TEXT[] by splitting the text by characters skipping spaces. Example 'te st' -> {{t,e,s,t}} . Do not put tailing commas, do not put double or single quotes around characters" }}"#),
            create_column: true,
            stream: true,
            job_type: Some(EmbeddingJobType::Completion),
            column_type: Some("TEXT[]".to_owned()),
            failed_rows_table: Some(failure_table_name.clone()),
            internal_schema: "public".to_owned(),
            create_cast_fn: false,
            check_column_type: true,
            job_id: 0
        },
        true,
        Some(Box::new(callback)),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(processed_rows, 10);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(id) FROM {table_name} WHERE id < 11 AND chars != '{{H,e,l,l,o,w,o,r,l,d,!}}'"
            ),
            &[],
        )
        .await
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    drop_db_tables(&mut db_client, &table_name).await;

    assert_eq!(cnt, 0);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_openai_completion_special_chars_from_db() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let api_token = env::var("OPENAI_TOKEN").unwrap_or("".to_owned());

    if api_token == "" {
        return;
    }

    let table_name = String::from("_completion_test_openai_special_chars");
    let failure_table_name = format!("{table_name}_failure_info");
    let (mut db_client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Can not connect to database");
    tokio::spawn(async move { connection.await.unwrap() });
    setup_db_tables(&mut db_client, &table_name).await;

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    let (processed_rows, _) = embeddings::create_embeddings_from_db(
        cli::EmbeddingArgs {
            model: "gpt-4o".to_owned(),
            uri: db_url.clone(),
            pk: "id".to_owned(),
            column: "content".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            out_uri: None,
            out_column: "chars".to_owned(),
            batch_size: None,
            visual: false,
            out_table: None,
            limit: Some(2),
            filter: None,
            runtime: Runtime::OpenAi,
            runtime_params: format!(r#"{{"api_token": "{api_token}", "system_prompt": "for any input return multi line text which will contain escape characters which can potentially break postgres COPY" }}"#),
            create_column: true,
            stream: true,
            job_type: Some(EmbeddingJobType::Completion),
            column_type: Some("TEXT".to_owned()),
            failed_rows_table: Some(failure_table_name.clone()),
            internal_schema: "public".to_owned(),
            create_cast_fn: false,
            check_column_type: true,
            job_id: 0
        },
        true,
        Some(Box::new(callback)),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(processed_rows, 2);

    let cnt = db_client
        .query_one(
            &format!(
                "SELECT COUNT(id) FROM {table_name} WHERE id < 3 AND chars is not null AND chars not ilike 'error%'"
            ),
            &[],
        )
        .await
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    drop_db_tables(&mut db_client, &table_name).await;

    assert_eq!(cnt, 2);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_openai_completion_failed_rows_from_db() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let api_token = env::var("OPENAI_TOKEN").unwrap_or("".to_owned());

    if api_token == "" {
        return;
    }

    let table_name = String::from("_completion_test_openai_failed_rows");
    let failure_table_name = format!("{table_name}_failure_info");
    let (mut db_client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Can not connect to database");
    tokio::spawn(async move { connection.await.unwrap() });
    setup_db_tables(&mut db_client, &table_name).await;

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    let (processed_rows, _) = embeddings::create_embeddings_from_db(
        cli::EmbeddingArgs {
            model: "gpt-4o".to_owned(),
            uri: db_url.clone(),
            pk: "id".to_owned(),
            column: "content".to_owned(),
            table: table_name.clone(),
            schema: "public".to_owned(),
            out_uri: None,
            out_column: "chars".to_owned(),
            batch_size: None,
            visual: false,
            out_table: None,
            limit: Some(10),
            filter: None,
            runtime: Runtime::OpenAi,
            runtime_params: format!(r#"{{"api_token": "{api_token}", "system_prompt": "you will be given text, return array by splitting the text by characters skipping spaces. Example 'te st' -> [t,e,s,t]" }}"#),
            create_column: true,
            stream: true,
            job_type: Some(EmbeddingJobType::Completion),
            column_type: Some("TEXT[]".to_owned()),
            failed_rows_table: Some(failure_table_name.clone()),
            internal_schema: "public".to_owned(),
            create_cast_fn: false,
            check_column_type: true,
            job_id: 0
        },
        true,
        Some(Box::new(callback)),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(processed_rows, 10);

    let cnt = db_client
        .query_one(&format!("SELECT COUNT(id) FROM {failure_table_name}"), &[])
        .await
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    drop_db_tables(&mut db_client, &table_name).await;

    assert_eq!(cnt, 10);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);
}
