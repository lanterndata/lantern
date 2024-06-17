use std::{
    env,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use lantern_cli::embeddings;
use lantern_cli::embeddings::cli;
use lantern_cli::embeddings::core::Runtime;
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

async fn setup_db_tables(client: &mut Client, table_name: &str) {
    client
        .batch_execute(&format!(
            "
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, content TEXT);
    INSERT INTO {table_name} SELECT generate_series(1,4000), 'Hello world!';
"
        ))
        .await
        .expect("Could not create necessarry tables");
}

async fn drop_db_tables(client: &mut Client, table_name: &str) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS {table_name};
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
            out_csv: None,
            out_table: None,
            limit: None,
            filter: None,
            runtime: Runtime::Ort,
            runtime_params: "{\"data_path\": \"/tmp/lantern-embeddings-core-test\"}".to_owned(),
            create_column: true,
            stream: true,
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
