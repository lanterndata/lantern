use std::{
    env,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use lantern_embeddings;
use lantern_embeddings::cli;
use postgres::{Client, NoTls};

fn setup_db_tables(client: &mut Client, table_name: &str) {
    client
        .batch_execute(&format!(
            "
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, content TEXT);
    INSERT INTO {table_name} SELECT generate_series(1,1000), 'Hello world!';
"
        ))
        .expect("Could not create necessarry tables");
}

fn drop_db_tables(client: &mut Client, table_name: &str) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS {table_name};
    "
        ))
        .expect("Could not drop tables");
}

#[test]
fn test_embedding_generation_from_db() {
    let db_url = env::var("DB_URL").expect("`DB_URL` not specified");
    let table_name = String::from("_lantern_embeddings_test");
    let mut db_client = Client::connect(&db_url, NoTls).expect("Database connection failed");
    setup_db_tables(&mut db_client, &table_name);

    let final_progress = Arc::new(AtomicU8::new(0));
    let final_progress_r1 = final_progress.clone();

    let callback = move |progress: u8| {
        final_progress_r1.store(progress, Ordering::SeqCst);
    };

    lantern_embeddings::create_embeddings_from_db(
        cli::EmbeddingArgs {
            model: "BAAI/bge-small-en".to_owned(),
            uri: db_url.clone(),
            column: "content".to_owned(),
            table: table_name.clone(),
            pk: "id".to_owned(),
            schema: "public".to_owned(),
            out_uri: None,
            out_column: "emb".to_owned(),
            batch_size: None,
            visual: false,
            out_csv: None,
            out_table: None,
            limit: None,
            filter: None,
            data_path: Some("/tmp/lantern-embeddings-core-test".to_owned()),
            create_column: true,
            stream: false,
        },
        true,
        Some(Box::new(callback)),
        None,
    )
    .unwrap();

    let cnt = db_client
        .query_one(
            &format!(
            "SELECT COUNT(id) FROM {table_name} WHERE emb IS NULL OR array_length(emb, 1) != 384"
        ),
            &[],
        )
        .unwrap();

    let cnt = cnt.get::<usize, i64>(0);

    drop_db_tables(&mut db_client, &table_name);

    assert_eq!(cnt, 0);
    assert_eq!(final_progress.load(Ordering::SeqCst), 100);
}
