use std::{
    collections::HashMap,
    env,
    sync::mpsc::{self, Sender, TryRecvError},
    time::Duration,
};

use actix_web::http::StatusCode;
use isahc::{ReadResponseExt, Request};
use lantern_cli::{
    http_server::{self, cli::HttpServerArgs},
    types::AnyhowVoidResult,
};
use tokio_postgres::{Client, NoTls};

static TEST_COLLECTION_NAME: &'static str = "_lantern_http_test1";
static SERVER_URL: &'static str = "http://127.0.0.1:7777";

async fn drop_db_tables(client: &mut Client) {
    client
        .batch_execute(&format!(
            "
        DROP TABLE IF EXISTS {TEST_COLLECTION_NAME};
        DROP TABLE IF EXISTS _lantern_internal.http_collections;
        DROP TABLE IF EXISTS _lantern_internal.pq_{TEST_COLLECTION_NAME}_v;
    "
        ))
        .await
        .expect("Could not drop tables");
}

async fn test_setup() -> Sender<()> {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    start_server(db_uri)
}

fn start_server(db_uri: String) -> Sender<()> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        std::thread::spawn(move || {
            http_server::start(
                HttpServerArgs {
                    db_uri,
                    remote_database: true,
                    host: "127.0.0.1".to_owned(),
                    port: 7777,
                },
                None,
            )
            .expect("Failed to start HTTP Server");
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

    std::thread::sleep(Duration::from_secs(3));
    return tx;
}

async fn test_cleanup() {
    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (mut db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    drop_db_tables(&mut db_client).await;
}

async fn test_collection_create() -> AnyhowVoidResult {
    let body = format!(
        r#"{{
                 "name": "{TEST_COLLECTION_NAME}",
                 "schema": {{ "id": "serial primary key", "v": "REAL[]", "m": "JSONB" }}
             }}"#
    );
    let request = Request::post(&format!("{SERVER_URL}/collections"))
        .header("content-type", "application/json")
        .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;

    let mut body: Vec<u8> = Vec::with_capacity(body.capacity());
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;
    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, serde_json::Value> = serde_json::from_str(&body_json)?;
    assert_eq!(body_json.get("name").unwrap(), TEST_COLLECTION_NAME);

    Ok(())
}

async fn test_collection_insert() -> AnyhowVoidResult {
    let body = format!(
        r#"{{
               "rows": [
                    {{ "v": [0,0,1], "m": {{ "name": "test1" }} }},
                    {{ "v": [0,1,1], "m": {{ "name": "test2" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [0,0,0], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }},
                    {{ "v": [1,1,1], "m": {{ "name": "test3" }} }}
                ]
             }}"#
    );
    let request = Request::put(&format!("{SERVER_URL}/collections/{TEST_COLLECTION_NAME}"))
        .header("content-type", "application/json")
        .body(body.as_bytes())?;

    let response = isahc::send(request)?;
    assert_eq!(response.status(), StatusCode::from_u16(200)?);

    Ok(())
}

async fn test_collection_list() -> AnyhowVoidResult {
    let mut response = isahc::get(&format!("{SERVER_URL}/collections"))?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;
    println!("Response: {:?}", body_json);
    let body_json: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(&body_json)?;

    assert_eq!(body_json.len(), 1);
    assert_eq!(
        body_json.first().unwrap().get("name").unwrap(),
        TEST_COLLECTION_NAME
    );

    Ok(())
}

async fn test_collection_get() -> AnyhowVoidResult {
    let mut response = isahc::get(&format!("{SERVER_URL}/collections/{TEST_COLLECTION_NAME}"))?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;
    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, serde_json::Value> = serde_json::from_str(&body_json)?;

    assert_eq!(body_json.get("name").unwrap(), TEST_COLLECTION_NAME);
    assert_eq!(body_json.get("schema").unwrap().get("v").unwrap(), "ARRAY");
    assert_eq!(
        body_json.get("schema").unwrap().get("id").unwrap(),
        "integer"
    );
    assert_eq!(body_json.get("schema").unwrap().get("m").unwrap(), "jsonb");

    Ok(())
}

async fn test_collection_delete() -> AnyhowVoidResult {
    isahc::delete(&format!("{SERVER_URL}/collections/{TEST_COLLECTION_NAME}"))?;

    let mut response = isahc::get(&format!("{SERVER_URL}/collections"))?;
    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;
    let body_json: Vec<HashMap<String, String>> = serde_json::from_str(&body_json)?;

    assert_eq!(body_json.len(), 0);
    Ok(())
}

async fn test_pq() -> AnyhowVoidResult {
    let body = format!(
        r#"{{
               "clusters": 10,
               "column": "v",
               "splits": 1
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/pq"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;
    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    assert_eq!(response.status(), StatusCode::from_u16(200)?);

    Ok(())
}

async fn test_index_create() -> AnyhowVoidResult {
    let body = format!(
        r#"{{
               "name": "test_idx",
               "metric": "cosine",
               "column": "v",
               "pq": true
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/index"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let response = isahc::send(request)?;
    let mut body: Vec<u8> = Vec::new();
    let body_json = String::from_utf8(body)?;
    println!("Response: {:?}", body_json);
    assert_eq!(response.status(), StatusCode::from_u16(200)?);

    let mut response = isahc::get(&format!("{SERVER_URL}/collections/{TEST_COLLECTION_NAME}"))?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;
    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, serde_json::Value> = serde_json::from_str(&body_json)?;
    let indexes: Vec<HashMap<String, String>> =
        serde_json::from_value(body_json.get("indexes").unwrap().clone())?;

    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].get("name").unwrap(), "test_idx");
    assert_eq!(indexes[0].get("m").unwrap(), "16");
    assert_eq!(indexes[0].get("ef_construction").unwrap(), "128");
    assert_eq!(indexes[0].get("ef").unwrap(), "64");
    assert_eq!(indexes[0].get("dim").unwrap(), "3");
    assert_eq!(indexes[0].get("metric").unwrap(), "cos");
    Ok(())
}

async fn test_search_vector() -> AnyhowVoidResult {
    let body = format!(
        r#"{{
                 "column": "v",
                 "query_vector": [0,0,0],
                 "k": 1,
                 "select": "id"
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/search"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&body_json)?;

    let rows = body_json.get("rows").unwrap();
    assert_eq!(rows.len(), 1);
    let first = rows.first().unwrap();

    assert_eq!(first["id"], 6);
    assert_eq!(first["distance"], 0);

    // Test wih k=5
    let body = format!(
        r#"{{
                 "column": "v",
                 "query_vector": [0,0,0],
                 "k": 5,
                 "select": "id, v"
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/search"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&body_json)?;

    let rows = body_json.get("rows").unwrap();
    assert_eq!(rows.len(), 5);
    let first = rows.first().unwrap();

    assert_eq!(first["id"], 6);
    assert_eq!(first["v"], serde_json::to_value(vec![0, 0, 0])?);
    assert_eq!(first["distance"], 0);

    let db_uri = env::var("DB_URL").expect("`DB_URL` not specified");
    let (db_client, connection) = tokio_postgres::connect(&db_uri, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db_client.batch_execute(&format!("
        DROP TABLE {TEST_COLLECTION_NAME} CASCADE;
        CREATE TABLE {TEST_COLLECTION_NAME} (id serial primary key, v real[]);
        INSERT INTO {TEST_COLLECTION_NAME} (v) VALUES (text_embedding('BAAI/bge-small-en', 'Weather is nice today')), (text_embedding('BAAI/bge-small-en', 'The car is red'));
        CREATE INDEX test_idx ON {TEST_COLLECTION_NAME} USING lantern_hnsw (v) WITH (m=16, ef_construction=128, ef=128);
    ")).await?;
    // Test wih model
    let body = format!(
        r#"{{
                 "column": "v",
                 "query_text": "How is the weather today?",
                 "query_model": "BAAI/bge-small-en",
                 "k": 2,
                 "select": "id"
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/search"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&body_json)?;

    let rows = body_json.get("rows").unwrap();
    assert_eq!(rows.len(), 2);
    let first = rows.first().unwrap();

    assert_eq!(first["id"], 1);

    let body = format!(
        r#"{{
                 "column": "v",
                 "query_text": "What color is the car?",
                 "query_model": "BAAI/bge-small-en",
                 "k": 2,
                 "select": "id"
             }}"#
    );
    let request = Request::post(&format!(
        "{SERVER_URL}/collections/{TEST_COLLECTION_NAME}/search"
    ))
    .header("content-type", "application/json")
    .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;

    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    let body_json: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&body_json)?;

    let rows = body_json.get("rows").unwrap();
    assert_eq!(rows.len(), 2);
    let first = rows.first().unwrap();

    assert_eq!(first["id"], 2);
    Ok(())
}

async fn test_index_delete() -> AnyhowVoidResult {
    let body = String::new();
    let request = Request::delete(&format!("{SERVER_URL}/index/test_idx"))
        .header("content-type", "application/json")
        .body(body.as_bytes())?;

    let mut response = isahc::send(request)?;
    let mut body: Vec<u8> = Vec::new();
    response.copy_to(&mut body)?;
    let body_json = String::from_utf8(body)?;

    println!("Response: {:?}", body_json);
    assert_eq!(response.status(), StatusCode::from_u16(200)?);

    Ok(())
}

#[tokio::test]
async fn test_http_server() {
    test_cleanup().await;
    let tx = test_setup().await;
    test_collection_create().await.unwrap();
    test_collection_list().await.unwrap();
    test_collection_get().await.unwrap();
    test_collection_insert().await.unwrap();
    test_pq().await.unwrap();
    test_index_create().await.unwrap();
    test_search_vector().await.unwrap();
    test_index_delete().await.unwrap();
    test_collection_delete().await.unwrap();
    tx.send(()).unwrap();
    test_cleanup().await;
}
