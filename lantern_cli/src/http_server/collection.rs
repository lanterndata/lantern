use bytes::BytesMut;
use futures::SinkExt;
use itertools::Itertools;
use regex::Regex;
use std::collections::HashMap;

use actix_web::{
    delete,
    error::{ErrorBadRequest, ErrorInternalServerError, ErrorNotFound},
    get,
    http::StatusCode,
    post, put, web, HttpResponse, Responder, Result,
};

use crate::{external_index::cli::UMetricKind, utils::quote_ident};

use super::{AppState, COLLECTION_TABLE_NAME};
use serde::{Deserialize, Serialize};

fn parse_index_def(definition: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for key in [
        "ef_construction",
        "ef",
        "m",
        "dim",
        "pq",
        "_experimental_index_path",
    ]
    .iter()
    {
        let regex = Regex::new(&format!(r"[\(,\s]{}='(.*?)'", key)).unwrap();
        if let Some(match_) = regex.captures(&definition) {
            if *key == "_experimental_index_path" {
                result.insert("external".to_string(), "true".to_string());
            } else {
                result.insert(key.to_string(), match_[1].to_string());
            }
        }
    }

    let mut metric = "l2sq".to_string();
    let operator_match = Regex::new(r"hnsw\s*\(.*?\s+(\w+)\s*\)")
        .unwrap()
        .captures(&definition);

    if let Some(operator_class) = operator_match {
        let _match = operator_class[1].to_string();
        let umetric = UMetricKind::from_ops(&_match).unwrap();
        metric = umetric.to_string();
    }

    result.insert("metric".to_string(), metric);

    return result;
}

fn parse_indexes(index_definitions: Vec<HashMap<String, String>>) -> Vec<HashMap<String, String>> {
    let mut result = Vec::with_capacity(index_definitions.len());
    for index_info in &index_definitions {
        let mut parsed_info = parse_index_def(index_info.get("definition").unwrap());
        parsed_info.insert(
            "name".to_owned(),
            index_info.get("name").unwrap().to_string(),
        );
        result.push(parsed_info);
    }

    result
}

fn get_collection_query(filter: &str) -> String {
    format!("SELECT b.name, b.schema, COALESCE(json_agg(json_build_object('name', i.indexname , 'definition', i.indexdef)) FILTER (WHERE i.indexname IS NOT NULL), '[]')::text as indexes FROM (SELECT c.name, json_object_agg(t.column_name, t.data_type)::text as schema FROM {COLLECTION_TABLE_NAME} c INNER JOIN information_schema.columns t ON t.table_name=c.name  {filter} GROUP BY c.name) b LEFT JOIN pg_indexes i ON i.tablename=b.name AND i.indexdef ILIKE '%USING lantern_hnsw%' GROUP BY b.name, b.schema")
}

#[derive(Serialize, Debug, utoipa::ToSchema)]
pub struct CollectionInfo {
    name: String,
    schema: HashMap<String, String>,
    indexes: Vec<HashMap<String, String>>,
}
/// Get all collections
#[utoipa::path(
    get,
    path = "/collections",
    responses(
        (status = 200, description = "Returns tables Created via Lantern HTTP API", body = Vec<CollectionInfo>),
        (status = 500, description = "Internal Server Error")
    )
)]
#[get("/collections")]
pub async fn list(data: web::Data<AppState>) -> Result<impl Responder> {
    let client = data.pool.get().await?;
    let rows = client
        .query(&get_collection_query(""), &[])
        .await
        .map_err(ErrorInternalServerError)?;

    let tables: Vec<CollectionInfo> = rows
        .iter()
        .map(|r| CollectionInfo {
            name: r.get::<usize, String>(0),
            schema: serde_json::from_str(r.get::<usize, &str>(1)).unwrap(),
            indexes: parse_indexes(serde_json::from_str(r.get::<usize, &str>(2)).unwrap()),
        })
        .collect();

    Ok(web::Json(tables))
}

/// Get collection by name
#[utoipa::path(
    get,
    path = "/collections/{name}",
    responses(
        (status = 200, description = "Returns the collection data", body = CollectionInfo),
        (status = 500, description = "Internal Server Error")
    ),
    params(
       ("name", description = "Collection name")
    ),
)]
#[get("/collections/{name}")]
pub async fn get(data: web::Data<AppState>, name: web::Path<String>) -> Result<impl Responder> {
    let client = data.pool.get().await?;
    let rows = client
        .query(
            &get_collection_query("WHERE c.name=$1"),
            &[&name.to_string()],
        )
        .await
        .map_err(ErrorInternalServerError)?;

    if rows.is_empty() {
        return Err(ErrorNotFound("Collection not found"));
    }

    let first_row = rows.first().unwrap();

    let table: CollectionInfo = CollectionInfo {
        name: first_row.get::<usize, String>(0),
        schema: serde_json::from_str(first_row.get::<usize, &str>(1)).unwrap(),
        indexes: parse_indexes(serde_json::from_str(first_row.get::<usize, &str>(2)).unwrap()),
    };

    Ok(web::Json(table))
}

#[derive(Deserialize, Debug, Clone, utoipa::ToSchema)]
pub struct CreateTableInput {
    name: String,
    schema: Option<HashMap<String, String>>,
}

/// Create new table with specified schema
///
/// If schema is empty the default schema will be used
/// ```no_run
/// (bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, vector REAL[], data TEXT, metadata JSONB)
/// ```
///
/// schema should be specified like this:
/// ```no_run
/// { "id": "serial primary key", "v": "REAL[]", "t": "TEXT" }
/// ```
#[utoipa::path(
    post,
    path = "/collections",
    request_body (
        content = CreateTableInput,
        example = json!(r#"{ "name": "my_test_collection", "schema": {"id": "bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", "vector": "REAL[]", "data": "TEXT", "metadata": "JSONB" } }"#)
    ),
    responses(
        (status = 200, description = "Returns the created table", body = CollectionInfo),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal Server Error")
    )
)]
#[post("/collections")]
pub async fn create(
    data: web::Data<AppState>,
    body: web::Json<CreateTableInput>,
) -> Result<impl Responder> {
    let mut client = data.pool.get().await.unwrap();
    let transaction = client
        .transaction()
        .await
        .map_err(ErrorInternalServerError)?;

    let default_schema = HashMap::from([
        (
            "id".to_owned(),
            "bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY".to_owned(),
        ),
        ("vector".to_owned(), "REAL[]".to_owned()),
        ("data".to_owned(), "TEXT".to_owned()),
        ("metadata".to_owned(), "JSONB".to_owned()),
    ]);

    let mut schema = body.schema.clone().unwrap_or(default_schema.clone());

    if schema.is_empty() {
        schema = default_schema;
    }

    let mut statement = format!("CREATE TABLE {name} (", name = quote_ident(&body.name));

    for (idx, column_info) in schema.iter().enumerate() {
        let comma = if idx == 0 { "" } else { "," };
        statement = format!(
            "{statement}{comma}{name} {data_type}",
            name = quote_ident(&column_info.0),
            data_type = &column_info.1
        );
    }
    statement = format!("{statement})");

    let res = transaction.execute(&statement, &[]).await;

    if let Err(e) = res {
        return Err(ErrorBadRequest(format!(
            "Error: {e}. Generated Statement: {statement}"
        )));
    }

    transaction
        .execute(
            &format!("INSERT INTO {COLLECTION_TABLE_NAME} (name) VALUES ($1)"),
            &[&body.name],
        )
        .await
        .map_err(ErrorInternalServerError)?;

    transaction
        .commit()
        .await
        .map_err(ErrorInternalServerError)?;

    Ok(web::Json(CollectionInfo {
        name: body.name.clone(),
        schema,
        indexes: Vec::new(),
    }))
}

/// Delete the specified collection by name
#[utoipa::path(
    delete,
    path = "/collections/{name}",
    responses(
        (status = 200, description = "Table succesfully deleted"),
        (status = 400, description = "Bad request"),
    ),
    params(
       ("name", description = "Collection name")
    ),
)]
#[delete("/collections/{name}")]
pub async fn delete(data: web::Data<AppState>, name: web::Path<String>) -> Result<impl Responder> {
    let client = data.pool.get().await.unwrap();

    let statement = format!(
        r#"
        DROP TABLE "{name}" CASCADE;
        DELETE FROM {COLLECTION_TABLE_NAME} WHERE name='{name}';
    "#
    );

    client
        .batch_execute(&statement)
        .await
        .map_err(ErrorBadRequest)?;

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}

#[derive(Deserialize, Debug, utoipa::ToSchema)]
pub struct InserDataInput {
    rows: Vec<serde_json::Value>,
}

/// Insert rows into collection
///
/// Rows will be inserted using `COPY` protocol
/// for maximum performance.
///
/// Keys from the first row will be taken as column names
#[utoipa::path(
    put,
    path = "/collections/{name}",
    request_body  (
        content = InserDataInput,
        example = json!(r#"{ "rows": [{"vector": [1,1,1], "data": "t1", "metadata": {"k": "v"}}, {"vector": [2,2,2], "data": "t2", "metadata": {"k": "v"}}] }"#)
    ),
    responses(
        (status = 200, description = "Rows successfully inserted"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal Server Error")
    ),
    params(
       ("name", description = "Collection name")
    ),
)]
#[put("/collections/{name}")]
async fn insert_data(
    data: web::Data<AppState>,
    body: web::Json<InserDataInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let mut client = data.pool.get().await.unwrap();

    if body.rows.len() == 0 {
        return Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()));
    }

    let mut columns: Option<Vec<String>> = None;
    for row in &body.rows {
        let map = match row.as_object() {
            Some(m) => m,
            None => {
                continue;
            }
        };
        columns = Some(map.keys().map(|k| k.to_owned()).collect());
        break;
    }

    let columns = match columns {
        Some(c) => c,
        None => return Err(ErrorBadRequest("all rows are empty")),
    };

    let column_names = columns.iter().map(|k| quote_ident(k)).join(",");
    let copy_statement = format!(
        "COPY {name} ({column_names}) FROM stdin NULL 'null'",
        name = quote_ident(&name)
    );

    let transaction = client
        .transaction()
        .await
        .map_err(ErrorInternalServerError)?;

    let writer_sink = transaction
        .copy_in(&copy_statement)
        .await
        .map_err(ErrorInternalServerError)?;

    futures::pin_mut!(writer_sink);
    let chunk_size = 1024 * 1024 * 10; // 10 MB
    let mut buf = BytesMut::with_capacity(chunk_size * 2);
    for row in &body.rows {
        for (idx, column) in columns.iter().enumerate() {
            let elem = row[column].to_string();
            let mut chars = elem.chars();
            let first_char = chars.next().unwrap();
            let last_char = chars.next_back().unwrap();
            if first_char == '[' && last_char == ']' {
                buf.extend_from_slice("{".as_bytes());
                buf.extend_from_slice(elem[1..elem.len() - 1].as_bytes());
                buf.extend_from_slice("}".as_bytes());
            } else {
                buf.extend_from_slice(elem.as_bytes());
            };

            if idx != columns.len() - 1 {
                buf.extend_from_slice("\t".as_bytes());
            }
        }
        buf.extend_from_slice("\n".as_bytes());

        if buf.len() > chunk_size {
            writer_sink
                .send(buf.split().freeze())
                .await
                .map_err(ErrorInternalServerError)?;
        }
    }

    if !buf.is_empty() {
        writer_sink
            .send(buf.split().freeze())
            .await
            .map_err(ErrorInternalServerError)?;
    }

    writer_sink.finish().await.map_err(ErrorBadRequest)?;

    transaction
        .commit()
        .await
        .map_err(ErrorInternalServerError)?;

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}
