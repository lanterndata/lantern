use bytes::BytesMut;
use futures::SinkExt;
use itertools::Itertools;
use std::collections::HashMap;

use actix_web::{
    delete,
    error::{ErrorBadRequest, ErrorInternalServerError},
    get,
    http::StatusCode,
    post, put, web, HttpResponse, Responder, Result,
};

use crate::utils::quote_ident;

use super::{AppState, COLLECTION_TABLE_NAME};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, utoipa::ToSchema)]
pub struct CollectionInfo {
    name: String,
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
        .query(&format!("SELECT name FROM {COLLECTION_TABLE_NAME}"), &[])
        .await;

    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => return Err(ErrorInternalServerError(e)),
    };

    let tables: Vec<CollectionInfo> = rows
        .iter()
        .map(|r| CollectionInfo {
            name: r.get::<usize, String>(0),
        })
        .collect();

    Ok(web::Json(tables))
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
    let mut buf = BytesMut::new();
    for row in &body.rows {
        let mut row_str = String::from("");
        for (idx, column) in columns.iter().enumerate() {
            let entry = row[column].to_string().replace("[", "{").replace("]", "}");
            row_str.push_str(&entry);
            if idx != columns.len() - 1 {
                row_str.push_str("\t");
            }
        }
        row_str.push_str("\n");

        if buf.len() > 4096 {
            writer_sink
                .send(buf.split().freeze())
                .await
                .map_err(ErrorInternalServerError)?;
        }

        buf.extend_from_slice(row_str.as_bytes());
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
