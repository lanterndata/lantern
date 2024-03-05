use std::collections::HashMap;

use actix_web::{
    delete,
    error::{ErrorBadRequest, ErrorInternalServerError},
    get,
    http::StatusCode,
    post, web, HttpResponse, Responder, Result,
};

use crate::utils::quote_ident;

use super::{AppState, COLLECTION_TABLE_NAME};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, utoipa::ToSchema)]
pub struct CollectionInfo {
    name: String,
}
/// Get all tables created with Lantern HTTP API
#[utoipa::path(
    get,
    path = "/collections",
    responses(
        (status = 200, description = "Returns tables Created via Lantern HTTP API", body = Vec<CollectionInfo>),
        (status = 500, description = "Internal Server Error")
    )
)]
#[get("/collections")]
pub async fn get_all_tables(data: web::Data<AppState>) -> Result<impl Responder> {
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

/// Creates new table with specified schema
///
/// If schema is empty the default schema will be used
/// ```
/// (bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, vector REAL[], data TEXT, metadata JSONB)
/// ```
///
/// schema should be specified like this:
/// ```
/// { "id": "serial primary key", "v": "REAL[]", "t": "TEXT" }
/// ```
#[utoipa::path(
    post,
    path = "/collections",
    request_body (
        content = CreateTableInput,
        example = json!(r#"{"id": "bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", "v": "REAL[]", "t": "TEXT" }"#)
    ),
    responses(
        (status = 200, description = "Returns the created table", body = CollectionInfo),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal Server Error")
    )
)]
#[post("/collections")]
pub async fn create_table(
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

/// Deletes the specified table by name
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
pub async fn delete_table(
    data: web::Data<AppState>,
    name: web::Path<String>,
) -> Result<impl Responder> {
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
