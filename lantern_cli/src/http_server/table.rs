use actix_web::{
    delete,
    error::{ErrorBadRequest, ErrorInternalServerError},
    get,
    http::StatusCode,
    post, web, HttpResponse, Responder, Result,
};

use super::{AppState, COLLECTION_TABLE_NAME};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
struct CollectionInfo {
    name: String,
}

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

#[derive(Deserialize, Debug)]
struct CreateTableInput {
    name: String,
}

#[post("/collections")]
pub async fn create_table(
    data: web::Data<AppState>,
    body: web::Json<CreateTableInput>,
) -> Result<impl Responder> {
    let client = data.pool.get().await.unwrap();

    let statement = format!(
        r#"CREATE TABLE "{name}"(
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      vector REAL[],
      data TEXT,
      metadata JSONB
    )"#,
        name = body.name
    );
    let res = client.execute(&statement, &[]).await;

    if let Err(e) = res {
        return Err(ErrorBadRequest(e));
    }

    Ok(web::Json(CollectionInfo {
        name: body.name.clone(),
    }))
}

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
    let res = client.batch_execute(&statement).await;

    if let Err(e) = res {
        return Err(ErrorBadRequest(e));
    }

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}
