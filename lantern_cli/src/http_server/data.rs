use actix_web::{
    error::{ErrorBadRequest, ErrorInternalServerError},
    http::StatusCode,
    post, put, web, HttpResponse, Responder, Result,
};
use futures::SinkExt;
use itertools::Itertools;

use crate::{external_index::cli::UMetricKind, utils::quote_ident};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};

use super::AppState;

#[derive(Deserialize, Debug, utoipa::ToSchema)]
pub struct InserDataInput {
    rows: Vec<serde_json::Value>,
}

/// Inser rows into table
///
/// Rows will be inserted using `COPY` protocol
/// for maximum performance.
///
/// Keys from the first row will be taken as column names
#[utoipa::path(
    post,
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

#[derive(Deserialize, Debug)]
struct SearchInput {
    query_vector: Option<Vec<f32>>,
    query_text: Option<String>,
    query_model: Option<String>,
    metric: Option<String>,
    select: Option<String>,
    k: Option<usize>,
    ef: Option<usize>,
}

#[derive(Serialize, Debug)]
struct SearchResponse {
    rows: Vec<serde_json::Value>,
}

#[post("/collections/{name}/search")]
async fn search(
    data: web::Data<AppState>,
    body: web::Json<SearchInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let client = data.pool.get().await.unwrap();
    let k = body.k.unwrap_or(10);
    let ef = body.ef.unwrap_or(10);
    let metric = body.metric.clone().unwrap_or("l2sq".to_owned());
    let select_fields = body.select.clone().unwrap_or("*".to_owned());

    let metric_kind = UMetricKind::from(&metric).map_err(ErrorBadRequest)?;

    let operator = metric_kind.sql_operator();
    let function = metric_kind.sql_function();

    client
        .batch_execute(&format!(
            "
        SET lantern_hnsw.init_k={k};
        SET lantern_hnsw.ef={ef};
    "
        ))
        .await
        .map_err(ErrorInternalServerError)?;

    let res = match &body.query_vector {
        Some(vector) => {
            client
                .query(
                    &format!(
                        "
           SELECT COALESCE(json_agg(q.*)::text, '[]') as data FROM (
             SELECT {select_fields}, {function}(vector, $1) as distance FROM {name} ORDER BY vector {operator} $1 LIMIT {k}
            ) q;
        ",
                        name = quote_ident(&name)
                    ),
                    &[&vector],
                )
                .await
        }
        None => {
            if body.query_model.is_none() || body.query_text.is_none() {
                return Err(ErrorBadRequest("Please provide query_vector or query_text and query_model"));
            }
            let text = body.query_text.as_ref().unwrap();
            let model = body.query_model.as_ref().unwrap();
            client
                .query(
                    &format!(
                        "
           WITH cte AS (SELECT text_embedding($1, $2) as emb)
           SELECT COALESCE(json_agg(q.*)::text, '[]') as data FROM (
              SELECT {select_fields}, {function}(vector, cte.emb) as distance FROM {name}, cte ORDER BY vector {operator} cte.emb LIMIT {k}
           ) q;
        ",
                        name = quote_ident(&name)
                    ),
                    &[model, text],
                )
                .await
        }
    };

    let response = match res {
        Err(e) => return Err(ErrorBadRequest(e)),
        Ok(rows) => rows
            .iter()
            .map(|r| serde_json::from_str(r.get(0)).unwrap())
            .collect::<Vec<serde_json::Value>>(),
    };

    Ok(web::Json(SearchResponse { rows: response }))
}
