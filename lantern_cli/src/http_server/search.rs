use actix_web::{
    error::{ErrorBadRequest, ErrorInternalServerError},
    post, web, Responder, Result,
};

use crate::{external_index::cli::UMetricKind, utils::quote_ident};
use serde::{Deserialize, Serialize};

use super::AppState;

#[derive(Deserialize, Debug, utoipa::ToSchema)]
pub struct SearchInput {
    column: String,
    query_vector: Option<Vec<f32>>,
    query_text: Option<String>,
    query_model: Option<String>,
    metric: Option<String>,
    select: Option<String>,
    k: Option<usize>,
    ef: Option<usize>,
}

#[derive(Serialize, Debug, utoipa::ToSchema)]
pub struct SearchResponse {
    rows: Vec<serde_json::Value>,
}

/// Search rows in collection using vector search operators
///
/// You can provide the `query_vector` or `query_text` with `query_model`
/// in the later case embedding will be created automatically using the query model
///
/// Output type is array of dynamic json constructed from your table columns.
///
/// The `select` param should be string with comma separated values or you can omit it to get all the
/// columns back
///
/// Metric can be one of `cosine`, `l2sq`, `hamming`
#[utoipa::path(
    post,
    path = "/collections/{name}/search",
    request_body  (
        content = SearchInput,
        examples (
         ("Search by vector" = (value = json!(r#"{ "column": "vector", "query_vector": [1,0,1], "metric": "cosine", "select": "id,metadata", "k": 10, "ef": 64 }"#) )),
         ("Search with model" = (value = json!(r#"{ "column": "vector", "query_text": "User query text", "query_model": "BAAI/bge-small-en", "metric": "l2sq", "select": "id,metadata", "k": 10, "ef": 64 }"#) ))
            
        ),
    ),
    responses(
        (status = 200, body=SearchResponse, description = "Array with the columns selected"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal Server Error")
    ),
    params(
       ("name", description = "Collection name")
    ),
)]
#[post("/collections/{name}/search")]
async fn vector_search(
    data: web::Data<AppState>,
    body: web::Json<SearchInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let client = data.pool.get().await.unwrap();
    let k = body.k.unwrap_or(10);
    let ef = body.ef.unwrap_or(10);
    let metric = body.metric.clone().unwrap_or("l2sq".to_owned());
    let select_fields = body.select.clone().unwrap_or("*".to_owned());
    let column = &body.column;

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
                .query_one(
                    &format!(
                        "
           SELECT COALESCE(json_agg(q.*)::text, '[]') as data FROM (
             SELECT {select_fields}, {function}({column}, $1) as distance FROM {name} ORDER BY {column} {operator} $1 LIMIT {k}
            ) q;
        ",
                        name = quote_ident(&name),
                        column = quote_ident(column)
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
                .query_one(
                    &format!(
                        "
           WITH cte AS (SELECT text_embedding($1, $2) as emb)
           SELECT COALESCE(json_agg(q.*)::text, '[]') as data FROM (
              SELECT {select_fields}, {function}({column}, cte.emb) as distance FROM {name}, cte ORDER BY {column} {operator} cte.emb LIMIT {k}
           ) q;
        ",
                        name = quote_ident(&name),
                        column = quote_ident(column)
                    ),
                    &[model, text],
                )
                .await
        }
    };

    let response = match res {
        Err(e) => return Err(ErrorBadRequest(e)),
        Ok(row) => serde_json::from_str(row.get(0)).unwrap(),
    };

    Ok(web::Json(SearchResponse { rows: response }))
}
