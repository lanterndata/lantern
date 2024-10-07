use actix_web::{
    delete,
    error::{ErrorBadRequest, ErrorInternalServerError},
    http::StatusCode,
    post, web, HttpResponse, Responder, Result,
};

use crate::{
    external_index::{
        cli::{CreateIndexArgs, UMetricKind},
        create_usearch_index,
    },
    utils::quote_ident,
};

use rand::Rng;
use serde::Deserialize;

use super::AppState;

#[derive(Deserialize, Debug, utoipa::ToSchema)]
pub struct CreateIndexInput {
    name: Option<String>,
    column: String,
    metric: Option<String>,
    ef: Option<usize>,
    ef_construction: Option<usize>,
    m: Option<usize>,
    pq: Option<bool>,
    external: Option<bool>,
}

/// Create vector index on the collection
///
/// The `external` param indicates if the index should be built using lantern_cli's high performant
/// indexing or inside postgres on a single core.
///
/// The `pq` argument should be passed only if you have quantized your table and want to create a
/// pq index over it
///
/// Metric can be one of `cosine`, `l2sq`, `hamming`
#[utoipa::path(
    post,
    path = "/collections/{name}/index",
    request_body  (
        content = CreateIndexInput,
        example = json!(r#"{ "metric": "cosine", "column": "vector", "ef_construction": 128, "ef": 64, "m": 16, "pq": false, "external": true }"#),
    ),
    responses(
        (status = 200, description = "Index created successfully"),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal Server Error")
    ),
    params(
       ("name", description = "Collection name")
    ),
)]
#[post("/collections/{name}/index")]
async fn create_index(
    data: web::Data<AppState>,
    body: web::Json<CreateIndexInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let external = body.external.unwrap_or(true);
    let metric = body.metric.clone().unwrap_or("l2sq".to_owned());
    let column = body.column.clone();
    let ef = body.ef.unwrap_or(64);
    let ef_construction = body.ef_construction.unwrap_or(128);
    let m = body.m.unwrap_or(16);
    let pq = body.pq.unwrap_or(false);
    let index_name = body.name.clone().unwrap_or("".to_owned());

    let metric_kind = UMetricKind::from(&metric).map_err(ErrorBadRequest)?;

    let client = data.pool.get().await?;
    if !external {
        client
            .execute(
                &format!(
                    "CREATE INDEX {index_name} ON {name} USING lantern_hnsw({column} {op_class}) WITH (m={m}, ef={ef}, ef_construction={ef_construction}, pq={pq})",
                    index_name = quote_ident(&index_name),
                    name = quote_ident(&name),
                    column = quote_ident(&column),
                    op_class = metric_kind.to_ops()
                ),
                &[],
            )
            .await.map_err(ErrorInternalServerError)?;
    } else {
        let data_dir = if !data.is_remote_database {
            match client
                .query_one(
                    "SELECT setting::text FROM pg_settings WHERE name = 'data_directory'",
                    &[],
                )
                .await
            {
                Err(e) => {
                    return Err(ErrorInternalServerError(e));
                }
                Ok(row) => row.get::<usize, String>(0),
            }
        } else {
            "/tmp".to_owned()
        };

        let mut rng = rand::thread_rng();
        let index_path = format!("{data_dir}/ldb-index-{}.usearch", rng.gen_range(0..1000));
        let body_index_name = body.name.clone();
        tokio::task::spawn_blocking(move || {
            create_usearch_index(
                &CreateIndexArgs {
                    column: column.to_owned(),
                    metric_kind,
                    efc: ef_construction,
                    ef,
                    m,
                    dims: 0,
                    import: true,
                    index_name: body_index_name,
                    uri: data.db_uri.clone(),
                    out: index_path,
                    schema: "public".to_owned(),
                    table: name.clone(),
                    pq,
                    remote_database: data.is_remote_database,
                },
                None,
                None,
                None,
            )
        })
        .await
        .map_err(ErrorInternalServerError)?
        .map_err(ErrorInternalServerError)?;
    }

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}

/// Delete the specified index by name
#[utoipa::path(
    delete,
    path = "/index/{index_name}",
    responses(
        (status = 200, description = "Index succesfully deleted"),
        (status = 500, description = "Internal server error"),
    ),
    params(
       ("index_name", description = "Name of the index")
    ),
)]
#[delete("/index/{index_name}")]
async fn delete_index(
    data: web::Data<AppState>,
    index_name: web::Path<String>,
) -> Result<impl Responder> {
    let client = data.pool.get().await?;
    let res = client
        .execute(
            &format!(
                "DROP INDEX {index_name} CASCADE",
                index_name = quote_ident(&index_name),
            ),
            &[],
        )
        .await;

    if let Err(e) = res {
        return Err(ErrorInternalServerError(e));
    }

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}
