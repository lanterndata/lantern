use actix_web::{
    error::{ErrorInternalServerError, ErrorUnprocessableEntity},
    http::StatusCode,
    post, web, HttpResponse, Responder, Result,
};

use crate::pq::{cli::PQArgs, quantize_table};

use serde::Deserialize;

use super::AppState;

#[derive(Deserialize, Debug)]
struct CreatePQInput {
    column: String,
    clusters: Option<usize>,
    splits: usize,
    limit: Option<usize>,
}

#[post("/collections/{name}/pq")]
async fn quantize_table_route(
    data: web::Data<AppState>,
    body: web::Json<CreatePQInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let column = body.column.clone();
    let clusters = body.clusters.unwrap_or(256);
    let splits = body.splits;
    let dataset_limit = body.limit.clone();

    let client = data.pool.get().await?;

    let pk_query = format!("SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{name}'::regclass AND i.indisprimary");
    let pk = match client.query(&pk_query, &[]).await {
        Ok(rows) => {
            if rows.is_empty() {
                return Err(ErrorUnprocessableEntity(
                    "Table should have serial primary key",
                ));
            }

            rows.first().unwrap().get::<usize, String>(0)
        }
        Err(e) => return Err(ErrorInternalServerError(e)),
    };

    tokio::task::spawn_blocking(move || {
        quantize_table(
            PQArgs {
                column,
                clusters,
                splits,
                pk,
                uri: data.db_uri.clone(),
                table: name.clone(),
                schema: "public".to_owned(),
                codebook_table_name: None,
                dataset_limit,
                subvector_id: None,
                skip_table_setup: true,
                skip_vector_quantization: false,
                skip_codebook_creation: true,
                total_task_count: None,
                parallel_task_count: None,
                quantization_task_id: None,
                run_on_gcp: false,
                gcp_cli_image_tag: None,
                gcp_project: None,
                gcp_region: None,
                gcp_image: None,
                gcp_quantization_task_count: None,
                gcp_quantization_task_parallelism: None,
                gcp_clustering_task_parallelism: None,
                gcp_enable_image_streaming: false,
                gcp_clustering_cpu: None,
                gcp_clustering_memory_gb: None,
                gcp_quantization_cpu: None,
                gcp_quantization_memory_gb: None,
                dataset_size: None,
                start_offset_id: None,
            },
            None,
            None,
            None,
        )
    })
    .await
    .map_err(ErrorInternalServerError)?
    .map_err(ErrorInternalServerError)?;

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
}
