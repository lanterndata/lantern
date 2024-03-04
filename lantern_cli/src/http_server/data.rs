use actix_web::{
    error::{ErrorBadRequest, ErrorInternalServerError},
    http::StatusCode,
    post, put, web, HttpResponse, Responder, Result,
};

use crate::utils::quote_ident;
use serde::{Deserialize, Serialize};

use super::AppState;

#[derive(Deserialize, Debug)]
struct InserData {
    vector: Option<Vec<f32>>,
    data: Option<String>,
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct InserDataInput {
    rows: Vec<InserData>,
}
#[put("/collections/{name}")]
async fn insert_data(
    data: web::Data<AppState>,
    body: web::Json<InserDataInput>,
    name: web::Path<String>,
) -> Result<impl Responder> {
    let mut client = data.pool.get().await.unwrap();
    let transaction = match client.transaction().await {
        Ok(tr) => tr,
        Err(e) => return Err(ErrorInternalServerError(e)),
    };

    for row in &body.rows {
        let res = transaction
            .execute(
                &format!(
                    "INSERT INTO {name} (vector, data) VALUES ($1, $2)",
                    name = quote_ident(&name)
                ),
                &[&row.vector, &row.data],
            )
            .await;

        if let Err(e) = res {
            return Err(ErrorInternalServerError(e));
        }
    }

    if let Err(e) = transaction.commit().await {
        return Err(ErrorInternalServerError(e));
    }

    Ok(HttpResponse::new(StatusCode::from_u16(200).unwrap()))
    // let mut writer_sink = transaction
    //     .copy_in(&format!(
    //         "COPY {name} (vector, data, metadata) FROM stdin BINARY",
    //         name = quote_ident(name.as_str())
    //     ))
    //     .await
    //     .unwrap();
    // let mut writer_sink = match transaction
    //     .copy_in(&format!(
    //         "COPY {name} (vector, data, metadata) FROM stdin BINARY",
    //         name = quote_ident(name.as_str())
    //     ))
    //     .await
    // {
    //     Ok(wr) => wr,
    //     Err(e) => return Err(ErrorInternalServerError(e)),
    // };

    // let writer =
    //     BinaryCopyInWriter::new(writer_sink, &[Type::FLOAT4_ARRAY, Type::TEXT, Type::JSONB]);

    // for row in &body.rows {
    //     match row.vector {
    //         None => writer.write("NULL".as_bytes()),
    //         Some(data) => {
    //             writer.write("{".as_bytes())?;
    //             let row_str: String = data.iter().map(|&x| x.to_string() + ",").collect();
    //             writer.write(row_str[0..row_str.len() - 1].as_bytes())?;
    //             drop(row_str);
    //             writer.write("}".as_bytes())?;
    //         }
    //     }
    //     writer.write("\t".as_bytes())?;
    //     writer.write("\n".as_bytes())?;
    // }
    //
    // if let Err(e) = res {
    //     return Err(ErrorBadRequest(e));
    // }
}

#[derive(Deserialize, Debug)]
struct SearchInput {
    query_vector: Option<Vec<f32>>,
    query_text: Option<String>,
    query_model: Option<String>,
    k: Option<usize>,
    ef: Option<usize>,
}

#[derive(Serialize, Debug)]
struct SearchRow {
    id: Option<i64>,
    vector: Option<Vec<f32>>,
    data: Option<String>,
    metadata: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
struct SearchResponse {
    rows: Vec<SearchRow>,
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

    let select_fields = "id, vector::real[], data::text, metadata::text";
    let operator = "<->";

    let res = client
        .batch_execute(&format!(
            "
        SET lantern_hnsw.init_k={k};
        SET lantern_hnsw.ef={ef};
    "
        ))
        .await;

    if let Err(e) = res {
        return Err(ErrorInternalServerError(e));
    }

    let res = match &body.query_vector {
        Some(vector) => {
            client
                .query(
                    &format!(
                        "
           SELECT {select_fields} FROM {name} ORDER BY vector {operator} $1 LIMIT {k};
        ",
                        name = quote_ident(&name)
                    ),
                    &[&vector],
                )
                .await
        }
        None => {
            if body.query_model.is_none() || body.query_text.is_none() {
                return Err(ErrorBadRequest("Please provide query_text and query_model"));
            }
            let text = body.query_text.as_ref().unwrap();
            let model = body.query_model.as_ref().unwrap();
            client
            .query(
                &format!(
                    "
           SELECT {select_fields} FROM {name} ORDER BY vector {operator} text_embedding($1, $2) LIMIT {k};
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
            .map(|r| {
                let metadata = match serde_json::from_str(&r.get::<&str, String>("metadata")) {
                    Ok(val) => val,
                    Err(_) => serde_json::from_str("{}").unwrap(),
                };

                SearchRow {
                    id: r.get("id"),
                    vector: r.get::<&str, Option<Vec<f32>>>("vector"),
                    data: r.get::<&str, Option<String>>("data"),
                    metadata: Some(metadata),
                }
            })
            .collect::<Vec<SearchRow>>(),
    };

    Ok(web::Json(SearchResponse { rows: response }))
}
