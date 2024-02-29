use actix_web::{get, web, App, HttpServer, Responder};
use cli::HttpServerArgs;

use crate::types::AnyhowVoidResult;

pub mod cli;

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[actix_web::main]
pub async fn run(args: HttpServerArgs) -> AnyhowVoidResult {
    HttpServer::new(|| App::new().service(greet))
        .bind((args.host.clone(), args.port))?
        .run()
        .await?;
    Ok(())
}
