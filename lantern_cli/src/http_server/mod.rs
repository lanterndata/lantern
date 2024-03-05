use crate::types::AnyhowVoidResult;
use actix_web::{
    error::ErrorInternalServerError, middleware::Logger, web, App, HttpServer, Result,
};
use cli::HttpServerArgs;
use deadpool_postgres::{Config as PoolConfig, Manager, Pool};
use tokio_postgres::NoTls;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub mod cli;
mod data;
mod index;
mod pq;
mod setup;
mod table;

type PoolClient = deadpool::managed::Object<Manager>;

pub const COLLECTION_TABLE_NAME: &str = "_lantern_internal.collections";

struct AppPool {
    inner: Pool,
}

impl AppPool {
    fn new(pool: Pool) -> AppPool {
        Self { inner: pool }
    }

    async fn get(&self) -> Result<PoolClient, actix_web::Error> {
        let client = self.inner.get().await;
        match client {
            Err(e) => Err(ErrorInternalServerError(e)),
            Ok(client) => Ok(client),
        }
    }
}

pub struct AppState {
    db_uri: String,
    pool: AppPool,
    is_remote_database: bool,
    #[allow(dead_code)]
    logger: crate::logger::Logger,
}

/*
* PATCH - /collection/:name/:id -
*   { row: Row }
* */

#[derive(OpenApi)]
#[openapi(
    paths(
        table::get_all_tables,
        table::create_table,
        table::delete_table,
        data::insert_data
    ),
    components(schemas(table::CollectionInfo, table::CreateTableInput, data::InserDataInput))
)]
pub struct ApiDoc;
#[actix_web::main]

pub async fn run(args: HttpServerArgs, logger: crate::logger::Logger) -> AnyhowVoidResult {
    logger.info(&format!(
        "Starting web server on http://{host}:{port}",
        host = args.host,
        port = args.port,
    ));
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let mut config = PoolConfig::new();
    config.url = Some(args.db_uri.clone());
    let pool = config.create_pool(None, NoTls)?;

    setup::setup_tables(&pool).await?;
    let state = web::Data::new(AppState {
        db_uri: args.db_uri.clone(),
        is_remote_database: args.remote_database,
        pool: AppPool::new(pool),
        logger,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r - %s %Dms"))
            .app_data(state.clone())
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
            .service(table::get_all_tables)
            .service(table::create_table)
            .service(table::delete_table)
            .service(data::insert_data)
            .service(data::search)
            .service(index::create_index)
            .service(index::delete_index)
            .service(pq::quantize_table_route)
    })
    .bind((args.host.clone(), args.port))?
    .run()
    .await?;
    Ok(())
}
