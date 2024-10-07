use lantern_cli::{
    daemon::{cli::DaemonArgs, start},
    embeddings::core::{
        cohere_runtime::CohereRuntimeParams, openai_runtime::OpenAiRuntimeParams,
        ort_runtime::DATA_PATH,
    },
    logger::{LogLevel, Logger},
    types::AnyhowVoidResult,
    utils::{get_full_table_name, quote_ident},
};
use pgrx::{bgworkers::BackgroundWorker, prelude::*};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use crate::{
    COHERE_TOKEN, DAEMON_DATABASES, OPENAI_AZURE_API_TOKEN, OPENAI_AZURE_ENTRA_TOKEN,
    OPENAI_DEPLOYMENT_URL, OPENAI_TOKEN,
};

pub fn start_daemon(
    embeddings: bool,
    indexing: bool,
    autotune: bool,
    cancellation_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let (db, user, socket_path, port) = BackgroundWorker::transaction(|| {
        Spi::connect(|client| {
            let row = client
            .select(
                "
           SELECT current_database()::text AS db,
           (SELECT setting::text FROM pg_settings WHERE name = 'unix_socket_directories') AS socket_path,
           (SELECT setting::text FROM pg_settings WHERE name = 'port') AS port,
           (SELECT rolname::text FROM pg_roles WHERE rolsuper = true LIMIT 1) as user
           ",
                None,
                None,
            )?
            .first();

            let db = row.get_by_name::<String, &str>("db")?.unwrap();
            let socket_path = row.get_by_name::<String, &str>("socket_path")?.unwrap();
            let port = row.get_by_name::<String, &str>("port")?.unwrap();
            let user = row.get_by_name::<String, &str>("user")?.unwrap();
            let db = if db.trim() != "" {
                db
            } else {
                "postgres".to_owned()
            };

            Ok::<(String, String, String, String), anyhow::Error>((db, user, socket_path, port))
        })
    })?;

    let mut target_dbs = vec![];

    if let Some(db_list) = DAEMON_DATABASES.get() {
        let list = db_list.to_str()?.trim();
        if list != "" {
            for db_name in list.split(",") {
                let connection_string = format!(
                    "postgresql://{user}@{socket_path}:{port}/{db}",
                    socket_path = socket_path.replace("/", "%2F"),
                    db = db_name.trim()
                );
                target_dbs.push(connection_string);
            }
        }
    }

    if target_dbs.len() == 0 {
        let connection_string = format!(
            "postgresql://{user}@{socket_path}:{port}/{db}",
            socket_path = socket_path.replace("/", "%2F")
        );
        target_dbs.push(connection_string);
    }

    std::thread::spawn(move || {
        let logger = Logger::new("Lantern Daemon", LogLevel::Debug);
        let rt = Runtime::new().unwrap();
        let res = rt.block_on(start(
            DaemonArgs {
                label: None,
                embeddings,
                external_index: indexing,
                autotune,
                log_level: lantern_cli::daemon::cli::LogLevel::Debug,
                databases_table: String::new(),
                master_db: None,
                master_db_schema: String::new(),
                schema: String::from("_lantern_extras_internal"),
                target_db: Some(target_dbs.clone()),
                data_path: Some(DATA_PATH.to_owned()),
            },
            Some(logger.clone()),
            cancellation_token.clone(),
        ));

        if let Err(e) = res {
            eprintln!("{e}");
            logger.error(&format!("{e}"));
        }
    });

    Ok(())
}

#[pg_extern(immutable, parallel_unsafe, security_definer)]
fn add_embedding_job<'a>(
    table: &'a str,
    src_column: &'a str,
    dst_column: &'a str,
    embedding_model: &'a str,
    runtime: default!(&'a str, "'ort'"),
    runtime_params: default!(&'a str, "'{}'"),
    pk: default!(&'a str, "'id'"),
    schema: default!(&'a str, "'public'"),
) -> Result<i32, anyhow::Error> {
    let mut params = runtime_params.to_owned();
    if params == "{}" {
        match runtime {
            "openai" => {
                let base_url = if let Some(deployment_url) = OPENAI_DEPLOYMENT_URL.get() {
                    Some(deployment_url.to_str().unwrap().to_owned())
                } else {
                    None
                };

                let api_token = if let Some(api_token) = OPENAI_TOKEN.get() {
                    Some(api_token.to_str().unwrap().to_owned())
                } else {
                    None
                };

                let azure_api_token = if let Some(api_token) = OPENAI_AZURE_API_TOKEN.get() {
                    Some(api_token.to_str().unwrap().to_owned())
                } else {
                    None
                };

                let azure_entra_token = if let Some(api_token) = OPENAI_AZURE_ENTRA_TOKEN.get() {
                    Some(api_token.to_str().unwrap().to_owned())
                } else {
                    None
                };

                params = serde_json::to_string(&OpenAiRuntimeParams {
                    dimensions: Some(1536),
                    base_url,
                    api_token,
                    azure_api_token,
                    azure_entra_token,
                })?;
            }
            "cohere" => {
                let api_token = if let Some(api_token) = COHERE_TOKEN.get() {
                    Some(api_token.to_str().unwrap().to_owned())
                } else {
                    None
                };

                params = serde_json::to_string(&CohereRuntimeParams {
                    api_token,
                    input_type: Some("search_document".to_owned()),
                })?;
            }
            _ => {}
        }
    }

    let id: Option<i32> = Spi::get_one_with_args(
        &format!(
            r#"
          ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {dst_column} REAL[];
          INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("table", "schema", pk, src_column, dst_column, embedding_model, runtime, runtime_params) VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id;
        "#,
            table = get_full_table_name(schema, table),
            dst_column = quote_ident(dst_column)
        ),
        vec![
            (PgBuiltInOids::TEXTOID.oid(), table.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), schema.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), pk.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), src_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), dst_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), embedding_model.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), runtime.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), params.into_datum()),
        ],
    )?;

    Ok(id.unwrap())
}

#[pg_extern(immutable, parallel_safe, security_definer)]
fn get_embedding_job_status<'a>(
    job_id: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(status, Option<String>),
            name!(progress, Option<i16>),
            name!(error, Option<String>),
        ),
    >,
    anyhow::Error,
> {
    let tuple = Spi::get_three_with_args(
        r#"
          SELECT
          CASE
            WHEN init_failed_at IS NOT NULL THEN 'failed'
            WHEN canceled_at IS NOT NULL THEN 'canceled'
            WHEN init_finished_at IS NOT NULL THEN 'enabled'
            WHEN init_started_at IS NOT NULL THEN 'in_progress'
            ELSE 'queued'
          END AS status,
          init_progress as progress,
          init_failure_reason as error
          FROM _lantern_extras_internal.embedding_generation_jobs
          WHERE id=$1;
        "#,
        vec![(PgBuiltInOids::INT4OID.oid(), job_id.into_datum())],
    );

    if tuple.is_err() {
        return Ok(TableIterator::once((None, None, None)));
    }

    Ok(TableIterator::once(tuple.unwrap()))
}

#[pg_extern(immutable, parallel_safe, security_definer)]
fn get_embedding_jobs<'a>() -> Result<
    TableIterator<
        'static,
        (
            name!(id, Option<i32>),
            name!(status, Option<String>),
            name!(progress, Option<i16>),
            name!(error, Option<String>),
        ),
    >,
    anyhow::Error,
> {
    Spi::connect(|client| {
        client.select("SELECT id, (get_embedding_job_status(id)).* FROM _lantern_extras_internal.embedding_generation_jobs", None, None)?
            .map(|row| Ok((row["id"].value()?, row["status"].value()?, row["progress"].value()?, row["error"].value()?)))
            .collect::<Result<Vec<_>, _>>()
    }).map(TableIterator::new)
}

#[pg_extern(immutable, parallel_safe, security_definer)]
fn cancel_embedding_job<'a>(job_id: i32) -> AnyhowVoidResult {
    Spi::run_with_args(
        r#"
          UPDATE _lantern_extras_internal.embedding_generation_jobs
          SET canceled_at=NOW()
          WHERE id=$1;
        "#,
        Some(vec![(PgBuiltInOids::INT4OID.oid(), job_id.into_datum())]),
    )?;

    Ok(())
}

#[pg_extern(immutable, parallel_safe, security_definer)]
fn resume_embedding_job<'a>(job_id: i32) -> AnyhowVoidResult {
    Spi::run_with_args(
        r#"
          UPDATE _lantern_extras_internal.embedding_generation_jobs
          SET canceled_at=NULL
          WHERE id=$1;
        "#,
        Some(vec![(PgBuiltInOids::INT4OID.oid(), job_id.into_datum())]),
    )?;

    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
pub mod tests {
    use crate::*;
    use std::time::Duration;

    #[pg_test]
    fn test_add_daemon_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                ",
                None,
                None,
            )?;
            let id = client.select("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_add_daemon_job_default_params() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                SET lantern_extras.openai_token='test_openai';
                SET lantern_extras.cohere_token='test_cohere';
                ",
                None,
                None,
            )?;
            let id = client.select("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'openai', '{}', 'id', 'public')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);

            let rows = client.select("SELECT (runtime_params->'api_token')::text as token FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let api_token: Option<String> = rows.first().get(1)?;

            assert_eq!(api_token.unwrap(), "\"test_openai\"".to_owned());

            let id = client.select("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'cohere', '{}', 'id', 'public')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);

            let rows = client.select("SELECT (runtime_params->'api_token')::text as token FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let api_token: Option<String> = rows.first().get(1)?;

            assert_eq!(api_token.unwrap(), "\"test_cohere\"".to_owned());
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_get_daemon_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                ",
                None,
                None,
            )?;

            let id = client.update("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;
            let id: i32 = id.first().get(1)?.unwrap();

            // queued
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "queued");
            assert_eq!(progress, 0);
            assert_eq!(error, None);

            // Failed

            client.update("UPDATE _lantern_extras_internal.embedding_generation_jobs SET init_failed_at=NOW(), init_failure_reason='test';", None, None)?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: &str = job.get(3)?.unwrap();

            assert_eq!(status, "failed");
            assert_eq!(progress, 0);
            assert_eq!(error, "test");

            // In progress
            client.update("UPDATE _lantern_extras_internal.embedding_generation_jobs SET init_failed_at=NULL, init_failure_reason=NULL, init_progress=60, init_started_at=NOW();", None, None)?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "in_progress");
            assert_eq!(progress, 60);
            assert_eq!(error, None);

            // Canceled
            client.update("UPDATE _lantern_extras_internal.embedding_generation_jobs SET init_failed_at=NULL, init_failure_reason=NULL, init_progress=0, init_started_at=NULL, canceled_at=NOW();", None, None)?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "canceled");
            assert_eq!(progress, 0);
            assert_eq!(error, None);

            // Enabled
            client.update("UPDATE _lantern_extras_internal.embedding_generation_jobs SET init_failed_at=NULL, init_failure_reason=NULL, init_progress=100, init_started_at=NULL, canceled_at=NULL, init_finished_at=NOW();", None, None)?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "enabled");
            assert_eq!(progress, 100);
            assert_eq!(error, None);

            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_get_daemon_jobs() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                ",
                None,
                None,
            )?;

            client.update("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;
            client.update("SELECT add_embedding_job('t1', 'title', 'title_embedding2', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;

            // queued
            let rows = client.select("SELECT status, progress, error FROM get_embedding_jobs()", None, None)?;
            for job in rows {
                let status: &str = job.get(1)?.unwrap();
                let progress: i16 = job.get(2)?.unwrap();
                let error: Option<&str> = job.get(3)?;

                assert_eq!(status, "queued");
                assert_eq!(progress, 0);
                assert_eq!(error, None);
            }

            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_cancel_daemon_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                ",
                None,
                None,
            )?;
            let id = client.update("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;
            let id: i32 = id.first().get(1)?.unwrap();
            client.update("SELECT cancel_embedding_job($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "canceled");
            assert_eq!(progress, 0);
            assert_eq!(error, None);
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_resume_daemon_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                ",
                None,
                None,
            )?;
            let id = client.update("SELECT add_embedding_job('t1', 'title', 'title_embedding', 'BAAI/bge-small-en', 'ort', '{}', 'id', 'public')", None, None)?;
            let id: i32 = id.first().get(1)?.unwrap();
            client.update("SELECT cancel_embedding_job($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            client.update("SELECT resume_embedding_job($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let rows = client.select("SELECT status, progress, error FROM get_embedding_job_status($1)", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let job = rows.first();

            let status: &str = job.get(1)?.unwrap();
            let progress: i16 = job.get(2)?.unwrap();
            let error: Option<&str> = job.get(3)?;

            assert_eq!(status, "queued");
            assert_eq!(progress, 0);
            assert_eq!(error, None);
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }
}
