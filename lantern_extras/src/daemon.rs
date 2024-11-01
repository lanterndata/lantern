use std::time::Duration;

use lantern_cli::{
    daemon::{cli::DaemonArgs, start},
    embeddings::core::ort_runtime::DATA_PATH,
    logger::{LogLevel, Logger},
    types::AnyhowVoidResult,
    utils::{get_full_table_name, quote_ident},
};
use pgrx::{bgworkers::BackgroundWorker, prelude::*};
use tokio_util::sync::CancellationToken;

use crate::{
    embeddings::{get_cohere_runtime_params, get_openai_runtime_params},
    DAEMON_DATABASES, ENABLE_DAEMON,
};

pub fn start_daemon(embeddings: bool, indexing: bool, autotune: bool) -> Result<(), anyhow::Error> {
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

    let logger = Logger::new("Lantern Daemon", LogLevel::Debug);
    let cancellation_token = CancellationToken::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap(); // Runtime::new().unwrap();
    rt.block_on(async {
        start(
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
                inside_postgres: true,
            },
            Some(logger.clone()),
            cancellation_token.clone(),
        )
        .await?;

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                anyhow::bail!("cancelled");
            }
            _ = async {
                loop {
                    if BackgroundWorker::sighup_received() && !ENABLE_DAEMON.get() {
                        cancellation_token.cancel();
                        break;
                    }

                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            } => {}
        }

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

#[pg_extern(immutable, parallel_unsafe, security_definer)]
fn add_embedding_job<'a>(
    table_name: &'a str,
    src_column: &'a str,
    dst_column: &'a str,
    model: default!(&'a str, "'text-embedding-3-small'"),
    pk: default!(&'a str, "'id'"),
    schema: default!(&'a str, "'public'"),
    base_url: default!(&'a str, "''"),
    batch_size: default!(i32, -1),
    dimensions: default!(i32, 1536),
    api_token: default!(&'a str, "''"),
    azure_entra_token: default!(&'a str, "''"),
    runtime: default!(&'a str, "'openai'"),
) -> Result<i32, anyhow::Error> {
    let params = match runtime {
        "openai" => {
            get_openai_runtime_params(api_token, azure_entra_token, base_url, "", dimensions)?
        }
        "cohere" => get_cohere_runtime_params(api_token, "search_document")?,
        _ => "{}".to_owned(),
    };

    let batch_size = if batch_size == -1 {
        "NULL".to_string()
    } else {
        batch_size.to_string()
    };
    let id: Option<i32> = Spi::get_one_with_args(
        &format!(
            r#"
          ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {dst_column} REAL[];
          INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("table", "schema", pk, src_column, dst_column, embedding_model, runtime, runtime_params, batch_size) VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, {batch_size}) RETURNING id;
        "#,
            table = get_full_table_name(schema, table_name),
            dst_column = quote_ident(dst_column)
        ),
        vec![
            (PgBuiltInOids::TEXTOID.oid(), table_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), schema.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), pk.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), src_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), dst_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), model.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), runtime.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), params.into_datum()),
        ],
    )?;

    Ok(id.unwrap())
}

#[pg_extern(immutable, parallel_unsafe, security_definer)]
fn add_completion_job<'a>(
    table_name: &'a str,
    src_column: &'a str,
    dst_column: &'a str,
    system_prompt: default!(&'a str, "''"),
    column_type: default!(&'a str, "'TEXT'"),
    model: default!(&'a str, "'gpt-4o'"),
    pk: default!(&'a str, "'id'"),
    schema: default!(&'a str, "'public'"),
    base_url: default!(&'a str, "''"),
    batch_size: default!(i32, -1),
    api_token: default!(&'a str, "''"),
    azure_entra_token: default!(&'a str, "''"),
    runtime: default!(&'a str, "'openai'"),
) -> Result<i32, anyhow::Error> {
    let params = match runtime {
        "openai" => {
            get_openai_runtime_params(api_token, azure_entra_token, base_url, system_prompt, 0)?
        }
        _ => anyhow::bail!("Runtime {runtime} does not support completion jobs"),
    };

    let batch_size = if batch_size == -1 {
        "NULL".to_string()
    } else {
        batch_size.to_string()
    };

    let id: Option<i32> = Spi::get_one_with_args(
        &format!(
            r#"
          ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {dst_column} {column_type};
          INSERT INTO _lantern_extras_internal.embedding_generation_jobs ("table", "schema", pk, src_column, dst_column, embedding_model, runtime, runtime_params, column_type, batch_size, job_type) VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, {batch_size}, 'completion') RETURNING id;
        "#,
            table = get_full_table_name(schema, table_name),
            dst_column = quote_ident(dst_column)
        ),
        vec![
            (PgBuiltInOids::TEXTOID.oid(), table_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), schema.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), pk.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), src_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), dst_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), model.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), runtime.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), params.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), column_type.into_datum()),
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
fn get_completion_job_failures<'a>(
    job_id: i32,
) -> Result<
    TableIterator<'static, (name!(row_id, Option<i32>), name!(value, Option<String>))>,
    anyhow::Error,
> {
    Spi::connect(|client| {
        client.select("SELECT row_id, value FROM _lantern_extras_internal.embedding_failure_info WHERE job_id=$1", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), job_id.into_datum())]))?
            .map(|row| Ok((row["row_id"].value()?, row["value"].value()?)))
            .collect::<Result<Vec<_>, _>>()
    }).map(TableIterator::new)
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
        client.select("SELECT id, (get_embedding_job_status(id)).* FROM _lantern_extras_internal.embedding_generation_jobs WHERE job_type = 'embedding_generation'", None, None)?
            .map(|row| Ok((row["id"].value()?, row["status"].value()?, row["progress"].value()?, row["error"].value()?)))
            .collect::<Result<Vec<_>, _>>()
    }).map(TableIterator::new)
}

#[pg_extern(immutable, parallel_safe, security_definer)]
fn get_completion_jobs<'a>() -> Result<
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
        client.select("SELECT id, (get_embedding_job_status(id)).* FROM _lantern_extras_internal.embedding_generation_jobs WHERE job_type = 'completion'", None, None)?
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
            let id = client.select("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);
            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_add_daemon_completion_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                SET lantern_extras.openai_token='test';
                ",
                None,
                None,
            )?;
            let id = client.select(
                "
                SELECT add_completion_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', system_prompt => 'my test prompt', column_type => 'TEXT[]');
                ",
                None,
                None,
            )?;

            let id: Option<i32> = id.first().get(1)?;
            assert_eq!(id.is_none(), false);

            let row = client.select(
                "SELECT column_type, job_type, runtime, embedding_model, (runtime_params->'system_prompt')::text as system_prompt, batch_size FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1",
                None,
                Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())])
            )?;

            let row = row.first();

            assert_eq!(row.get::<&str>(1)?.unwrap(), "TEXT[]");
            assert_eq!(row.get::<&str>(2)?.unwrap(), "completion");
            assert_eq!(row.get::<&str>(3)?.unwrap(), "openai");
            assert_eq!(row.get::<&str>(4)?.unwrap(), "gpt-4o");
            assert_eq!(row.get::<&str>(5)?.unwrap(), "\"my test prompt\"");
            assert_eq!(row.get::<i32>(6)?.is_none(), true);

            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_add_daemon_completion_job_batch_size() {
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
            let id = client.select(
                "
                SELECT add_completion_job(api_token => 'test', table_name => 't1', src_column => 'title', dst_column => 'title_embedding', system_prompt => 'my test prompt', column_type => 'TEXT[]', batch_size => 15, model => 'gpt-4o');
                ",
                None,
                None,
            )?;

            let id: Option<i32> = id.first().get(1)?;
            assert_eq!(id.is_none(), false);

            let row = client.select(
                "SELECT column_type, job_type, runtime, embedding_model, (runtime_params->'system_prompt')::text as system_prompt, batch_size, (runtime_params->'api_token')::text as api_token FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1",
                None,
                Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())])
            )?;

            let row = row.first();

            assert_eq!(row.get::<&str>(1)?.unwrap(), "TEXT[]");
            assert_eq!(row.get::<&str>(2)?.unwrap(), "completion");
            assert_eq!(row.get::<&str>(3)?.unwrap(), "openai");
            assert_eq!(row.get::<&str>(4)?.unwrap(), "gpt-4o");
            assert_eq!(row.get::<&str>(5)?.unwrap(), "\"my test prompt\"");
            assert_eq!(row.get::<i32>(6)?.unwrap(), 15);
            assert_eq!(row.get::<&str>(7)?.unwrap(), "\"test\"");

            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_get_daemon_completion_job() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                CREATE TABLE t1 (id serial primary key, title text);
                SET lantern_extras.openai_token='test';
                ",
                None,
                None,
            )?;
            let id = client.select(
                "
                SELECT add_completion_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', system_prompt => 'my test prompt', column_type => 'TEXT[]');
                ",
                None,
                None,
            )?;

            let id: Option<i32> = id.first().get(1)?;
            assert_eq!(id.is_none(), false);

            let row = client.select(
                "SELECT id, status, progress, error FROM get_completion_jobs() WHERE id=$1",
                None,
                Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())])
            )?;

            let row = row.first();

            assert_eq!(row.get::<i32>(1)?.unwrap(), id.unwrap());

            Ok::<(), anyhow::Error>(())
        })
        .unwrap();
    }

    #[pg_test]
    fn test_get_completion_job_failures() {
        Spi::connect(|mut client| {
            // wait for daemon
            std::thread::sleep(Duration::from_secs(5));
            client.update(
                "
                INSERT INTO _lantern_extras_internal.embedding_failure_info (job_id, row_id, value) VALUES
                (1, 1, '1test1'),
                (1, 2, '1test2'),
                (2, 1, '2test1');
                ",
                None,
                None,
            )?;

            let mut rows = client.select(
                "SELECT row_id, value FROM get_completion_job_failures($1)",
                None,
                Some(vec![(PgBuiltInOids::INT4OID.oid(), 1.into_datum())])
            )?;

            assert_eq!(rows.len(), 2);

            let row = rows.next().unwrap();

            assert_eq!(row.get::<i32>(1)?.unwrap(), 1);
            assert_eq!(row.get::<&str>(2)?.unwrap(), "1test1");

            let row = rows.next().unwrap();

            assert_eq!(row.get::<i32>(1)?.unwrap(), 2);
            assert_eq!(row.get::<&str>(2)?.unwrap(), "1test2");

            let mut rows = client.select(
                "SELECT row_id, value FROM get_completion_job_failures($1)",
                None,
                Some(vec![(PgBuiltInOids::INT4OID.oid(), 2.into_datum())])
            )?;

            assert_eq!(rows.len(), 1);

            let row = rows.next().unwrap();

            assert_eq!(row.get::<i32>(1)?.unwrap(), 1);
            assert_eq!(row.get::<&str>(2)?.unwrap(), "2test1");

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
                SET lantern_extras.llm_token='test_llm_token';
                ",
                None,
                None,
            )?;
            let id = client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', runtime => 'openai')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);

            let rows = client.select("SELECT (runtime_params->'api_token')::text as token FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let api_token: Option<String> = rows.first().get(1)?;

            assert_eq!(api_token.unwrap(), "\"test_llm_token\"".to_owned());

            let id = client.select("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', runtime => 'cohere')", None, None)?;

            let id: Option<i32> = id.first().get(1)?;

            assert_eq!(id.is_none(), false);

            let rows = client.select("SELECT (runtime_params->'api_token')::text as token FROM _lantern_extras_internal.embedding_generation_jobs WHERE id=$1", None, Some(vec![(PgBuiltInOids::INT4OID.oid(), id.into_datum())]))?;
            let api_token: Option<String> = rows.first().get(1)?;

            assert_eq!(api_token.unwrap(), "\"test_llm_token\"".to_owned());
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

            let id = client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;
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

            client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;
            client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding2', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;

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
            let id = client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;
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
            let id = client.update("SELECT add_embedding_job(table_name => 't1', src_column => 'title', dst_column => 'title_embedding', model => 'BAAI/bge-small-en', runtime => 'ort')", None, None)?;
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
