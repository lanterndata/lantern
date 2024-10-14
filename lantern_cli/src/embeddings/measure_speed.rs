use std::{cmp, time::Instant};

use super::core::{EmbeddingRuntime, Runtime};
use crate::logger::{LogLevel, Logger};
use postgres::NoTls;
use tokio_util::sync::CancellationToken;

use super::cli::MeasureModelSpeedArgs;
use crate::types::*;

static TABLE_NAME: &'static str = "_lantern_emb_test";
static SCHEMA_NAME: &'static str = "_lantern_test";
static COLUMN_NAME: &'static str = "title";
static OUT_COLUMN_NAME: &'static str = "title_emb";
static PK_NAME: &'static str = "id";
static LOREM_TEXT: &'static str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer efficitur sem dui, at ultricies velit congue nec. Aenean in neque nunc. Fusce a auctor elit. Proin convallis fringilla mauris ut congue. Donec pretium, justo lobortis pharetra finibus, nulla elit pretium magna, et elementum nisl turpis vitae arcu. Nam vitae enim non magna porttitor tristique. Suspendisse ac dapibus massa. Proin pulvinar felis sed lobortis sagittis. Etiam efficitur leo ut eros mollis, vel tempus justo faucibus. Integer iaculis sed elit vel blandit. Sed maximus libero tortor. Nam vitae dui euismod urna egestas tincidunt. Suspendisse ante felis, feugiat in metus ut, mollis consequat mi. Mauris quis augue vitae mi auctor rutrum. Nulla commodo pharetra erat, ac lacinia leo euismod a. Ut consequat mollis enim, id tristique metus vehicula vitae. Phasellus venenatis faucibus dolor. Morbi a metus odio. Aenean gravida eleifend ante. Proin at mi tristique, varius risus a, porttitor ligula. Vestibulum hendrerit pellentesque risus eu semper. Proin eu condimentum enim.";

async fn measure_model_speed(
    runtime: &Runtime,
    runtime_params: &String,
    model_name: &str,
    db_uri: &str,
    table_name: &str,
    initial_limit: u32,
    batch_size: Option<usize>,
) -> AnyhowUsizeResult {
    let mut limit = initial_limit;
    let speed: usize;
    let mut i = 0;
    loop {
        let logger = Logger::new("Lantern Embeddings", LogLevel::Error);
        let args = super::cli::EmbeddingArgs {
            uri: db_uri.to_owned(),
            pk: "id".to_owned(),
            create_column: false,
            stream: false,
            model: model_name.to_owned(),
            column: COLUMN_NAME.to_owned(),
            out_column: OUT_COLUMN_NAME.to_owned(),
            schema: SCHEMA_NAME.to_owned(),
            table: table_name.to_owned(),
            out_uri: None,
            out_table: None,
            runtime: runtime.clone(),
            runtime_params: runtime_params.to_owned(),
            batch_size: batch_size.clone(),
            visual: false,
            limit: Some(limit.clone()),
            filter: None,
        };
        let start = Instant::now();
        let (processed, _) = super::create_embeddings_from_db(
            args,
            false,
            None,
            CancellationToken::new(),
            Some(logger),
        )
        .await?;
        let elapsed = start.elapsed();

        if i == 0 {
            // skip first iteration to not count the downloading and cold start time
            i = 1;
            continue;
        }

        if elapsed.as_millis() >= 1500 {
            speed = processed / elapsed.as_secs() as usize;
            break;
        }

        limit = limit * 2;
    }
    return Ok(speed.try_into()?);
}

pub async fn start_speed_test(
    args: &MeasureModelSpeedArgs,
    logger: Option<Logger>,
) -> AnyhowVoidResult {
    // connect to database
    let table_name_small = format!("{TABLE_NAME}_min");
    let table_name_large = format!("{TABLE_NAME}_max");

    let (client, connection) = tokio_postgres::connect(&args.uri, NoTls).await?;

    tokio::spawn(async move { connection.await });
    client.batch_execute(&format!("
       DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE;
       CREATE SCHEMA {SCHEMA_NAME};
       SET search_path TO {SCHEMA_NAME};
       CREATE TABLE {table_name_small} ({PK_NAME} SERIAL PRIMARY KEY, {COLUMN_NAME} TEXT, {OUT_COLUMN_NAME} REAL[]);
       CREATE TABLE {table_name_large} ({PK_NAME} SERIAL PRIMARY KEY, {COLUMN_NAME} TEXT, {OUT_COLUMN_NAME} REAL[]);
       INSERT INTO {table_name_small} SELECT generate_series(0, 5000), 'My small title text!';
       INSERT INTO {table_name_large} SELECT generate_series(0, 5000), 'title';
    ")).await?;

    let mut text = LOREM_TEXT.to_owned();
    let word_count = LOREM_TEXT.split(" ").collect::<Vec<_>>().len();

    if args.max_tokens > word_count {
        let repeat_cnt = cmp::max(args.max_tokens / word_count, 1);
        text = LOREM_TEXT.repeat(repeat_cnt);
    }

    client
        .execute(
            &format!("UPDATE {table_name_large} SET {COLUMN_NAME}=$1;"),
            &[&text],
        )
        .await?;

    let runtime = EmbeddingRuntime::new(&args.runtime, None, &args.runtime_params)?;

    let models: Vec<_> = runtime
        .get_available_models()
        .await
        .1
        .iter()
        .filter_map(|el| {
            if let Some(model) = &args.model {
                if el.0 == *model {
                    return Some(model.clone());
                }

                return None;
            }

            if !el.1 {
                return Some(el.0.clone());
            }

            None
        })
        .collect();

    let logger = logger.unwrap_or(Logger::new("Lantern Embeddings", LogLevel::Info));
    for model_name in models {
        let speed_max = measure_model_speed(
            &args.runtime,
            &args.runtime_params,
            &model_name,
            &args.uri,
            &table_name_small,
            args.initial_limit,
            args.batch_size,
        )
        .await?;
        let speed_min = measure_model_speed(
            &args.runtime,
            &args.runtime_params,
            &model_name,
            &args.uri,
            &table_name_large,
            args.initial_limit,
            args.batch_size,
        )
        .await?;
        let speed_avg = (speed_min + speed_max) / 2;

        logger.info(&format!("{model_name} max speed - {speed_max} emb/s"));
        logger.info(&format!("{model_name} min speed - {speed_min} emb/s"));
        logger.info(&format!("{model_name} avg speed - {speed_avg} emb/s"));
    }
    client
        .execute(&format!("DROP SCHEMA {SCHEMA_NAME} CASCADE"), &[])
        .await?;
    Ok(())
}
