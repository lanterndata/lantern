use std::{collections::HashSet, time::Instant};
use uuid::Uuid;

use lantern_create_index::cli::CreateIndexArgs;
use lantern_logger::{LogLevel, Logger};
use lantern_utils::{append_params_to_uri, get_full_table_name, quote_ident};
use postgres::{types::ToSql, Client, NoTls};
use rand::Rng;

pub mod cli;

type AnyhowVoidResult = Result<(), anyhow::Error>;
type GroundTruth = Vec<(Vec<f32>, Vec<String>)>;

static INTERNAL_SCHEMA_NAME: &'static str = "_lantern_internal";
static CONNECTION_PARAMS: &'static str = "connect_timeout=10";

#[derive(Debug)]
struct IndexParams {
    ef: usize,
    ef_construction: usize,
    m: usize,
}

#[derive(Debug, Clone)]
struct AutotuneResult {
    job_id: String,
    metric_kind: String,
    ef: i32,
    ef_construction: i32,
    m: i32,
    k: i32,
    dim: i32,
    sample_size: i32,
    recall: f64,
    latency: i32,
    indexing_duration: i32,
}

fn create_test_table(
    client: &mut Client,
    tmp_table_name: &str,
    src_table_name: &str,
    column_name: &str,
    test_data_size: usize,
) -> Result<usize, anyhow::Error> {
    client.batch_execute(&format!(
        "
      CREATE SCHEMA IF NOT EXISTS {INTERNAL_SCHEMA_NAME};
      DROP TABLE IF EXISTS {tmp_table_name};
      SELECT * INTO {tmp_table_name} FROM {src_table_name} LIMIT {test_data_size};
    "
    ))?;
    let dims = client.query_one(
        &format!(
            "SELECT ARRAY_LENGTH({column_name}, 1) FROM {tmp_table_name} LIMIT 1",
            column_name = quote_ident(column_name)
        ),
        &[],
    )?;
    let dims: i32 = dims.get(0);

    if dims == 0 {
        anyhow::bail!("Column does not have dimensions");
    }

    Ok(dims as usize)
}

fn create_results_table(client: &mut Client, result_table_full_name: &str) -> AnyhowVoidResult {
    client.execute(&format!("CREATE TABLE IF NOT EXISTS {result_table_full_name} (id SERIAL PRIMARY KEY, job_id TEXT, ef INT, ef_construction INT, m INT, k INT, recall FLOAT, latency INT, dim INT, sample_size INT, indexing_duration INT, metric_kind TEXT)"), &[])?;
    Ok(())
}

fn export_results(
    client: &mut Client,
    result_table_full_name: &str,
    autotune_results: Vec<AutotuneResult>,
) -> AnyhowVoidResult {
    let mut query = format!("INSERT INTO {result_table_full_name} (job_id, ef, ef_construction, m, k, recall, latency, dim, sample_size, indexing_duration, metric_kind) VALUES ");
    let mut param_idx = 1;
    let params: Vec<&(dyn ToSql + Sync)> = autotune_results
        .iter()
        .flat_map(|row| {
            let comma_str = if param_idx == 1 { "" } else { "," };
            query = format!(
                "{}{} (${},${},${},${},${},${},${},${},${},${},${})",
                query,
                comma_str,
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3,
                param_idx + 4,
                param_idx + 5,
                param_idx + 6,
                param_idx + 7,
                param_idx + 8,
                param_idx + 9,
                param_idx + 10,
            );

            param_idx += 11;
            [
                &row.job_id as &(dyn ToSql + Sync),
                &row.ef,
                &row.ef_construction,
                &row.m,
                &row.k,
                &row.recall,
                &row.latency,
                &row.dim,
                &row.sample_size,
                &row.indexing_duration,
                &row.metric_kind,
            ]
        })
        .collect();

    client.execute(&query, &params[..])?;

    Ok(())
}

fn find_best_variant<'a>(
    autotune_results: &'a Vec<AutotuneResult>,
    _taret_recall: u64,
) -> &'a AutotuneResult {
    return autotune_results.first().unwrap();
}

fn calculate_ground_truth(
    client: &mut Client,
    pk: &str,
    emb_col: &str,
    tmp_table_name: &str,
    truth_table_name: &str,
    distance_function: &str,
    k: u16,
) -> Result<GroundTruth, anyhow::Error> {
    client.batch_execute(&format!(
        "
         DROP TABLE IF EXISTS {truth_table_name};
         SELECT tmp.{pk} as id, tmp.{emb_col}::real[] as v, ARRAY(SELECT {pk}::text FROM {tmp_table_name} tmp2 ORDER BY {distance_function}(tmp.{emb_col}, tmp2.{emb_col}) LIMIT {k}) as neighbors
         INTO {truth_table_name}
         FROM {tmp_table_name} tmp
         WHERE {pk} IN (SELECT {pk} FROM {tmp_table_name} ORDER BY RANDOM() LIMIT 10)",
        pk = quote_ident(pk),
        emb_col = quote_ident(emb_col),
    ))?;
    let ground_truth = client.query(
        &format!(
            "SELECT {emb_col}, neighbors FROM {truth_table_name}",
            emb_col = quote_ident(emb_col)
        ),
        &[],
    )?;

    Ok(ground_truth
        .iter()
        .map(|row| {
            return (
                row.get::<usize, Vec<f32>>(0),
                row.get::<usize, Vec<String>>(1),
            );
        })
        .collect())
}

fn calculate_recall_and_latency(
    client: &mut Client,
    ground_truth: &GroundTruth,
    test_table_name: &str,
    k: u16,
) -> Result<(f32, usize), anyhow::Error> {
    let mut recall: f32 = 0.0;
    let mut latency: usize = 0;

    for (query, neighbors) in ground_truth {
        let start = Instant::now();
        let rows = client.query(
            &format!("SELECT id::text FROM {test_table_name} ORDER BY $1<->v LIMIT {k}"),
            &[query],
        )?;
        latency += start.elapsed().as_millis() as usize;

        let truth: HashSet<String> = neighbors.into_iter().map(|s| s.to_owned()).collect();
        let result: HashSet<String> = rows
            .into_iter()
            .map(|r| r.get::<usize, &str>(0).to_owned())
            .collect();
        let intersection = truth.intersection(&result).collect::<Vec<_>>();

        let query_recall = (intersection.len() as f32 / truth.len() as f32) * 100.0;
        recall += query_recall;
    }

    recall = recall / ground_truth.len() as f32;
    latency = latency / ground_truth.len();
    Ok((recall, latency))
}

pub fn autotune_index(args: &cli::IndexAutotuneArgs, logger: Option<Logger>) -> AnyhowVoidResult {
    let logger = logger.unwrap_or(Logger::new("Lantern Index", LogLevel::Debug));

    let uri = append_params_to_uri(&args.uri, CONNECTION_PARAMS);
    let mut client = Client::connect(&uri, NoTls)?;

    let src_table_name = get_full_table_name(&args.schema, &args.table);
    let tmp_table_name = format!("_test_{}", &args.table);
    let tmp_table_full_name = get_full_table_name(INTERNAL_SCHEMA_NAME, &tmp_table_name);
    let truth_table_name =
        get_full_table_name(INTERNAL_SCHEMA_NAME, &format!("_truth_{}", &args.table));

    let column_dims = create_test_table(
        &mut client,
        &tmp_table_full_name,
        &src_table_name,
        &args.column,
        args.test_data_size,
    )?;
    let ground_truth = calculate_ground_truth(
        &mut client,
        &args.pk,
        &args.column,
        &tmp_table_full_name,
        &truth_table_name,
        &args.metric_kind.sql_function(),
        args.k,
    )?;

    let index_variants = vec![
        IndexParams {
            ef: 64,
            ef_construction: 32,
            m: 12,
        }, // fast + low recall
        IndexParams {
            ef: 64,
            ef_construction: 64,
            m: 32,
        }, // medium
        IndexParams {
            ef: 128,
            ef_construction: 128,
            m: 48,
        }, // slow + high recall
    ];

    let mut rng = rand::thread_rng();
    let index_path = format!("/tmp/index-autotune-{}.usearch", rng.gen_range(0..1000));
    let index_name = format!("lantern_autotune_idx_{}", rng.gen_range(0..1000));
    let uuid = Uuid::new_v4().to_string();
    let job_id = args.job_id.as_ref().unwrap_or(&uuid);

    let mut autotune_results: Vec<AutotuneResult> = Vec::with_capacity(index_variants.len());

    for variant in &index_variants {
        client.execute(
            &format!(
                "DROP INDEX IF EXISTS {index_name}",
                index_name = get_full_table_name(INTERNAL_SCHEMA_NAME, &index_name)
            ),
            &[],
        )?;
        let start = Instant::now();
        lantern_create_index::create_usearch_index(
            &CreateIndexArgs {
                import: true,
                out: index_path.clone(),
                table: tmp_table_name.clone(),
                schema: INTERNAL_SCHEMA_NAME.to_owned(),
                metric_kind: args.metric_kind.clone(),
                efc: variant.ef_construction,
                ef: variant.ef,
                m: variant.m,
                uri: uri.clone(),
                column: args.column.clone(),
                dims: column_dims as usize,
                index_name: Some(index_name.clone()),
            },
            Some(Logger::new(&logger.label, LogLevel::Info)),
            None,
        )?;
        let indexing_duration = start.elapsed().as_secs() as usize;
        let (recall, latency) =
            calculate_recall_and_latency(&mut client, &ground_truth, &tmp_table_full_name, args.k)?;
        logger.info(&format!(
            "Variant {:?}, recall: {recall}%, latency: {latency}ms, indexing duration: {indexing_duration}s",
            variant,
        ));
        autotune_results.push(AutotuneResult {
            job_id: job_id.clone(),
            metric_kind: args.metric_kind.sql_function(),
            ef: variant.ef as i32,
            ef_construction: variant.ef_construction as i32,
            m: variant.m as i32,
            k: args.k as i32,
            dim: column_dims as i32,
            sample_size: args.test_data_size as i32,
            recall: recall as f64,
            latency: latency as i32,
            indexing_duration: indexing_duration as i32,
        });
    }

    client.execute(
        &format!("DROP TABLE IF EXISTS {tmp_table_full_name} CASCADE"),
        &[],
    )?;

    if args.export {
        let result_table_name = &args.export_table_name;
        let result_table_full_name =
            get_full_table_name(&args.export_schema_name, &result_table_name);

        let mut export_client = client;
        if let Some(uri) = &args.export_db_uri {
            let uri = append_params_to_uri(&uri, CONNECTION_PARAMS);
            export_client = Client::connect(&uri, NoTls)?;
        }

        create_results_table(&mut export_client, &result_table_full_name)?;
        export_results(
            &mut export_client,
            &result_table_full_name,
            autotune_results.clone(),
        )?;
        logger.debug(&format!(
            "Results for job {job_id} exported to {result_table_name}"
        ));
    }

    if args.create_index {
        let best_result = find_best_variant(&autotune_results, args.recall);
        logger.debug(&format!(
            "Creating index with the best result for job {job_id}"
        ));
        let start = Instant::now();
        lantern_create_index::create_usearch_index(
            &CreateIndexArgs {
                import: true,
                out: index_path.clone(),
                table: args.table.clone(),
                schema: args.schema.clone(),
                metric_kind: args.metric_kind.clone(),
                efc: best_result.ef_construction as usize,
                ef: best_result.ef as usize,
                m: best_result.m as usize,
                uri: uri.clone(),
                column: args.column.clone(),
                dims: column_dims as usize,
                index_name: None,
            },
            Some(Logger::new(&logger.label, LogLevel::Info)),
            None,
        )?;
        let duration = start.elapsed().as_secs();
        logger.debug(&format!("Index for job {job_id} created in {duration}s"));
    }

    Ok(())
}
