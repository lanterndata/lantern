use super::cli::PQArgs;
use super::setup::{make_codebook_logged_and_readonly, setup_tables, setup_triggers};
use super::{set_and_report_progress, AnyhowVoidResult, ProgressCbFn};
use crate::logger::Logger;
use crate::utils::quote_ident;
use isahc::{prelude::*, HttpClient, Request};
use postgres::{Client, NoTls};
use rand::Rng;
use serde::Deserialize;
use serde_json::{self, json, Value};
use std::cmp;
use std::sync::atomic::AtomicU8;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

static CLUSTERING_TASK_TEMPLATE: &'static str = r#"{
   "taskGroups": [{
     "taskSpec": {
      "runnables": [
       {
         "container": {
           "imageUri": "{gcp_image}",
           "enableImageStreaming": false,
           "entrypoint": "/bin/sh",
           "commands": [
             "-c",
             "/lantern-cli pq-table --uri ${DB_URI} --table ${TABLE} --column ${COLUMN} --clusters ${CLUSTERS} --splits ${SPLITS} --parallel-task-count ${PARALLEL_TASK_COUNT} --dataset-size ${DATASET_SIZE} --dataset-limit ${DATASET_LIMIT} --start-offset-id ${START_OFFSET_ID} --subvector-id ${BATCH_TASK_INDEX} --skip-table-setup --skip-vector-quantization; exit $?"
           ]
         },
         "environment": {
           "variables": {
             "DB_URI": "{db_uri}",
             "TABLE": "{table_name}",
             "COLUMN": "{column}",
             "CLUSTERS": "{cluster_count}",
             "SPLITS": "{splits}",
             "PARALLEL_TASK_COUNT": "{gcp_quantization_task_parallelism}",
             "DATASET_SIZE": "{dataset_size}",
             "DATASET_LIMIT": "{dataset_limit}",
             "START_OFFSET_ID": "{start_offset_id}"
           }
         }
       }
      ],
      "computeResource": {
        "cpuMilli": 0,
        "memoryMib": 0
      },
      "maxRetryCount": 1,
      "maxRunDuration": "2000s"
     },
    "taskCount": "{splits}",
    "taskCountPerNode": 1,
    "parallelism": "{gcp_clustering_task_parallelism}"
   }],
   "logsPolicy": {
     "destination": "CLOUD_LOGGING"
   },
   "allocationPolicy": {
     "instances": [
       {
         "policy": { "machineType": "{machine_type}" }
       }
     ]
   }
}"#;

static QUANTIZATION_TASK_TEMPLATE: &'static str = r#"{
   "taskGroups": [{
     "taskSpec": {
      "runnables": [
       {
         "container": {
           "imageUri": "{gcp_image}",
           "enableImageStreaming": false,
           "entrypoint": "/bin/sh",
           "commands": [
             "-c",
             "/lantern-cli pq-table --uri ${DB_URI} --table ${TABLE} --column ${COLUMN} --clusters ${CLUSTERS} --splits ${SPLITS} --dataset-size ${DATASET_SIZE} --skip-table-setup --skip-codebook-creation --total-task-count ${QUANTIZATION_TASK_COUNT} --parallel-task-count ${PARALLEL_TASK_COUNT} --quantization-task-id ${BATCH_TASK_INDEX}; exit $?"
           ]
         },
         "environment": {
           "variables": {
             "DB_URI": "{db_uri}",
             "TABLE": "{table_name}",
             "COLUMN": "{column}",
             "CLUSTERS": "{cluster_count}",
             "SPLITS": "{splits}",
             "DATASET_SIZE": "{dataset_size}",
             "QUANTIZATION_TASK_COUNT": "{gcp_quantization_task_count}",
             "PARALLEL_TASK_COUNT": "{gcp_quantization_task_parallelism}"
           }
         }
       }
      ],
      "computeResource": {
        "cpuMilli": 0,
        "memoryMib": 0
      },
      "maxRetryCount": 1,
      "maxRunDuration": "3000s"
     },
     "taskCount": "{gcp_quantization_task_count}",
     "taskCountPerNode": 1,
     "parallelism": "{gcp_quantization_task_parallelism}"
   }],
   "logsPolicy": {
     "destination": "CLOUD_LOGGING"
   },
   "allocationPolicy": {
     "instances": [
       {
         "policy": { "machineType": "{machine_type}" }
       }
     ]
   }
}"#;

#[derive(Debug, Deserialize)]
struct JobStatusEvent {
    description: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStatus {
    state: String,
    status_events: Option<Vec<JobStatusEvent>>,
}

#[derive(Debug, Deserialize)]
struct BatchJobResponse {
    name: Option<String>,
    status: JobStatus,
}

fn run_batch_job(logger: &Logger, task_body: &str, parent: &str) -> AnyhowVoidResult {
    let url = format!("https://batch.googleapis.com/v1/{parent}/jobs");
    let runtime = Runtime::new()?;
    let authentication_manager = runtime.block_on(gcp_auth::AuthenticationManager::new())?;
    let token = runtime.block_on(
        authentication_manager.get_token(&["https://www.googleapis.com/auth/cloud-platform"]),
    )?;
    let token_str = token.as_str();

    let response = Request::post(url)
        .header("Authorization", &format!("Bearer {token_str}"))
        .header("Content-Type", "application/json")
        .body(task_body)?
        .send()?
        .bytes()?;

    let result: Result<BatchJobResponse, serde_json::Error> = serde_json::from_slice(&response);
    if let Err(e) = result {
        anyhow::bail!(
            "Error: {e}. GCP response: {:?}",
            serde_json::from_slice::<serde_json::Value>(&response)?
        );
    }

    let result = result.unwrap();

    if result.name.is_none() {
        anyhow::bail!("Invalid job response: {:?}", result);
    }

    let job_name = result.name.unwrap();
    let job_url = format!("https://batch.googleapis.com/v1alpha/{}", job_name);

    logger.info(&format!("Job {} created. Waiting to succeed", job_name));
    loop {
        let token = runtime.block_on(
            authentication_manager.get_token(&["https://www.googleapis.com/auth/cloud-platform"]),
        )?;
        let token_str = token.as_str();

        let http_client = HttpClient::builder()
            .default_header("Authorization", &format!("Bearer {token_str}"))
            .default_header("Content-Type", "application/json")
            .build()?;

        let mut response = http_client.get(&job_url)?;
        let result: Result<BatchJobResponse, serde_json::Error> =
            serde_json::from_slice(&response.bytes()?);
        if let Err(e) = result {
            anyhow::bail!("Error: {e}. GCP response: {:?}", response.text()?);
        }

        let job = result.unwrap();

        match job.status.state.as_str() {
            "FAILED" => {
                let mut descrption = "None";

                if let Some(status_events) = &job.status.status_events {
                    if status_events.len() > 0 {
                        descrption = &status_events.last().as_ref().unwrap().description;
                    }
                }
                anyhow::bail!(
                    "Job: {} failed. Last event description: {}",
                    job_name,
                    descrption
                )
            }
            "SUCCEEDED" => break,
            _ => (),
        }
        logger.debug(&format!("Job state is: {}", job.status.state));
        std::thread::sleep(Duration::from_secs(60));
    }
    Ok(())
}

pub fn quantize_table_on_gcp(
    args: PQArgs,
    main_progress: AtomicU8,
    db_uri: &str,
    full_table_name: &str,
    full_codebook_table_name: &str,
    pq_column_name: &str,
    progress_cb: Option<ProgressCbFn>,
    logger: &Logger,
) -> AnyhowVoidResult {
    // Validate required arguments
    let gcp_project_id = match &args.gcp_project {
        Some(project_id) => project_id,
        None => anyhow::bail!("Argument --gcp-project is required"),
    };
    let gcp_region = args.gcp_region.unwrap_or("us-central1".to_owned());
    let gcp_cli_image_tag = args.gcp_cli_image_tag.unwrap_or("0.0.39-cpu".to_owned());
    let gcp_image = args.gcp_image.unwrap_or(
        format!(
            "{gcp_region}-docker.pkg.dev/{gcp_project_id}/lanterndata/lantern-cli:{gcp_cli_image_tag}"
        )
        .to_owned(),
    );

    let mut db_client = Client::connect(&db_uri, NoTls)?;
    let mut transaction = db_client.transaction()?;

    let max_connections = transaction.query_one(
        "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",
        &[],
    )?;
    let max_connections = max_connections.get::<usize, i32>(0) as usize;

    let total_row_count = match args.dataset_size {
        Some(row_count) => row_count,
        None => {
            let total_row_count_query = transaction.query_one(
                &format!(
                    "SELECT COUNT({pk}) FROM {full_table_name};",
                    pk = quote_ident(&args.pk)
                ),
                &[],
            )?;
            total_row_count_query.try_get::<usize, i64>(0)? as usize
        }
    };

    let gcp_quantization_cpu_count = args.gcp_quantization_cpu.unwrap_or(4);
    let gcp_quantization_memory_gb = args
        .gcp_quantization_memory_gb
        .unwrap_or((gcp_quantization_cpu_count as f64 * 3.75) as usize);

    let gcp_clustering_cpu_count = args.gcp_quantization_cpu.unwrap_or_else(|| {
        if total_row_count < 100_000 {
            8
        } else if total_row_count < 1_000_000 {
            16
        } else if total_row_count < 5_000_000 {
            32
        } else if total_row_count < 10_000_000 {
            64
        } else {
            96
        }
    });

    // Mem / CPU rotio taken from GCP
    let gcp_clustering_memory_gb = args
        .gcp_clustering_memory_gb
        .unwrap_or((gcp_clustering_cpu_count as f64 * 3.75) as usize);

    // Let each vm process max 50k rows
    let gcp_quantization_task_count = args
        .gcp_quantization_task_count
        .unwrap_or(cmp::max(total_row_count / 100000, 1));

    // Limit parallel task count to not exceed max connection limit
    let gcp_quantization_task_parallelism = args
        .gcp_quantization_task_parallelism
        .unwrap_or(cmp::max(1, max_connections / gcp_quantization_cpu_count));

    let gcp_quantization_task_parallelism = cmp::min(
        gcp_quantization_task_parallelism,
        gcp_quantization_task_count,
    );

    // Limit parallel task count to not exceed max connection limit
    let gcp_clustering_task_parallelism = args.gcp_clustering_task_parallelism.unwrap_or(cmp::min(
        args.splits,
        cmp::max(1, max_connections / args.splits),
    ));

    // Create codebook table and add pqvec column to table
    if !args.skip_table_setup {
        setup_tables(
            &mut transaction,
            &full_table_name,
            &full_codebook_table_name,
            &pq_column_name,
            args.overwrite,
            &logger,
        )?;

        setup_triggers(
            &mut transaction,
            &full_table_name,
            &full_codebook_table_name,
            &pq_column_name,
            &args.column,
            "l2sq",
            args.splits,
        )?;

        // Creating new transaction, because  current transaction will lock table reads
        // and block the process
        transaction.commit()?;
        transaction = db_client.transaction()?;
        set_and_report_progress(&progress_cb, &logger, &main_progress, 5);
    }

    if !args.skip_codebook_creation {
        let task_start = Instant::now();

        let limit = if let Some(limit) = args.dataset_limit {
            limit
        } else {
            0
        };

        if limit > 0 && limit < args.clusters {
            anyhow::bail!("--dataset-limit should be greater than or equal to cluster count");
        }

        let start_offset_id = if limit > 0 {
            let mut rng = rand::thread_rng();
            let max_id = if limit > total_row_count {
                0
            } else {
                total_row_count - limit
            };

            // Generate random offset to take portion of dataset
            // We are not doing order by random() limit X, because it is slow, and chunking based on id
            // will become harder
            rng.gen_range(0..max_id)
        } else {
            0
        };

        let mut body_json: Value = serde_json::from_str(CLUSTERING_TASK_TEMPLATE)?;
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["imageUri"] =
            json!(gcp_image);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
            ["enableImageStreaming"] = json!(args.gcp_enable_image_streaming);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["DB_URI"] = json!(args.uri);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["TABLE"] = json!(args.table);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["COLUMN"] = json!(args.column);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["CLUSTERS"] = json!(args.clusters.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["SPLITS"] = json!(args.splits.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["START_OFFSET_ID"] = json!(start_offset_id.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["DATASET_SIZE"] = json!(total_row_count.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["DATASET_LIMIT"] = json!(args
            .dataset_limit
            .clone()
            .unwrap_or(total_row_count)
            .to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["PARALLEL_TASK_COUNT"] = json!(gcp_clustering_task_parallelism.to_string());
        body_json["taskGroups"][0]["taskSpec"]["computeResource"]["cpuMilli"] =
            json!(gcp_clustering_cpu_count * 1000);
        body_json["taskGroups"][0]["taskSpec"]["computeResource"]["memoryMib"] =
            json!(gcp_clustering_memory_gb * 1000);
        body_json["taskGroups"][0]["taskCount"] = json!(args.splits);
        body_json["taskGroups"][0]["parallelism"] = json!(gcp_clustering_task_parallelism);
        body_json["allocationPolicy"]["instances"][0]["policy"]["machineType"] =
            json!(format!("n1-standard-{gcp_clustering_cpu_count}"));

        run_batch_job(
            &logger,
            &body_json.to_string(),
            &format!("projects/{gcp_project_id}/locations/{gcp_region}"),
        )?;
        logger.debug(&format!(
            "Clustering duration: {}s",
            task_start.elapsed().as_secs()
        ));
        make_codebook_logged_and_readonly(&mut transaction, &full_codebook_table_name)?;
        transaction.commit()?;
        set_and_report_progress(&progress_cb, &logger, &main_progress, 90);
    }

    if !args.skip_vector_quantization {
        let task_start = Instant::now();
        let mut body_json: Value = serde_json::from_str(QUANTIZATION_TASK_TEMPLATE)?;
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["imageUri"] =
            json!(gcp_image);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
            ["enableImageStreaming"] = json!(args.gcp_enable_image_streaming);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["DB_URI"] = json!(args.uri);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["TABLE"] = json!(args.table);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["COLUMN"] = json!(args.column);
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["CLUSTERS"] = json!(args.clusters.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["SPLITS"] = json!(args.splits.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["DATASET_SIZE"] = json!(total_row_count.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["QUANTIZATION_TASK_COUNT"] = json!(gcp_quantization_task_count.to_string());
        body_json["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"]
            ["PARALLEL_TASK_COUNT"] = json!(gcp_quantization_task_parallelism.to_string());
        body_json["taskGroups"][0]["taskSpec"]["computeResource"]["cpuMilli"] =
            json!(gcp_quantization_cpu_count * 1000);
        body_json["taskGroups"][0]["taskSpec"]["computeResource"]["memoryMib"] =
            json!(gcp_quantization_memory_gb * 1000);
        body_json["taskGroups"][0]["taskCount"] = json!(gcp_quantization_task_count);
        body_json["taskGroups"][0]["parallelism"] = json!(gcp_quantization_task_parallelism);
        body_json["allocationPolicy"]["instances"][0]["policy"]["machineType"] =
            json!(format!("n1-standard-{gcp_quantization_cpu_count}"));

        run_batch_job(
            &logger,
            &body_json.to_string(),
            &format!("projects/{gcp_project_id}/locations/{gcp_region}"),
        )?;
        logger.debug(&format!(
            "quantization duration: {}s",
            task_start.elapsed().as_secs()
        ));
    }
    set_and_report_progress(&progress_cb, &logger, &main_progress, 100);
    Ok(())
}
