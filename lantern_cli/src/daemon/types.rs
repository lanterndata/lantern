use crate::embeddings::cli::Runtime;
use crate::utils::get_common_embedding_ignore_filters;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::{mpsc::Sender, Mutex, RwLock};
use tokio_postgres::Row;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct JobRunArgs {
    pub uri: String,
    pub schema: String,
    pub log_level: crate::logger::LogLevel,
    pub table_name: String,
}

#[derive(Clone, Debug)]
pub struct TargetDB {
    pub uri: String,
    pub name: String,
}

impl TargetDB {
    pub fn from_uri(db_url: &str) -> Result<TargetDB, anyhow::Error> {
        let parts: Vec<&str> = db_url.split('@').collect();
        if parts.len() < 2 {
            anyhow::bail!("Invalid format for --target-db, should be 'postgres://username:[password]@host/db'")
        }

        Ok(TargetDB {
            name: parts[parts.len() - 1].to_owned(),
            uri: db_url.to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddingJob {
    pub id: i32,
    pub is_init: bool,
    pub db_uri: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub pk: String,
    pub filter: Option<String>,
    pub out_column: String,
    pub model: String,
    pub runtime_params: String,
    pub runtime: Runtime,
    pub batch_size: Option<usize>,
    pub row_ids: Option<Vec<String>>,
    pub report_progress: Option<u8>,
    pub is_last_chunk: bool,
}

impl EmbeddingJob {
    pub fn new(row: Row, data_path: &str, db_uri: &str) -> Result<EmbeddingJob, anyhow::Error> {
        let runtime = Runtime::try_from(row.get::<&str, Option<&str>>("runtime").unwrap_or("ort"))?;
        let runtime_params = if runtime == Runtime::Ort {
            format!(r#"{{ "data_path": "{data_path}" }}"#)
        } else {
            row.get::<&str, Option<String>>("runtime_params")
                .unwrap_or("{}".to_owned())
        };

        Ok(Self {
            id: row.get::<&str, i32>("id"),
            pk: "id".to_owned(), // TODO:: row.get::<&str, String>("pk"),
            db_uri: db_uri.to_owned(),
            schema: row.get::<&str, String>("schema"),
            table: row.get::<&str, String>("table"),
            column: row.get::<&str, String>("column"),
            out_column: row.get::<&str, String>("dst_column"),
            model: row.get::<&str, String>("model"),
            runtime,
            runtime_params,
            filter: None,
            row_ids: None,
            is_init: true,
            batch_size: None,
            report_progress: None,
            is_last_chunk: false,
        })
    }

    pub fn set_filter(&mut self, filter: &str) {
        self.filter = Some(filter.to_owned());
    }

    pub fn set_is_init(&mut self, is_init: bool) {
        self.is_init = is_init;
    }

    pub fn set_row_ids(&mut self, row_ids: Vec<String>) {
        self.row_ids = Some(row_ids);
    }

    pub fn set_report_progress(&mut self, progress: u8) {
        self.report_progress = Some(progress);
    }

    pub fn set_is_last_chunk(&mut self, status: bool) {
        self.is_last_chunk = status;
    }

    #[allow(dead_code)]
    pub fn set_ctid_filter(&mut self, row_ids: &Vec<String>) {
        let row_ctids_str = row_ids
            .iter()
            .map(|r| {
                format!(
                    "currtid2('{table_name}','{r}'::tid)",
                    table_name = &self.table
                )
            })
            .join(",");
        self.set_filter(&format!("ctid IN ({row_ctids_str})"));
    }

    pub fn set_id_filter(&mut self, row_ids: &Vec<String>) {
        let row_ctids_str = row_ids.iter().join(",");
        self.set_filter(&format!(
            "id IN ({row_ctids_str}) AND {common_filter}",
            common_filter = get_common_embedding_ignore_filters(&self.column)
        ));
    }
}

#[derive(Debug)]
pub struct AutotuneJob {
    pub id: i32,
    pub is_init: bool,
    pub db_uri: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub metric_kind: String,
    pub model_name: Option<String>,
    pub recall: f64,
    pub k: u16,
    pub sample_size: usize,
    pub create_index: bool,
}

impl AutotuneJob {
    pub fn new(row: Row, db_uri: &str) -> AutotuneJob {
        Self {
            id: row.get::<&str, i32>("id"),
            db_uri: db_uri.to_owned(),
            schema: row.get::<&str, String>("schema"),
            table: row.get::<&str, String>("table"),
            column: row.get::<&str, String>("column"),
            metric_kind: row.get::<&str, String>("metric_kind"),
            model_name: row.get::<&str, Option<String>>("model"),
            recall: row.get::<&str, f64>("target_recall"),
            k: row.get::<&str, i32>("k") as u16,
            sample_size: row.get::<&str, i32>("sample_size") as usize,
            create_index: row.get::<&str, bool>("create_index"),
            is_init: true,
        }
    }
}

#[derive(Debug)]
pub struct ExternalIndexJob {
    pub id: i32,
    pub db_uri: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub operator_class: String,
    pub index_name: Option<String>,
    pub ef: usize,
    pub efc: usize,
    pub m: usize,
}

impl ExternalIndexJob {
    pub fn new(row: Row, db_uri: &str) -> ExternalIndexJob {
        Self {
            id: row.get::<&str, i32>("id"),
            db_uri: db_uri.to_owned(),
            schema: row.get::<&str, String>("schema"),
            table: row.get::<&str, String>("table"),
            column: row.get::<&str, String>("column"),
            operator_class: row.get::<&str, String>("operator"),
            index_name: row.get::<&str, Option<String>>("index"),
            ef: row.get::<&str, i32>("ef") as usize,
            efc: row.get::<&str, i32>("efc") as usize,
            m: row.get::<&str, i32>("m") as usize,
        }
    }
}

#[derive(Debug)]
pub struct JobInsertNotification {
    pub id: i32,
    pub init: bool,
    pub generate_missing: bool,
    pub row_id: Option<String>,
    pub filter: Option<String>,
    pub limit: Option<u32>,
}

pub struct JobUpdateNotification {
    pub id: i32,
    pub generate_missing: bool,
}

pub type JobTaskCancelTx = Sender<String>;
pub type JobCancellationHandlersMap = RwLock<HashMap<i32, JobTaskCancelTx>>;
pub type JobBatchingHashMap = Mutex<HashMap<i32, Vec<String>>>;
pub type DaemonJobHandlerMap = RwLock<HashMap<String, CancellationToken>>;
