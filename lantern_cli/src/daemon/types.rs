use std::collections::HashMap;
use tokio::sync::{
    mpsc::{Sender, UnboundedSender},
    Mutex, RwLock,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct JobRunArgs {
    pub uri: String,
    pub schema: String,
    pub log_level: crate::logger::LogLevel,
    pub table_name: String,
    pub label: Option<String>,
    pub data_path: Option<String>,
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

        let db_host = parts[parts.len() - 1];
        let db_name_index = db_host.find("/");

        if db_name_index.is_none() {
            anyhow::bail!("Can not get database name from specified url");
        }

        let db_name_index = db_name_index.unwrap();

        Ok(TargetDB {
            name: format!("{}{}", &db_host[..5], &db_host[db_name_index..]),
            uri: db_url.to_owned(),
        })
    }
}

#[derive(Debug)]
pub struct JobInsertNotification {
    pub id: i32,
    pub generate_missing: bool,
    pub row_id: Option<String>,
    pub filter: Option<String>,
    #[allow(dead_code)]
    pub limit: Option<u32>,
}

pub struct JobUpdateNotification {
    pub id: i32,
    pub generate_missing: bool,
}

pub type JobTaskEventTx = Sender<JobEvent>;
pub type JobEventHandlersMap = RwLock<HashMap<i32, JobTaskEventTx>>;
pub type JobBatchingHashMap = Mutex<HashMap<i32, Vec<String>>>;
pub type ClientJobsMap = RwLock<HashMap<i32, UnboundedSender<ClientJobSignal>>>;
pub type DaemonJobHandlerMap = RwLock<HashMap<String, CancellationToken>>;

pub enum JobEvent {
    Done,
    Errored(String),
}

pub enum ClientJobSignal {
    Stop,
    Restart,
}

#[cfg(feature = "embeddings")]
pub type EmbeddingProcessorArgs = (
    crate::embeddings::cli::EmbeddingArgs,
    Sender<Result<(usize, usize), anyhow::Error>>,
    crate::logger::Logger,
);
#[cfg(not(feature = "embeddings"))]
pub type EmbeddingProcessorArgs = ();

#[cfg(feature = "autotune")]
pub type AutotuneProcessorArgs = (
    crate::index_autotune::cli::IndexAutotuneArgs,
    Sender<crate::types::AnyhowVoidResult>,
    JobTaskEventTx,
    Option<crate::types::ProgressCbFn>,
    std::sync::Arc<std::sync::RwLock<bool>>,
    crate::logger::Logger,
);
#[cfg(not(feature = "autotune"))]
pub type AutotuneProcessorArgs = ();

#[cfg(feature = "external-index")]
pub type ExternalIndexProcessorArgs = (
    crate::external_index::cli::CreateIndexArgs,
    Sender<crate::types::AnyhowVoidResult>,
    JobTaskEventTx,
    Option<crate::types::ProgressCbFn>,
    std::sync::Arc<std::sync::RwLock<bool>>,
    crate::logger::Logger,
);
#[cfg(not(feature = "external-index"))]
pub type ExternalIndexProcessorArgs = ();

pub enum JobType {
    Embeddings(Sender<EmbeddingProcessorArgs>),
    #[allow(dead_code)]
    ExternalIndex(Sender<ExternalIndexProcessorArgs>),
    #[allow(dead_code)]
    Autotune(Sender<AutotuneProcessorArgs>),
}
