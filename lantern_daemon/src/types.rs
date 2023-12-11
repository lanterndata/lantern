use futures::Future;
use std::pin::Pin;
use tokio_postgres::Row;

#[derive(Debug)]
pub struct EmbeddingJob {
    pub id: i32,
    pub is_init: bool,
    pub db_uri: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub filter: Option<String>,
    pub out_column: String,
    pub model: String,
    pub batch_size: Option<usize>,
}

impl EmbeddingJob {
    pub fn new(row: Row) -> EmbeddingJob {
        Self {
            id: row.get::<&str, i32>("id"),
            db_uri: row.get::<&str, String>("db_uri"),
            schema: row.get::<&str, String>("schema"),
            table: row.get::<&str, String>("table"),
            column: row.get::<&str, String>("column"),
            out_column: row.get::<&str, String>("dst_column"),
            model: row.get::<&str, String>("model"),
            filter: None,
            is_init: true,
            batch_size: None,
        }
    }

    pub fn set_filter(&mut self, filter: &str) {
        self.filter = Some(filter.to_owned());
    }

    pub fn set_is_init(&mut self, is_init: bool) {
        self.is_init = is_init;
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
    pub recall: usize,
    pub k: u16,
    pub create_index: bool,
}

impl AutotuneJob {
    pub fn new(row: Row) -> AutotuneJob {
        Self {
            id: row.get::<&str, i32>("id"),
            db_uri: row.get::<&str, String>("db_uri"),
            schema: row.get::<&str, String>("schema"),
            table: row.get::<&str, String>("table"),
            column: row.get::<&str, String>("column"),
            metric_kind: row.get::<&str, String>("metric_kind"),
            model_name: row.get::<&str, Option<String>>("model"),
            recall: row.get::<&str, i32>("target_recall") as usize,
            k: row.get::<&str, i32>("k") as u16,
            create_index: row.get::<&str, bool>("create_index"),
            is_init: true,
        }
    }

    pub fn set_is_init(&mut self, is_init: bool) {
        self.is_init = is_init;
    }
}

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

pub type AnyhowVoidResult = Result<(), anyhow::Error>;
pub type VoidFuture = Pin<Box<dyn Future<Output = AnyhowVoidResult>>>;
