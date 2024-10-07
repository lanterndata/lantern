use crate::logger;
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum LogLevel {
    Info,
    Debug,
    Warn,
    Error,
}

impl LogLevel {
    pub fn value(&self) -> logger::LogLevel {
        match self {
            LogLevel::Info => {
                return logger::LogLevel::Info;
            }
            LogLevel::Debug => {
                return logger::LogLevel::Debug;
            }
            LogLevel::Warn => {
                return logger::LogLevel::Warn;
            }
            LogLevel::Error => {
                return logger::LogLevel::Error;
            }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct DaemonArgs {
    /// Routing database to take new client databases to connect
    #[arg(long)]
    pub master_db: Option<String>,

    /// Schema where --databases-table is located
    #[arg(long, default_value = "public")]
    pub master_db_schema: String,

    /// Table on master database which contains target databases
    #[arg(long, default_value = "daemon_databases")]
    pub databases_table: String,

    /// List of target databases to connect and listen for jobs
    #[arg(long, value_delimiter = ' ', num_args = 1..)]
    pub target_db: Option<Vec<String>>,

    /// Enable Embedding jobs
    #[arg(long, default_value_t = false)]
    pub embeddings: bool,

    /// Enable Autotune jobs
    #[arg(long, default_value_t = false)]
    pub autotune: bool,

    /// Enable External Index jobs
    #[arg(long, default_value_t = false)]
    pub external_index: bool,

    /// Schema name
    #[arg(short, long, default_value = "_lantern_extras_internal")]
    pub schema: String,

    /// Label which will be matched against embedding job label
    #[arg(long)]
    pub label: Option<String>,

    /// Data path
    #[arg(long)]
    pub data_path: Option<String>,

    /// Log level
    #[arg(long, value_enum, default_value_t = LogLevel::Info)] // arg_enum here
    pub log_level: LogLevel,
}
