use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum LogLevel {
    Info,
    Debug,
    Warn,
    Error,
}

impl LogLevel {
    pub fn value(&self) -> lantern_logger::LogLevel {
        match self {
            LogLevel::Info => {
                return lantern_logger::LogLevel::Info;
            }
            LogLevel::Debug => {
                return lantern_logger::LogLevel::Debug;
            }
            LogLevel::Warn => {
                return lantern_logger::LogLevel::Warn;
            }
            LogLevel::Error => {
                return lantern_logger::LogLevel::Error;
            }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct DaemonArgs {
    /// Fully associated database connection string including db name to get jobs
    #[arg(short, long)]
    pub uri: String,

    /// Embedding jobs table name
    #[arg(long)]
    pub embedding_table: Option<String>,

    /// Autotune jobs table name
    #[arg(long)]
    pub autotune_table: Option<String>,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Internal schema name to create required tables
    #[arg(short, long, default_value = "lantern")]
    pub internal_schema: String,

    /// Max concurrent jobs
    #[arg(short, long, default_value_t = 1)]
    pub queue_size: usize,

    /// Log level
    #[arg(long, value_enum, default_value_t = LogLevel::Info)] // arg_enum here
    pub log_level: LogLevel,
}
