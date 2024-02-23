use crate::lantern_external_index::cli::UMetricKind;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct IndexAutotuneArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Column name
    #[arg(short, long)]
    pub column: String,

    /// Target recall
    #[arg(long, default_value_t = 99.9)]
    pub recall: f64,

    /// K limit of elements for query
    #[arg(long, default_value_t = 10)]
    pub k: u16,

    /// Test data size
    #[arg(long, default_value_t = 10000)]
    pub test_data_size: usize,

    /// Distance algorithm
    #[arg(long, value_enum, default_value_t = UMetricKind::L2sq)]
    pub metric_kind: UMetricKind,

    /// Create index with the best result
    #[arg(long, default_value_t = false)]
    pub create_index: bool,

    /// Job ID to use when exporting results, if not provided UUID will be generated
    #[arg(long)]
    pub job_id: Option<i32>,

    /// Database URL for exporting results, if not specified the --uri will be used
    #[arg(long)]
    pub export_db_uri: Option<String>,

    /// Schame name in which the export table is created
    #[arg(long, default_value = "public")]
    pub export_schema_name: String,

    /// Table name to export results, table should exist
    #[arg(long)]
    pub export_table_name: Option<String>,

    /// Schame name in which the jobs table is created
    #[arg(long, default_value = "public")]
    pub job_schema_name: String,

    /// Table name of autotune jobs
    #[arg(long)]
    pub job_table_name: Option<String>,

    /// Model name to save in results
    #[arg(long)]
    pub model_name: Option<String>,
}
