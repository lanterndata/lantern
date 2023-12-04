use clap::Parser;
use lantern_create_index::cli::UMetricKind;

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

    /// Primary key name
    #[arg(long)]
    pub pk: String,

    /// Target recall
    #[arg(long, default_value_t = 98)]
    pub recall: u64,

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

    /// Export results to table
    #[arg(long, default_value_t = false)]
    pub export: bool,

    /// Job ID to use when exporting results, if not provided UUID will be generated
    #[arg(long)]
    pub job_id: Option<String>,

    /// Database URL for exporting results, if not specified the --uri will be used
    #[arg(long)]
    pub export_db_uri: Option<String>,

    /// Schame name in which the export table will be created
    #[arg(long, default_value = "public")]
    pub export_schema_name: String,

    /// Table name to export results, table will be created if not exists
    #[arg(long, default_value = "lantern_autotune_results")]
    pub export_table_name: String,
}
