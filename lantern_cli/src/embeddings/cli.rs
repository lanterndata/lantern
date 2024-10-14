pub use super::core::Runtime;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct EmbeddingArgs {
    /// Model name
    #[arg(short, long)]
    pub model: String,

    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Column name to generate embeddings for
    #[arg(short, long)]
    pub column: String,

    /// Runtime Params JSON string
    #[arg(long, default_value = "id")]
    pub pk: String,

    /// Output db uri, fully associated database connection string including db name. Defaults to
    #[arg(long)]
    pub out_uri: Option<String>,

    /// Output table name. Defaults to table
    #[arg(long)]
    pub out_table: Option<String>,

    /// Output column name
    #[arg(long)]
    pub out_column: String,

    /// Batch size
    #[arg(short, long)]
    pub batch_size: Option<usize>,

    /// Runtime
    #[arg(long, default_value_t = Runtime::Ort)]
    pub runtime: Runtime,

    /// Runtime Params JSON string
    #[arg(long, default_value = "{}")]
    pub runtime_params: String,

    /// If model is visual
    #[arg(long, default_value_t = false)]
    pub visual: bool,

    /// Filter which will be used when getting data from source table
    #[arg(short, long)]
    pub filter: Option<String>,

    /// Limit will be applied to source table if specified
    #[arg(short, long)]
    pub limit: Option<u32>,

    /// Stream data to output table while still generating
    #[arg(long, default_value_t = false)]
    pub stream: bool,

    /// Create destination column if not exists
    #[arg(long, default_value_t = false)]
    pub create_column: bool,
}

impl EmbeddingArgs {
    pub fn with_defaults(self) -> Self {
        EmbeddingArgs {
            out_uri: self.out_uri.or(Some(self.uri.clone())),
            out_table: self.out_table.or(Some(self.table.clone())),
            ..self
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ShowModelsArgs {
    /// Data path
    #[arg(short, long)]
    pub data_path: Option<String>,
    /// Runtime
    #[arg(long, default_value_t = Runtime::Ort)]
    pub runtime: Runtime,

    /// Runtime Params JSON string
    #[arg(long, default_value = "{}")]
    pub runtime_params: String,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct MeasureModelSpeedArgs {
    /// Model name (if not passed all models will be tested)
    #[arg(short, long)]
    pub model: Option<String>,

    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Initial limit size for tests
    #[arg(short, long, default_value_t = 500)]
    pub initial_limit: u32,

    /// Batch size
    #[arg(short, long)]
    pub batch_size: Option<usize>,

    /// Maximum tokens for large text
    #[arg(long, default_value_t = 1000)]
    pub max_tokens: usize,

    /// Runtime
    #[arg(long, default_value_t = Runtime::Ort)]
    pub runtime: Runtime,

    /// Runtime Params JSON string
    #[arg(long, default_value = "{}")]
    pub runtime_params: String,
}
