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

    /// Table primary key column name
    #[arg(short, long, default_value = "id")]
    pub pk: String,

    /// Column name to generate embeddings for
    #[arg(short, long)]
    pub column: String,

    /// Output db uri, fully associated database connection string including db name. Defaults to
    /// uri
    #[arg(long)]
    pub out_uri: Option<String>,

    /// Output table name. Defaults to table
    #[arg(long)]
    pub out_table: Option<String>,

    /// Output column name
    #[arg(long)]
    pub out_column: String,

    /// Batch size
    #[arg(short, long, default_value_t = 200)]
    pub batch_size: usize,

    /// Data path
    #[arg(short, long)]
    pub data_path: Option<String>,

    /// If model is visual
    #[arg(long, default_value_t = false)]
    pub visual: bool,

    /// Output csv path. If specified result will be written in csv instead of database
    #[arg(short, long)]
    pub out_csv: Option<String>,
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
}
