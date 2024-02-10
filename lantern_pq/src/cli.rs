use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct PQArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub uri: String,

    /// Table name
    #[arg(short, long)]
    pub table: String,

    /// Schema name
    #[arg(short, long, default_value = "public")]
    pub schema: String,

    /// Column name to quantize
    #[arg(short, long)]
    pub column: String,

    /// Output db uri, fully associated database connection string including db name. Defaults to
    #[arg(long)]
    pub out_uri: Option<String>,

    /// Output table name. Defaults to table
    #[arg(long)]
    pub out_table: Option<String>,

    /// Output column name
    #[arg(long)]
    pub out_column: Option<String>,

    /// Name for codebook table
    #[arg(long)]
    pub codebook_table_name: Option<String>,

    /// Stream data to output table while still generating
    #[arg(long, default_value_t = 256)]
    pub clusters: usize,

    /// Stream data to output table while still generating
    #[arg(long, default_value_t = 1)]
    pub splits: usize,
}

impl PQArgs {
    pub fn with_defaults(self) -> Self {
        PQArgs {
            out_uri: self.out_uri.or(Some(self.uri.clone())),
            out_table: self.out_table.or(Some(self.table.clone())),
            out_column: self.out_column.or(Some(format!("{}_pq", self.column))),
            codebook_table_name: self
                .codebook_table_name
                .or(Some(format!("_lantern_codebook_{}", self.table))),
            ..self
        }
    }
}
