use clap::{Parser, Subcommand};
use lantern_create_index::cli::CreateIndexArgs;
use lantern_embeddings::cli::{EmbeddingArgs, ShowModelsArgs};

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Create external index
    CreateIndex(CreateIndexArgs),
    /// Create embeddings
    CreateEmbeddings(EmbeddingArgs),
    /// Show embedding models
    ShowModels(ShowModelsArgs),
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
