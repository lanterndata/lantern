use clap::{Parser, Subcommand};
use lantern_daemon::cli::DaemonArgs;
use lantern_embeddings::cli::{EmbeddingArgs, MeasureModelSpeedArgs, ShowModelsArgs};
use lantern_external_index::cli::CreateIndexArgs;
use lantern_index_autotune::cli::IndexAutotuneArgs;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Create external index
    CreateIndex(CreateIndexArgs),
    /// Create embeddings
    CreateEmbeddings(EmbeddingArgs),
    /// Show embedding models
    ShowRuntimes,
    /// Show embedding models
    ShowModels(ShowModelsArgs),
    /// Measure embedding geneartion speed
    MeasureModelSpeed(MeasureModelSpeedArgs),
    /// Autotune index
    AutotuneIndex(IndexAutotuneArgs),
    /// Start in daemon mode
    StartDaemon(DaemonArgs),
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
