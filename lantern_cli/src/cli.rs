use clap::{Parser, Subcommand};
use lantern_cli::daemon::cli::DaemonArgs;
use lantern_cli::embeddings::cli::{EmbeddingArgs, MeasureModelSpeedArgs, ShowModelsArgs};
use lantern_cli::external_index::cli::CreateIndexArgs;
use lantern_cli::external_index::cli::IndexServerArgs;
use lantern_cli::http_server::cli::HttpServerArgs;
use lantern_cli::index_autotune::cli::IndexAutotuneArgs;
use lantern_cli::pq::cli::PQArgs;

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
    /// Quantize table
    PQTable(PQArgs),
    /// Start in daemon mode
    StartDaemon(DaemonArgs),
    /// Start in http mode
    StartServer(HttpServerArgs),
    /// Start external index server
    StartIndexingServer(IndexServerArgs),
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}
