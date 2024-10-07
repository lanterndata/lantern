use super::daemon::cli::DaemonArgs;
use super::embeddings::cli::{EmbeddingArgs, MeasureModelSpeedArgs, ShowModelsArgs};
use super::external_index::cli::CreateIndexArgs;
use super::http_server::cli::HttpServerArgs;
use super::index_autotune::cli::IndexAutotuneArgs;
use super::pq::cli::PQArgs;
use clap::{Parser, Subcommand};
use lantern_cli::external_index::cli::IndexServerArgs;

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
