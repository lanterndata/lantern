use std::process;

use crate::logger::{LogLevel, Logger};
use clap::Parser;
use lantern_cli::*;
mod cli;

#[cfg(feature = "cli")]
fn main() {
    let cli = cli::Cli::parse();
    let mut _main_logger = None;
    let res = match cli.command {
        cli::Commands::CreateIndex(args) => {
            let logger = Logger::new("Lantern Index", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            external_index::create_usearch_index(&args, None, None, Some(logger))
        }
        cli::Commands::CreateEmbeddings(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            let res = embeddings::create_embeddings_from_db(args, true, None, None, Some(logger));
            // Handle error here as this call does not return void as others
            let logger = _main_logger.as_ref().unwrap();
            if let Err(e) = res {
                logger.error(&e.to_string());
            }
            Ok(())
        }
        cli::Commands::ShowModels(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            embeddings::show_available_models(&args, Some(logger))
        }
        cli::Commands::ShowRuntimes => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            embeddings::show_available_runtimes(Some(logger))
        }
        cli::Commands::MeasureModelSpeed(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Info);
            _main_logger = Some(logger.clone());
            embeddings::measure_speed::start_speed_test(&args, Some(logger))
        }
        cli::Commands::AutotuneIndex(args) => {
            let logger = Logger::new("Lantern Index Autotune", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            index_autotune::autotune_index(&args, None, None, Some(logger))
        }
        cli::Commands::PQTable(args) => {
            let logger = Logger::new("Lantern PQ", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            pq::quantize_table(args, None, None, Some(logger))
        }
        cli::Commands::StartDaemon(args) => {
            let logger = Logger::new("Lantern Daemon", args.log_level.value());
            _main_logger = Some(logger.clone());
            daemon::start(args, Some(logger))
        }
        cli::Commands::StartServer(args) => {
            let logger = Logger::new("Lantern HTTP", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            http_server::start(args, Some(logger))
        }
    };

    let logger = _main_logger.unwrap();
    if let Err(e) = res {
        logger.error(&e.to_string());
        process::exit(1);
    }
}
#[cfg(not(feature = "cli"))]
fn main() {}
