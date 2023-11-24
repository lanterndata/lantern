use clap::Parser;
use lantern_create_index;
use lantern_daemon;
use lantern_embeddings;
use lantern_logger::{LogLevel, Logger};
mod cli;

fn main() {
    let cli = cli::Cli::parse();
    let mut _main_logger = None;
    let res = match cli.command {
        cli::Commands::CreateIndex(args) => {
            let logger = Logger::new("Lantern Index", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            lantern_create_index::create_usearch_index(&args, Some(logger))
        }
        cli::Commands::CreateEmbeddings(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            let res = lantern_embeddings::create_embeddings_from_db(args, true, None, Some(logger));
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
            lantern_embeddings::show_available_models(&args, Some(logger))
        }
        cli::Commands::MeasureModelSpeed(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Info);
            _main_logger = Some(logger.clone());
            lantern_embeddings::measure_speed::start_speed_test(&args, Some(logger))
        }
        cli::Commands::StartDaemon(args) => {
            let logger = Logger::new("Lantern Daemon", args.log_level.value());
            _main_logger = Some(logger.clone());
            lantern_daemon::start(args, Some(logger))
        }
    };

    let logger = _main_logger.unwrap();
    if let Err(e) = res {
        logger.error(&e.to_string());
    }
}
