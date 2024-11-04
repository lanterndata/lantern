#[cfg(feature = "cli")]
mod cli;

#[cfg(feature = "cli")]
#[tokio::main]
async fn main() {
    use std::process;

    use clap::Parser;
    use lantern_cli::logger::{LogLevel, Logger};
    use lantern_cli::*;
    use tokio_util::sync::CancellationToken;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default CryptoProvider");

    let cli = cli::Cli::parse();
    let mut _main_logger = None;
    let res = match cli.command {
        cli::Commands::CreateEmbeddings(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            let res = embeddings::create_embeddings_from_db(
                args,
                true,
                None,
                CancellationToken::new(),
                Some(logger),
            )
            .await;

            if let Err(e) = res {
                let logger = _main_logger.as_ref().unwrap();
                logger.error(&e.to_string());
                process::exit(1);
            }
            Ok(())
        }
        cli::Commands::ShowModels(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            embeddings::show_available_models(&args, Some(logger)).await
        }
        cli::Commands::ShowRuntimes => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            embeddings::show_available_runtimes(Some(logger))
        }
        cli::Commands::MeasureModelSpeed(args) => {
            let logger = Logger::new("Lantern Embeddings", LogLevel::Info);
            _main_logger = Some(logger.clone());
            embeddings::measure_speed::start_speed_test(&args, Some(logger)).await
        }
        cli::Commands::AutotuneIndex(args) => {
            let logger = Logger::new("Lantern Index Autotune", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            tokio::task::spawn_blocking(move || {
                index_autotune::autotune_index(&args, None, None, Some(logger))
            })
            .await
            .unwrap()
        }
        cli::Commands::PQTable(args) => {
            let logger = Logger::new("Lantern PQ", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            tokio::task::spawn_blocking(move || pq::quantize_table(args, None, None, Some(logger)))
                .await
                .unwrap()
        }
        cli::Commands::StartDaemon(args) => {
            let logger = Logger::new("Lantern Daemon", args.log_level.value());
            _main_logger = Some(logger.clone());
            daemon::start(args, Some(logger), CancellationToken::new()).await
        }
        cli::Commands::StartServer(args) => {
            let logger = Logger::new("Lantern HTTP", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            http_server::start(args, Some(logger))
        }
        cli::Commands::StartIndexingServer(args) => {
            let logger = Logger::new("Lantern External Index", LogLevel::Debug);
            _main_logger = Some(logger.clone());
            external_index::server::start_tcp_server(args, Some(logger))
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
