use clap::Parser;
use lantern_create_index;
use lantern_embeddings;
mod cli;

fn main() {
    let cli = cli::Cli::parse();
    match &cli.command {
        cli::Commands::CreateIndex(args) => {
            lantern_create_index::create_usearch_index(args).unwrap();
        }
        cli::Commands::CreateEmbeddings(args) => {
            lantern_embeddings::create_embeddings_from_db(args).unwrap();
        }
        cli::Commands::ShowModels(args) => {
            lantern_embeddings::show_available_models(args);
        }
    }
}
