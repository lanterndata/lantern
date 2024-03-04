use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct HttpServerArgs {
    /// Fully associated database connection string including db name
    #[arg(short, long)]
    pub db_uri: String,

    /// Indicates if this is remote or local connection
    #[arg(short, long, default_value_t = false)]
    pub remote_database: bool,

    /// Host to listen
    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,

    /// Port to bind
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
}
