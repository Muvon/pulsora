use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info};

mod config;
mod error;
mod server;
mod storage;

use config::Config;
use error::Result;

#[derive(Parser)]
#[command(name = "pulsora")]
#[command(about = "High-performance time series database optimized for market data")]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Load configuration
    let config = match cli.config {
        Some(path) => {
            info!("Loading configuration from: {}", path.display());
            Config::from_file(path)?
        }
        None => {
            info!("Using default configuration");
            Config::default()
        }
    };

    info!(
        "Starting Pulsora server on {}:{}",
        config.server.host, config.server.port
    );
    info!("Data directory: {}", config.storage.data_dir);

    // Start the server
    if let Err(e) = server::start(config).await {
        error!("Server error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
