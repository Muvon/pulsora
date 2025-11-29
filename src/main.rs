// Copyright 2025 Muvon Un Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Parser;
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
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

    // Load configuration first to get logging settings
    let config = match cli.config {
        Some(path) => {
            println!("Loading configuration from: {}", path.display());
            Config::from_file(path)?
        }
        None => {
            println!("Using default configuration");
            Config::default()
        }
    };

    // Initialize logging with proper config settings
    initialize_logging(&config)?;

    info!(
        "🚀 Starting Pulsora v{} - High-Performance Time Series Database",
        env!("CARGO_PKG_VERSION")
    );
    info!("📊 Optimized for market data and time-ordered datasets");
    info!(
        "🌐 Server binding: {}:{}",
        config.server.host, config.server.port
    );
    info!("💾 Data directory: {}", config.storage.data_dir);
    info!("🗜️  Compression: {}", config.performance.compression);
    info!("📝 Log level: {}", config.logging.level);
    info!("🎯 Log format: {}", config.logging.format);

    if config.logging.enable_access_logs {
        info!("📋 Access logging: enabled");
    }
    if config.logging.enable_performance_logs {
        info!("⚡ Performance logging: enabled");
    }

    // Start the server
    if let Err(e) = server::start(config).await {
        error!("❌ Server error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}

fn initialize_logging(config: &Config) -> Result<()> {
    // Create filter with the configured level - RESPECT the config level
    let filter = EnvFilter::from_default_env()
        .add_directive(format!("pulsora={}", config.logging.level).parse().unwrap()) // Use config level for our crate
        .add_directive("tower_http=warn".parse().unwrap()) // Reduce HTTP middleware noise
        .add_directive("axum=warn".parse().unwrap()) // Reduce Axum framework noise
        .add_directive("rocksdb=warn".parse().unwrap()); // Reduce RocksDB noise

    // Configure format based on config with proper colors
    match config.logging.format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(false)
                .with_line_number(false)
                .init();
        }
        _ => {
            // Default to clean format with smart colors
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .with_ansi(atty::is(atty::Stream::Stdout)) // Auto-detect color support
                .init();
        }
    }

    Ok(())
}
