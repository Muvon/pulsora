use crate::error::{PulsoraError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub ingestion: IngestionConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub write_buffer_size_mb: usize,
    pub max_open_files: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    pub max_csv_size_mb: usize,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub compression: String,
    pub cache_size_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                write_buffer_size_mb: 64,
                max_open_files: 1000,
            },
            ingestion: IngestionConfig {
                max_csv_size_mb: 512,
                batch_size: 10000,
            },
            performance: PerformanceConfig {
                compression: "lz4".to_string(),
                cache_size_mb: 256,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
            },
        }
    }
}

impl Config {
    pub fn from_file(path: PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(&path).map_err(|e| {
            PulsoraError::Config(format!(
                "Failed to read config file {}: {}",
                path.display(),
                e
            ))
        })?;

        let config: Config = toml::from_str(&content)
            .map_err(|e| PulsoraError::Config(format!("Failed to parse config file: {}", e)))?;

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.server.port == 0 {
            return Err(PulsoraError::Config("Server port cannot be 0".to_string()));
        }

        if self.storage.write_buffer_size_mb == 0 {
            return Err(PulsoraError::Config(
                "Write buffer size must be greater than 0".to_string(),
            ));
        }

        if self.ingestion.batch_size == 0 {
            return Err(PulsoraError::Config(
                "Batch size must be greater than 0".to_string(),
            ));
        }

        let valid_compressions = ["none", "snappy", "lz4", "zstd"];
        if !valid_compressions.contains(&self.performance.compression.as_str()) {
            return Err(PulsoraError::Config(format!(
                "Invalid compression '{}'. Valid options: {:?}",
                self.performance.compression, valid_compressions
            )));
        }

        Ok(())
    }
}
