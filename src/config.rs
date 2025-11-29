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
    pub max_body_size_mb: usize, // 0 means unlimited
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub write_buffer_size_mb: usize,
    pub max_open_files: i32,
    pub buffer_size: usize,
    pub flush_interval_ms: u64,
    /// Enable Write-Ahead Log (WAL) for durability.
    /// If true, buffered rows are written to disk immediately.
    /// If false, buffered rows are lost on crash.
    #[serde(default = "default_wal_enabled")]
    pub wal_enabled: bool,
}

fn default_wal_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    pub max_csv_size_mb: usize,
    pub batch_size: usize,
    #[serde(default)]
    pub ingestion_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub compression: String,
    pub cache_size_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String, // "pretty" or "json"
    pub enable_access_logs: bool,
    pub enable_performance_logs: bool,
    pub file_output: Option<String>, // Optional file path for logs
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                max_body_size_mb: 0, // 0 means unlimited
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                write_buffer_size_mb: 64,
                max_open_files: 1000,
                buffer_size: 1000,
                flush_interval_ms: 1000,
                wal_enabled: true,
            },
            ingestion: IngestionConfig {
                max_csv_size_mb: 512,
                batch_size: 10000,
                ingestion_threads: 0, // 0 means auto-detect
            },
            performance: PerformanceConfig {
                compression: "lz4".to_string(),
                cache_size_mb: 256,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "pretty".to_string(),
                enable_access_logs: true,
                enable_performance_logs: true,
                file_output: None, // Default to stdout only
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

        let valid_log_levels = ["error", "warn", "info", "debug", "trace"];
        if !valid_log_levels.contains(&self.logging.level.as_str()) {
            return Err(PulsoraError::Config(format!(
                "Invalid log level '{}'. Valid options: {:?}",
                self.logging.level, valid_log_levels
            )));
        }

        let valid_log_formats = ["pretty", "json"];
        if !valid_log_formats.contains(&self.logging.format.as_str()) {
            return Err(PulsoraError::Config(format!(
                "Invalid log format '{}'. Valid options: {:?}",
                self.logging.format, valid_log_formats
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
