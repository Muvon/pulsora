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

use thiserror::Error;

pub type Result<T> = std::result::Result<T, PulsoraError>;

#[derive(Error, Debug)]
pub enum PulsoraError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Ingestion error: {0}")]
    Ingestion(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

impl From<serde_json::Error> for PulsoraError {
    fn from(err: serde_json::Error) -> Self {
        PulsoraError::Serialization(err.to_string())
    }
}

impl From<anyhow::Error> for PulsoraError {
    fn from(err: anyhow::Error) -> Self {
        PulsoraError::Serialization(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for PulsoraError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        PulsoraError::InvalidData(err.to_string())
    }
}

impl From<csv::Error> for PulsoraError {
    fn from(err: csv::Error) -> Self {
        PulsoraError::Ingestion(err.to_string())
    }
}
