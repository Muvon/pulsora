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

impl From<bincode::Error> for PulsoraError {
    fn from(err: bincode::Error) -> Self {
        PulsoraError::Serialization(err.to_string())
    }
}

impl From<csv::Error> for PulsoraError {
    fn from(err: csv::Error) -> Self {
        PulsoraError::Ingestion(err.to_string())
    }
}
