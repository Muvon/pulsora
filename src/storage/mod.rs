use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::error::{PulsoraError, Result};

pub mod encoding;
pub mod ingestion;
pub mod query;
pub mod schema;

use schema::SchemaManager;

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestionStats {
    pub rows_inserted: u64,
    pub processing_time_ms: u64,
}

#[derive(Clone)]
pub struct StorageEngine {
    pub db: Arc<DB>,
    pub schemas: Arc<RwLock<SchemaManager>>,
    config: Config,
}

impl StorageEngine {
    pub async fn new(config: &Config) -> Result<Self> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.storage.data_dir)?;

        // Configure RocksDB options for time series workload
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(config.storage.write_buffer_size_mb * 1024 * 1024);
        opts.set_max_open_files(config.storage.max_open_files);
        // Block cache will be set via block-based table options in production

        // Set compression based on config
        match config.performance.compression.as_str() {
            "none" => opts.set_compression_type(rocksdb::DBCompressionType::None),
            "snappy" => opts.set_compression_type(rocksdb::DBCompressionType::Snappy),
            "lz4" => opts.set_compression_type(rocksdb::DBCompressionType::Lz4),
            "zstd" => opts.set_compression_type(rocksdb::DBCompressionType::Zstd),
            _ => opts.set_compression_type(rocksdb::DBCompressionType::Lz4),
        }

        // Optimize for time series workload
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_max_background_jobs(4);
        opts.set_bytes_per_sync(1048576); // 1MB

        let db = DB::open(&opts, &config.storage.data_dir)?;

        info!("RocksDB opened at: {}", config.storage.data_dir);
        info!("Compression: {}", config.performance.compression);
        info!(
            "Write buffer size: {}MB",
            config.storage.write_buffer_size_mb
        );
        info!("Cache size: {}MB", config.performance.cache_size_mb);

        Ok(Self {
            db: Arc::new(db),
            schemas: Arc::new(RwLock::new(SchemaManager::new())),
            config: config.clone(),
        })
    }

    pub async fn ingest_csv(&self, table: &str, csv_data: String) -> Result<IngestionStats> {
        let start_time = std::time::Instant::now();

        // Parse CSV and infer/validate schema
        let rows = ingestion::parse_csv(&csv_data)?;
        if rows.is_empty() {
            return Err(PulsoraError::Ingestion("No data rows found".to_string()));
        }

        // Get or create schema (pass ALL rows for better type inference)
        let schema = {
            let mut schemas = self.schemas.write().await;
            schemas.get_or_create_schema(table, &rows)?
        };

        // Validate and insert data
        let rows_inserted = ingestion::insert_rows(
            &self.db,
            table,
            &schema,
            rows,
            self.config.ingestion.batch_size,
        )?;

        let processing_time_ms = start_time.elapsed().as_millis() as u64;

        info!(
            "Ingested {} rows into table '{}' in {}ms",
            rows_inserted, table, processing_time_ms
        );

        Ok(IngestionStats {
            rows_inserted,
            processing_time_ms,
        })
    }

    pub async fn query(
        &self,
        table: &str,
        start: Option<String>,
        end: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Query(format!("Table '{}' not found", table)))?;

        query::execute_query(&self.db, table, schema, start, end, limit, offset)
    }

    pub async fn get_schema(&self, table: &str) -> Result<serde_json::Value> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Schema(format!("Table '{}' not found", table)))?;

        Ok(serde_json::to_value(schema)?)
    }
}
