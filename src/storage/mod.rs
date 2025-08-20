use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::error::{PulsoraError, Result};

pub mod columnar;
pub mod compression;
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

        // Configure RocksDB options optimized for time series workload
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // Memory settings
        opts.set_write_buffer_size(config.storage.write_buffer_size_mb * 1024 * 1024);
        opts.set_max_write_buffer_number(6); // More buffers for parallel writes
        opts.set_min_write_buffer_number_to_merge(2);

        // File settings
        opts.set_max_open_files(config.storage.max_open_files);
        opts.set_max_total_wal_size(256 * 1024 * 1024); // 256MB WAL

        // Compaction settings optimized for time-series
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_max_background_jobs(8); // More parallel compaction
        opts.set_bytes_per_sync(1048576); // 1MB sync
        opts.set_wal_bytes_per_sync(1048576); // 1MB WAL sync

        // Target file size for better read performance
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB files
        opts.set_target_file_size_multiplier(2);

        // Optimize for sequential writes (time-series pattern)
        opts.set_advise_random_on_open(false);
        opts.set_compaction_readahead_size(2 * 1024 * 1024); // 2MB readahead

        // Block cache for read performance
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_size(16 * 1024); // 16KB blocks
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        // Create block cache
        let cache = rocksdb::Cache::new_lru_cache(config.performance.cache_size_mb * 1024 * 1024);
        block_opts.set_block_cache(&cache);

        // Bloom filter for faster lookups (10 bits per key)
        block_opts.set_bloom_filter(10.0, false);

        opts.set_block_based_table_factory(&block_opts);

        // Set compression based on config
        match config.performance.compression.as_str() {
            "none" => {
                opts.set_compression_type(rocksdb::DBCompressionType::None);
                opts.set_bottommost_compression_type(rocksdb::DBCompressionType::None);
            }
            "snappy" => {
                opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
                opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Snappy);
            }
            "lz4" => {
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Lz4);
            }
            "zstd" => {
                // Use lighter compression for upper levels, heavier for bottom
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
                opts.set_bottommost_compression_options(-14, 32767, 0, 1, true);
            }
            _ => {
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Lz4);
            }
        }

        // Enable statistics for monitoring
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(600); // Dump stats every 10 minutes

        let db = DB::open(&opts, &config.storage.data_dir)?;
        let db_arc = Arc::new(db);

        info!("RocksDB opened at: {}", config.storage.data_dir);
        info!("Compression: {}", config.performance.compression);
        info!(
            "Write buffer: {}MB x {} buffers",
            config.storage.write_buffer_size_mb, 6
        );
        info!("Block cache: {}MB", config.performance.cache_size_mb);
        info!("Max background jobs: 8");

        Ok(Self {
            db: Arc::clone(&db_arc),
            schemas: Arc::new(RwLock::new(SchemaManager::new(Arc::clone(&db_arc))?)),
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
