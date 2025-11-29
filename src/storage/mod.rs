use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::error::{PulsoraError, Result};

/// Calculate FNV-1a hash for table name
pub fn calculate_table_hash(table: &str) -> u32 {
    let mut hash = 2166136261u32;
    for byte in table.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

pub mod buffer;
pub mod columnar;
pub mod compression;
pub mod encoding;
pub mod id_manager;
pub mod ingestion;
pub mod query;
pub mod schema;
pub mod wal;

use buffer::TableBuffer;
use id_manager::IdManagerRegistry;
use schema::SchemaManager;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestionStats {
    pub rows_inserted: u64,
    pub processing_time_ms: u64,
}

#[derive(Clone)]
pub struct StorageEngine {
    pub db: Arc<DB>,
    pub schemas: Arc<RwLock<SchemaManager>>,
    pub id_managers: Arc<RwLock<IdManagerRegistry>>,
    pub buffers: Arc<RwLock<HashMap<String, TableBuffer>>>,
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

        let engine = Self {
            db: Arc::clone(&db_arc),
            schemas: Arc::new(RwLock::new(SchemaManager::new(Arc::clone(&db_arc))?)),
            id_managers: Arc::new(RwLock::new(IdManagerRegistry::new(Arc::clone(&db_arc)))),
            buffers: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
        };

        // Recover WALs if enabled
        if config.storage.wal_enabled {
            let wal_dir = std::path::PathBuf::from(&config.storage.data_dir).join("wal");
            if wal_dir.exists() {
                info!("Recovering WALs from {}", wal_dir.display());
                let mut buffers = engine.buffers.write().await;
                let schemas = engine.schemas.write().await;

                // Iterate over all .wal files
                if let Ok(entries) = std::fs::read_dir(&wal_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                            // Try to find table name from hash (this is tricky without a reverse map)
                            // For now, we'll rely on the fact that we need the schema to create the buffer.
                            // Since we can't easily map hash -> table name without scanning all schemas,
                            // we will implement a recovery strategy that loads schemas first.

                            // BETTER STRATEGY:
                            // 1. Load all schemas from RocksDB to get known tables.
                            // 2. For each table, check if a WAL file exists.
                            // 3. If yes, replay it.
                        }
                    }
                }

                // Correct Recovery Strategy:
                // Iterate over all known schemas (tables)
                let known_tables = schemas.list_tables(); // We need to add this method to SchemaManager
                for table in known_tables {
                    let table_hash = calculate_table_hash(&table);
                    let wal_path = wal_dir.join(format!("{}.wal", table_hash));

                    if wal_path.exists() {
                        info!("Recovering WAL for table '{}'", table);
                        if let Some(schema) = schemas.get_schema(&table) {
                            let wal = wal::WriteAheadLog::new(&config.storage.data_dir, &table)?;
                            let rows = wal.replay()?;

                            if !rows.is_empty() {
                                info!("Recovered {} rows for table '{}'", rows.len(), table);
                                let mut buffer = TableBuffer::new(schema.clone(), Some(wal));
                                for (id, row) in rows {
                                    // Directly insert into memory, bypassing WAL append (since we just read it)
                                    buffer.rows.insert(id, row);
                                }
                                buffers.insert(table, buffer);
                            }
                        }
                    }
                }
            }
        }

        // Spawn background flush task only if interval > 0
        let flush_engine = engine.clone();
        let flush_interval = config.storage.flush_interval_ms;

        if flush_interval > 0 {
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(flush_interval));
                loop {
                    interval.tick().await;

                    // Get list of tables to avoid holding lock during flush
                    let tables: Vec<String> = {
                        let buffers = flush_engine.buffers.read().await;
                        buffers.keys().cloned().collect()
                    };

                    for table in tables {
                        if let Err(e) = flush_engine.flush_table(&table).await {
                            tracing::error!("Failed to background flush table '{}': {}", table, e);
                        }
                    }
                }
            });
        } else {
            info!("Background flush disabled (flush_interval_ms = 0)");
        }

        Ok(engine)
    }

    pub async fn flush_table(&self, table: &str) -> Result<u64> {
        let mut buffers = self.buffers.write().await;
        if let Some(buffer) = buffers.get_mut(table) {
            if buffer.get_rows().is_empty() {
                return Ok(0);
            }

            let rows = buffer.get_rows();
            let count = ingestion::write_batch_to_rocksdb(&self.db, table, &buffer.schema, &rows)?;
            buffer.clear()?;
            Ok(count)
        } else {
            Ok(0)
        }
    }

    pub async fn ingest_csv(&self, table: &str, csv_data: String) -> Result<IngestionStats> {
        let start_time = std::time::Instant::now();

        // Try to get existing schema first to enable parallel processing
        let existing_schema = {
            let schemas = self.schemas.read().await;
            schemas.get_schema(table).cloned()
        };

        let (processed_rows, schema) = if let Some(schema) = existing_schema {
            // Parallel path for existing tables
            let id_manager = {
                let mut id_managers = self.id_managers.write().await;
                id_managers.get_or_create(table)?.clone()
            };

            let rows = ingestion::process_csv_parallel(
                &csv_data,
                &schema,
                &id_manager,
                self.config.ingestion.ingestion_threads,
            )?;

            (rows, schema)
        } else {
            // Serial path for new tables (need to infer schema)
            let rows = ingestion::parse_csv(&csv_data)?;
            if rows.is_empty() {
                return Err(PulsoraError::Ingestion("No data rows found".to_string()));
            }

            // Get or create schema (pass ALL rows for better type inference)
            let schema = {
                let mut schemas = self.schemas.write().await;
                schemas.get_or_create_schema(table, &rows)?
            };

            let id_manager = {
                let mut id_managers = self.id_managers.write().await;
                id_managers.get_or_create(table)?.clone()
            };

            let mut processed = Vec::with_capacity(rows.len());
            for mut row in rows {
                // Handle ID assignment
                let row_id = if let Some(id_str) = row.get(&schema.id_column) {
                    if id_str.trim().is_empty() {
                        crate::storage::id_manager::RowId::Auto
                    } else {
                        let parsed_id = crate::storage::id_manager::RowId::from_string(id_str)?;
                        if let crate::storage::id_manager::RowId::User(id) = parsed_id {
                            id_manager.register_user_id(id)?;
                        }
                        parsed_id
                    }
                } else {
                    crate::storage::id_manager::RowId::Auto
                };

                let actual_id = row_id.resolve(&id_manager);
                row.insert(schema.id_column.clone(), actual_id.to_string());
                schema.validate_row(&row)?;
                processed.push((actual_id, row));
            }

            (processed, schema)
        };

        let rows_inserted = processed_rows.len() as u64;

        // Push to buffer (Batch)
        {
            let mut buffers = self.buffers.write().await;

            // Get or create buffer for this table
            if !buffers.contains_key(table) {
                let wal = if self.config.storage.wal_enabled {
                    Some(wal::WriteAheadLog::new(
                        &self.config.storage.data_dir,
                        table,
                    )?)
                } else {
                    None
                };
                buffers.insert(table.to_string(), TableBuffer::new(schema.clone(), wal));
            }

            let buffer = buffers.get_mut(table).unwrap();

            // Batch push - ONE WAL write, ONE lock acquisition
            buffer.push_batch(processed_rows)?;

            // Check if we need to flush
            if buffer.should_flush(
                self.config.storage.buffer_size,
                self.config.storage.flush_interval_ms,
            ) {
                let rows_to_write = buffer.get_rows();
                ingestion::write_batch_to_rocksdb(&self.db, table, &schema, &rows_to_write)?;
                buffer.clear()?;
            }
        } // Release buffers lock

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

        let db_results = query::execute_query(
            &self.db,
            table,
            schema,
            start.clone(),
            end.clone(),
            limit,
            offset,
        )?;

        // Merge with buffer data and deduplicate
        let buffers = self.buffers.read().await;
        let mut merged_map: HashMap<String, serde_json::Value> = HashMap::new();
        let id_col = &schema.id_column;

        // 1. Add DB results to map
        for row in db_results {
            let id = row
                .get(id_col)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => String::new(),
                })
                .unwrap_or_default();
            if !id.is_empty() {
                merged_map.insert(id, row);
            }
        }

        // 2. Add/Overwrite with buffer results
        if let Some(buffer) = buffers.get(table) {
            let buffered_rows = buffer.get_rows();
            if !buffered_rows.is_empty() {
                let start_ts = start
                    .as_deref()
                    .map(query::parse_query_timestamp)
                    .transpose()?
                    .unwrap_or(0);
                let end_ts = end
                    .as_deref()
                    .map(query::parse_query_timestamp)
                    .transpose()?
                    .unwrap_or(i64::MAX);

                for (_, row) in buffered_rows {
                    // Check timestamp
                    let mut include = true;
                    if let Some(ts_col) = schema.get_timestamp_column() {
                        if let Some(ts_val) = row.get(ts_col) {
                            if let Ok(ts) = ingestion::parse_timestamp(ts_val) {
                                if ts < start_ts || ts > end_ts {
                                    include = false;
                                }
                            }
                        }
                    }

                    if include {
                        let json_row = query::convert_row_to_json(&row, schema)?;
                        let id = json_row
                            .get(id_col)
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                _ => String::new(),
                            })
                            .unwrap_or_default();
                        if !id.is_empty() {
                            merged_map.insert(id, json_row);
                        }
                    }
                }
            }
        }

        // Convert back to Vec
        // Note: This loses the original sort order from DB query.
        // Ideally we should re-sort by timestamp if it's a time-series query.
        let mut results: Vec<serde_json::Value> = merged_map.into_values().collect();

        // Sort by timestamp if possible
        if let Some(ts_col) = schema.get_timestamp_column() {
            results.sort_by(|a, b| {
                let ts_a = a.get(ts_col).and_then(|v| v.as_str()).unwrap_or("");
                let ts_b = b.get(ts_col).and_then(|v| v.as_str()).unwrap_or("");
                ts_a.cmp(ts_b)
            });
        }

        // Re-apply limit/offset
        if let Some(offset_val) = offset {
            if offset_val < results.len() {
                results.drain(0..offset_val);
            } else {
                results.clear();
            }
        }

        if let Some(limit_val) = limit {
            if limit_val < results.len() {
                results.truncate(limit_val);
            }
        }

        Ok(results)
    }

    pub async fn get_table_count(&self, table: &str) -> Result<u64> {
        // Check if table exists by checking schema
        let schemas = self.schemas.read().await;
        if schemas.get_schema(table).is_none() {
            return Err(PulsoraError::Query(format!("Table '{}' not found", table)));
        }
        drop(schemas);

        // Count rows by iterating through table keys
        let table_hash = calculate_table_hash(table);
        let mut count = 0u64;

        // Create prefix for this table's keys
        let prefix = table_hash.to_be_bytes();
        let iter = self.db.prefix_iterator(prefix);

        for item in iter {
            match item {
                Ok((key, value)) => {
                    // Verify this is actually our table (not a hash collision)
                    if key.len() >= 4 {
                        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
                        if key_table_hash == table_hash {
                            // Check if this is a row pointer (not a block or schema)
                            if !value.is_empty() && value[0] == 0xFF {
                                count += 1;
                            }
                        } else {
                            // Hash collision or end of our table's data
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        Ok(count)
    }

    pub async fn get_schema(&self, table: &str) -> Result<serde_json::Value> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Schema(format!("Table '{}' not found", table)))?;

        Ok(serde_json::to_value(schema)?)
    }

    pub async fn get_row_by_id_json(
        &self,
        table: &str,
        id: u64,
    ) -> Result<Option<serde_json::Value>> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Query(format!("Table '{}' not found", table)))?;

        if let Some(row) = query::get_row_by_id(&self.db, table, schema, id)? {
            let json_row = query::convert_row_to_json(&row, schema)?;
            Ok(Some(json_row))
        } else {
            Ok(None)
        }
    }
}
