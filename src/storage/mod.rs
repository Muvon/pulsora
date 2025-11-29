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

use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::Config;
use crate::error::{PulsoraError, Result};

use arrow::array::Array;
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
use schema::Schema;
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
    #[allow(dead_code)] // Used during initialization to configure Rayon
    ingestion_threads: usize,
    query_threads: usize,
}

impl StorageEngine {
    pub async fn new(config: &Config) -> Result<Self> {
        // Configure Rayon thread pool for parallel processing (used for ingestion)
        // This must be done before any parallel operations
        let ingestion_thread_count = if config.ingestion.ingestion_threads == 0 {
            rayon::current_num_threads() // Auto-detect (default: num CPUs)
        } else {
            config.ingestion.ingestion_threads
        };

        // Try to configure global thread pool (only works if not already initialized)
        // Ignore error if already initialized (e.g., in tests)
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(ingestion_thread_count)
            .build_global();

        info!(
            "Rayon thread pool configured: {} threads for ingestion",
            rayon::current_num_threads()
        );

        // Determine query thread count
        let query_thread_count = if config.query.query_threads == 0 {
            rayon::current_num_threads() // Auto-detect (same as ingestion by default)
        } else {
            config.query.query_threads
        };

        info!("Query parallelism: {} threads", query_thread_count);

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
            ingestion_threads: ingestion_thread_count,
            query_threads: query_thread_count,
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

    async fn ingest_processed_rows(
        &self,
        table: &str,
        processed_rows: Vec<(u64, HashMap<String, String>)>,
        schema: Schema,
        start_time: std::time::Instant,
    ) -> Result<IngestionStats> {
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

    pub async fn ingest_arrow(&self, table: &str, arrow_data: Vec<u8>) -> Result<IngestionStats> {
        let start_time = std::time::Instant::now();

        // 1. Create Arrow Reader
        let cursor = std::io::Cursor::new(&arrow_data);
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(|e| {
            PulsoraError::InvalidData(format!("Failed to create Arrow stream reader: {}", e))
        })?;

        // 2. Read all batches (usually just one for ingestion)
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                PulsoraError::InvalidData(format!("Failed to read Arrow batch: {}", e))
            })?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            return Ok(IngestionStats {
                rows_inserted: 0,
                processing_time_ms: 0,
            });
        }

        // 3. Get or create schema
        // For schema inference, we might need to convert the first batch to rows if schema doesn't exist
        // But for now, let's assume we can infer from Arrow schema or fallback to row-based inference
        let schema = {
            let mut schemas = self.schemas.write().await;
            if let Some(s) = schemas.get_schema(table) {
                s.clone()
            } else {
                // Fast path: Infer schema directly from Arrow IPC stream
                let cursor = std::io::Cursor::new(&arrow_data);
                let reader =
                    arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(|e| {
                        PulsoraError::InvalidData(format!("Failed to read Arrow stream: {}", e))
                    })?;
                let arrow_schema = reader.schema();

                let schema = schemas.infer_schema_from_arrow(table, &arrow_schema)?;

                // Register the inferred schema
                // Note: infer_schema_from_arrow doesn't save to DB automatically like get_or_create_schema
                // We need to manually save it if we want persistence, or update infer_schema_from_arrow to do it.
                // Let's use a helper method on schemas to save it.
                // Actually, get_or_create_schema does save. We should probably add a method to register schema.
                // For now, let's just use the internal save method if we can access it, or add a public register method.
                // Wait, `infer_schema_from_arrow` is on `SchemaManager` (self.schemas is SchemaManager).
                // Let's check SchemaManager methods.

                // We need to modify SchemaManager to support registering/saving a schema directly.
                // Or we can just call save_schema_to_db if it was public, but it's private.
                // Let's modify SchemaManager in schema.rs to have a register_schema method.

                // For now, let's assume we add register_schema to SchemaManager.
                schemas.register_schema(table, schema)?
            }
        };

        let id_manager = {
            let mut id_managers = self.id_managers.write().await;
            id_managers.get_or_create(table)?.clone()
        };

        let mut total_rows = 0;

        // 4. Process batches
        for batch in batches {
            let row_count = batch.num_rows();
            total_rows += row_count as u64;

            // FAST PATH: If batch is large enough, bypass row conversion
            if row_count >= 1000 {
                // Flush existing buffer first to maintain order
                {
                    let mut buffers = self.buffers.write().await;
                    if let Some(buffer) = buffers.get_mut(table) {
                        if !buffer.is_empty() {
                            let rows_to_write = buffer.get_rows();
                            ingestion::write_batch_to_rocksdb(
                                &self.db,
                                table,
                                &schema,
                                &rows_to_write,
                            )?;
                            buffer.clear()?;
                        }
                    }
                }

                // Handle IDs
                let mut ids = Vec::with_capacity(row_count);
                let mut timestamps = Vec::with_capacity(row_count);
                let mut min_ts = i64::MAX;
                let mut max_ts = i64::MIN;

                // Extract IDs and Timestamps
                // We need to handle "Auto" IDs and User IDs
                // For simplicity in Fast Path, we assume ID column exists in Arrow batch
                // If not, we generate Auto IDs

                // Check for ID column
                let id_col_idx = batch.schema().index_of(&schema.id_column).ok();
                let ts_col_idx = schema
                    .get_timestamp_column()
                    .and_then(|name| batch.schema().index_of(name).ok());

                // Generate IDs
                if let Some(idx) = id_col_idx {
                    let col = batch.column(idx);
                    // Assume Int64 or String
                    // For now, let's assume we can cast to Int64 or parse String
                    // This is a simplification. A robust implementation handles all types.
                    use arrow::array::{Int64Array, StringArray};

                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..row_count {
                            if arr.is_null(i) {
                                ids.push(id_manager.next_auto_id());
                            } else {
                                let user_id = arr.value(i) as u64;
                                id_manager.register_user_id(user_id)?;
                                ids.push(user_id);
                            }
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        for i in 0..row_count {
                            if arr.is_null(i) {
                                ids.push(id_manager.next_auto_id());
                            } else {
                                let s = arr.value(i);
                                if s.is_empty() {
                                    ids.push(id_manager.next_auto_id());
                                } else if let Ok(uid) = s.parse::<u64>() {
                                    id_manager.register_user_id(uid)?;
                                    ids.push(uid);
                                } else {
                                    // Hash string to u64 or error?
                                    // For now, error or auto
                                    ids.push(id_manager.next_auto_id());
                                }
                            }
                        }
                    } else {
                        // Unsupported ID type, fallback to auto
                        for _ in 0..row_count {
                            ids.push(id_manager.next_auto_id());
                        }
                    }
                } else {
                    // No ID column, auto-generate
                    for _ in 0..row_count {
                        ids.push(id_manager.next_auto_id());
                    }
                }

                // Extract Timestamps
                if let Some(idx) = ts_col_idx {
                    let col = batch.column(idx);
                    // Assume Int64 (timestamp) or String
                    use arrow::array::{Int64Array, StringArray};

                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..row_count {
                            let ts = if arr.is_null(i) {
                                chrono::Utc::now().timestamp_millis()
                            } else {
                                arr.value(i)
                            };
                            timestamps.push(ts);
                            min_ts = min_ts.min(ts);
                            max_ts = max_ts.max(ts);
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        for i in 0..row_count {
                            let ts = if arr.is_null(i) {
                                chrono::Utc::now().timestamp_millis()
                            } else {
                                ingestion::parse_timestamp(arr.value(i))
                                    .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis())
                            };
                            timestamps.push(ts);
                            min_ts = min_ts.min(ts);
                            max_ts = max_ts.max(ts);
                        }
                    } else {
                        let now = chrono::Utc::now().timestamp_millis();
                        timestamps.resize(row_count, now);
                        min_ts = now;
                        max_ts = now;
                    }
                } else {
                    let now = chrono::Utc::now().timestamp_millis();
                    timestamps.resize(row_count, now);
                    min_ts = now;
                    max_ts = now;
                }

                // Create ColumnBlock directly
                let column_block = columnar::ColumnBlock::from_arrow(&batch, &schema, &ids)?;
                // Write directly
                ingestion::write_column_block_to_rocksdb(
                    &self.db,
                    table,
                    &column_block,
                    &ids,
                    min_ts,
                    max_ts,
                    Some(&timestamps),
                )?;
            } else {
                // SLOW PATH: Convert to rows and use buffer
                // We need to convert this specific batch to rows
                // But we don't have a function for single batch -> rows yet, only bytes -> rows
                // So we'll use the existing parse_arrow which parses ALL batches
                // This is inefficient for mixed workloads but fine for now

                // Re-serialize batch to bytes? No, that's wasteful.
                // Let's just use the existing flow for small batches
                let rows = ingestion::parse_arrow(&arrow_data)?;
                let processed_rows = ingestion::process_rows_parallel(rows, &schema, &id_manager)?;
                self.ingest_processed_rows(table, processed_rows, schema.clone(), start_time)
                    .await?;
                return Ok(IngestionStats {
                    rows_inserted: total_rows,
                    processing_time_ms: start_time.elapsed().as_millis() as u64,
                });
            }
        }

        Ok(IngestionStats {
            rows_inserted: total_rows,
            processing_time_ms: start_time.elapsed().as_millis() as u64,
        })
    }

    pub async fn ingest_protobuf(
        &self,
        table: &str,
        proto_data: Vec<u8>,
    ) -> Result<IngestionStats> {
        let start_time = std::time::Instant::now();

        // Parse first to get rows
        let rows = ingestion::parse_protobuf(&proto_data)?;
        if rows.is_empty() {
            return Ok(IngestionStats {
                rows_inserted: 0,
                processing_time_ms: 0,
            });
        }

        // Get or create schema
        let schema = {
            let mut schemas = self.schemas.write().await;
            schemas.get_or_create_schema(table, &rows)?
        };

        let id_manager = {
            let mut id_managers = self.id_managers.write().await;
            id_managers.get_or_create(table)?.clone()
        };

        let processed_rows = ingestion::process_rows_parallel(rows, &schema, &id_manager)?;
        self.ingest_processed_rows(table, processed_rows, schema, start_time)
            .await
    }
    pub async fn ingest_csv(&self, table: &str, csv_data: String) -> Result<IngestionStats> {
        let start_time = std::time::Instant::now();

        // Try to get existing schema first to enable parallel processing
        let existing_schema = {
            let schemas = self.schemas.read().await;
            schemas.get_schema(table).cloned()
        };

        if let Some(schema) = existing_schema {
            // Ensure any buffered data is flushed first to maintain consistency
            self.flush_table(table).await?;

            // FAST PATH: Bulk ingestion directly to ColumnBlock
            let id_manager = {
                let mut id_managers = self.id_managers.write().await;
                id_managers.get_or_create(table)?.clone()
            };

            // Use spawn_blocking for CPU-intensive parsing/compression
            let table_owned = table.to_string();
            let schema_clone = schema.clone();
            let id_manager_clone = id_manager.clone();

            let (column_block, ids, min_ts, max_ts, timestamps) =
                tokio::task::spawn_blocking(move || {
                    ingestion::process_csv_bulk(&csv_data, &schema_clone, &id_manager_clone)
                })
                .await
                .map_err(|e| PulsoraError::Internal(e.to_string()))??;

            let row_count = column_block.row_count as u64;
            if row_count > 0 {
                // Write directly to RocksDB
                ingestion::write_column_block_to_rocksdb(
                    &self.db,
                    &table_owned,
                    &column_block,
                    &ids,
                    min_ts,
                    max_ts,
                    timestamps.as_deref(),
                )?;
            }

            let processing_time_ms = start_time.elapsed().as_millis() as u64;
            info!(
                "Ingested {} rows into table '{}' in {}ms (Fast Path)",
                row_count, table, processing_time_ms
            );

            return Ok(IngestionStats {
                rows_inserted: row_count,
                processing_time_ms,
            });
        }

        // SLOW PATH: Schema inference needed
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

        self.ingest_processed_rows(table, processed, schema, start_time)
            .await
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

        // Calculate limit for DB query to ensure we get enough data for offset + limit
        // We don't pass offset to DB query because we need to merge with buffer first
        let db_limit = if let Some(l) = limit {
            Some(offset.unwrap_or(0) + l)
        } else {
            None
        };

        let db_results = query::execute_query(
            &self.db,
            table,
            schema,
            start.clone(),
            end.clone(),
            db_limit,
            None, // Don't apply offset in DB query
            self.query_threads,
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
                let get_ts = |v: &serde_json::Value| -> i64 {
                    match v.get(ts_col) {
                        Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
                        Some(serde_json::Value::String(s)) => {
                            ingestion::parse_timestamp(s).unwrap_or(0)
                        }
                        _ => 0,
                    }
                };

                let ts_a = get_ts(a);
                let ts_b = get_ts(b);
                ts_a.cmp(&ts_b)
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

    /// Execute query and return results as CSV string
    /// This avoids intermediate JSON allocation for better performance
    pub async fn query_csv(
        &self,
        table: &str,
        start: Option<String>,
        end: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<String> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Query(format!("Table '{}' not found", table)))?;

        // Use optimized CSV query execution
        let csv_chunks = query::execute_query_csv(
            &self.db,
            table,
            schema,
            start,
            end,
            limit,
            offset,
            self.query_threads,
        )?;

        // Estimate total size
        let total_len: usize = csv_chunks.iter().map(|s| s.len()).sum();
        let mut csv = String::with_capacity(total_len + 1024);

        // Write header
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        csv.push_str(&columns.join(","));
        csv.push('\n');

        // Append chunks
        for chunk in csv_chunks {
            csv.push_str(&chunk);
        }

        Ok(csv)
    }

    /// Execute query and return results as Arrow IPC stream bytes
    /// This avoids intermediate JSON allocation for better performance
    pub async fn query_arrow(
        &self,
        table: &str,
        start: Option<String>,
        end: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<u8>> {
        let schemas = self.schemas.read().await;
        let schema = schemas
            .get_schema(table)
            .ok_or_else(|| PulsoraError::Query(format!("Table '{}' not found", table)))?;

        // Use optimized Arrow query execution
        let batches = query::execute_query_arrow(
            &self.db,
            table,
            schema,
            start,
            end,
            limit,
            offset,
            self.query_threads,
        )?;

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize batches to Arrow IPC stream
        let mut buffer = Vec::new();
        {
            let arrow_schema = batches[0].schema();
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &arrow_schema)
                .map_err(|e| {
                    PulsoraError::Internal(format!("Failed to create Arrow writer: {}", e))
                })?;

            for batch in batches {
                writer.write(&batch).map_err(|e| {
                    PulsoraError::Internal(format!("Failed to write Arrow batch: {}", e))
                })?;
            }
            writer.finish().map_err(|e| {
                PulsoraError::Internal(format!("Failed to finish Arrow stream: {}", e))
            })?;
        }

        Ok(buffer)
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

#[cfg(test)]
#[path = "mod_test.rs"]
mod mod_test;
