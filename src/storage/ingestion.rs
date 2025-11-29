//! CSV data ingestion and processing with ID-based row management
//!
//! This module handles parsing CSV data, validating against schemas,
//! and efficiently storing data in RocksDB with ID-based keys and time-series optimization.

use chrono::{DateTime, Utc};
use rocksdb::{WriteBatch, DB};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::error::{PulsoraError, Result};
use crate::storage::calculate_table_hash;
use crate::storage::columnar::ColumnBlock;
use crate::storage::id_manager::{IdManager, IdManagerRegistry, RowId};
use crate::storage::schema::Schema;
use rayon::prelude::*;

pub fn parse_csv(csv_data: &str) -> Result<Vec<HashMap<String, String>>> {
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();
    let header_count = headers.len();
    let mut rows = Vec::new();

    for result in reader.records() {
        let record = result?;
        let mut row = HashMap::with_capacity(header_count); // Pre-allocate capacity

        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                row.insert(header.to_string(), field.to_string());
            }
        }

        if !row.is_empty() {
            rows.push(row);
        }
    }

    Ok(rows)
}

pub fn process_csv_parallel(
    csv_data: &str,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
    threads: usize,
) -> Result<Vec<(u64, HashMap<String, String>)>> {
    // 1. Extract header
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();
    let header_count = headers.len();

    // Find where the data starts (after the first newline)
    let data_start = match csv_data.find('\n') {
        Some(i) => i + 1,
        None => return Ok(Vec::new()),
    };

    if data_start >= csv_data.len() {
        return Ok(Vec::new());
    }

    let data_str = &csv_data[data_start..];
    let data_len = data_str.len();

    // 2. Determine chunk size
    let num_threads = if threads == 0 {
        rayon::current_num_threads()
    } else {
        threads
    };

    // If data is small, don't bother splitting
    if data_len < 1024 * 1024 || num_threads <= 1 {
        return process_csv_single_thread(csv_data, schema, id_manager);
    }

    let target_chunk_size = data_len / num_threads;
    let mut chunks = Vec::with_capacity(num_threads);
    let mut start = 0;

    // 3. Split into chunks respecting quotes
    while start < data_len {
        let mut end = std::cmp::min(start + target_chunk_size, data_len);
        let mut in_quote = false;

        // Scan from start to find the split point
        let mut current = start;
        let bytes = data_str.as_bytes();

        while current < data_len {
            let b = bytes[current];
            if b == b'"' {
                in_quote = !in_quote;
            } else if b == b'\n' && !in_quote && current >= end {
                end = current;
                break;
            }
            current += 1;
        }

        if current >= data_len {
            end = data_len;
        }

        if start < end {
            chunks.push(&data_str[start..end]);
        }
        start = end + 1; // Skip the newline
    }

    // 4. Process chunks in parallel
    let results: Result<Vec<Vec<_>>> = chunks
        .par_iter()
        .map(|chunk_str| {
            let mut chunk_rows = Vec::with_capacity(chunk_str.len() / 100); // Estimate row count
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(chunk_str.as_bytes());

            for result in reader.records() {
                let record = result?;
                let mut row = HashMap::with_capacity(header_count);

                for (i, field) in record.iter().enumerate() {
                    if let Some(header) = headers.get(i) {
                        row.insert(header.to_string(), field.to_string());
                    }
                }

                if !row.is_empty() {
                    // ID Assignment
                    let row_id = if let Some(id_str) = row.get(&schema.id_column) {
                        if id_str.trim().is_empty() {
                            RowId::Auto
                        } else {
                            let parsed_id = RowId::from_string(id_str)?;
                            if let RowId::User(id) = parsed_id {
                                id_manager.register_user_id(id)?;
                            }
                            parsed_id
                        }
                    } else {
                        RowId::Auto
                    };

                    let actual_id = row_id.resolve(id_manager);
                    row.insert(schema.id_column.clone(), actual_id.to_string());

                    // Validation
                    schema.validate_row(&row)?;

                    chunk_rows.push((actual_id, row));
                }
            }
            Ok(chunk_rows)
        })
        .collect();

    // 5. Flatten results
    let mut final_rows = Vec::with_capacity(
        results
            .as_ref()
            .map(|v| v.iter().map(|c| c.len()).sum())
            .unwrap_or(0),
    );
    for chunk_res in results? {
        final_rows.extend(chunk_res);
    }

    Ok(final_rows)
}

fn process_csv_single_thread(
    csv_data: &str,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<Vec<(u64, HashMap<String, String>)>> {
    let rows = parse_csv(csv_data)?;
    let mut processed_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let row_id = if let Some(id_str) = row.get(&schema.id_column) {
            if id_str.trim().is_empty() {
                RowId::Auto
            } else {
                let parsed_id = RowId::from_string(id_str)?;
                if let RowId::User(id) = parsed_id {
                    id_manager.register_user_id(id)?;
                }
                parsed_id
            }
        } else {
            RowId::Auto
        };

        let actual_id = row_id.resolve(id_manager);
        row.insert(schema.id_column.clone(), actual_id.to_string());
        schema.validate_row(&row)?;
        processed_rows.push((actual_id, row));
    }
    Ok(processed_rows)
}
pub fn write_batch_to_rocksdb(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    rows: &[(u64, HashMap<String, String>)],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Create column block for this batch
    let column_block = ColumnBlock::from_rows(rows, schema)?;
    let serialized_block = column_block.serialize()?;

    let mut batch = WriteBatch::default();

    // Generate a unique block ID for this chunk
    let timestamp = Utc::now().timestamp_millis();
    let block_id = format!("_block_{}_{}", table, uuid::Uuid::new_v4());

    // Store the compressed block ONCE with the block ID
    batch.put(block_id.as_bytes(), &serialized_block);

    // Get min and max timestamps from this block for range indexing
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;

    for (_, row) in rows.iter() {
        if let Some(ts_col) = schema.get_timestamp_column() {
            if let Some(ts_value) = row.get(ts_col) {
                if let Ok(ts) = parse_timestamp(ts_value) {
                    min_timestamp = min_timestamp.min(ts);
                    max_timestamp = max_timestamp.max(ts);
                }
            }
        }
    }

    // If no timestamp column, use current time
    if min_timestamp == i64::MAX {
        min_timestamp = timestamp;
        max_timestamp = timestamp;
    }

    // CRITICAL OPTIMIZATION: Store block-level index for fast range queries
    // Block index key: [table_hash:u32][B][min_timestamp:i64][block_id]
    // Appending block_id ensures uniqueness even if timestamps collide
    let mut block_index_key = Vec::with_capacity(13 + block_id.len());
    let table_hash = calculate_table_hash(table);
    block_index_key.extend_from_slice(&table_hash.to_be_bytes());
    block_index_key.push(b'B'); // Block marker to distinguish from row keys
    block_index_key.extend_from_slice(&min_timestamp.to_be_bytes());
    block_index_key.extend_from_slice(block_id.as_bytes());

    // Block index value: [block_id_len][block_id][min_ts][max_ts][row_count]
    let mut block_index_value = Vec::with_capacity(4 + block_id.len() + 16 + 4);
    block_index_value.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
    block_index_value.extend_from_slice(block_id.as_bytes());
    block_index_value.extend_from_slice(&min_timestamp.to_le_bytes());
    block_index_value.extend_from_slice(&max_timestamp.to_le_bytes());
    block_index_value.extend_from_slice(&(rows.len() as u32).to_le_bytes());

    batch.put(&block_index_key, &block_index_value);

    // For each row, store references using dual key strategy
    for (row_idx, (id, row)) in rows.iter().enumerate() {
        // Create reference data: [marker][block_id_len][block_id][row_idx]
        let mut ref_data = Vec::with_capacity(1 + 4 + block_id.len() + 4);
        ref_data.push(0xFF); // Reference marker
        ref_data.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
        ref_data.extend_from_slice(block_id.as_bytes());
        ref_data.extend_from_slice(&(row_idx as u32).to_le_bytes());

        // Primary key: [table_hash:u32][id:u64] - for direct ID lookups
        // For REPLACE semantics, we simply overwrite the existing key
        let id_key = generate_id_key(table, *id);
        batch.put(&id_key, &ref_data);

        // Time index key: [table_hash:u32][timestamp:i64][id:u64] - for time-range queries
        let time_key = generate_time_key(table, schema, row, *id)?;
        batch.put(&time_key, &ref_data);
    }

    db.write(batch)?;
    debug!("Wrote column block with {} rows", rows.len());

    Ok(rows.len() as u64)
}

#[allow(dead_code)]
pub fn insert_rows(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    id_managers: &mut IdManagerRegistry,
    rows: Vec<HashMap<String, String>>,
    batch_size: usize,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Get ID manager for this table
    let id_manager = id_managers.get_or_create(table)?;

    // Process rows and assign IDs with pre-allocation
    let mut processed_rows = Vec::with_capacity(rows.len());
    for mut row in rows {
        // Handle ID assignment
        let row_id = if let Some(id_str) = row.get(&schema.id_column) {
            if id_str.trim().is_empty() {
                // Empty ID - auto assign
                RowId::Auto
            } else {
                // Parse user-provided ID
                let parsed_id = RowId::from_string(id_str)?;
                if let RowId::User(id) = parsed_id {
                    id_manager.register_user_id(id)?;
                }
                parsed_id
            }
        } else {
            // Missing ID column - auto assign
            RowId::Auto
        };

        // Resolve ID to actual value
        let actual_id = row_id.resolve(&id_manager);

        // Set the ID in the row data
        row.insert(schema.id_column.clone(), actual_id.to_string());

        // Validate the complete row
        schema.validate_row(&row)?;

        processed_rows.push((actual_id, row));
    }

    // Process rows in chunks for columnar storage
    let chunks: Vec<_> = processed_rows.chunks(batch_size).collect();

    // PERFORMANCE OPTIMIZATION: Process chunks in parallel for massive speedup
    // Each chunk creates its column block independently
    use rayon::prelude::*;

    let chunk_results: Result<Vec<_>> = chunks
        .par_iter()
        .map(|chunk| write_batch_to_rocksdb(db, table, schema, chunk))
        .collect();

    // Process results and write batches
    let chunk_results = chunk_results?;
    let total_inserted = chunk_results.iter().sum();

    Ok(total_inserted)
}

/// Generate primary key for direct ID lookup: [table_hash:u32][id:u64]
fn generate_id_key(table: &str, id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(12); // 4 + 8 bytes

    // Table hash (4 bytes) - big-endian for correct ordering
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // ID (8 bytes) - big-endian for correct ordering
    key.extend_from_slice(&id.to_be_bytes());

    key
}

/// Generate time index key: [table_hash:u32][timestamp:i64][id:u64]
fn generate_time_key(
    table: &str,
    schema: &Schema,
    row: &HashMap<String, String>,
    id: u64,
) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(20); // 4 + 8 + 8 bytes

    // Table hash (4 bytes) - big-endian for correct ordering
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // Timestamp (8 bytes) - big-endian for correct ordering
    let timestamp = if let Some(ts_col) = schema.get_timestamp_column() {
        if let Some(ts_value) = row.get(ts_col) {
            parse_timestamp(ts_value)?
        } else {
            Utc::now().timestamp_millis()
        }
    } else {
        Utc::now().timestamp_millis()
    };
    key.extend_from_slice(&timestamp.to_be_bytes());

    // ID (8 bytes) - for uniqueness and ordering
    key.extend_from_slice(&id.to_be_bytes());

    Ok(key)
}

pub fn parse_timestamp(value: &str) -> Result<i64> {
    // Try parsing as Unix timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        // Check if it's in seconds (10 digits) or milliseconds (13 digits)
        if timestamp > 1_000_000_000 && timestamp < 10_000_000_000 {
            return Ok(timestamp * 1000); // Convert seconds to milliseconds
        } else if timestamp > 1_000_000_000_000 && timestamp < 10_000_000_000_000 {
            return Ok(timestamp); // Already in milliseconds
        }
    }

    // Try parsing as RFC3339 first (handles Z suffix)
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis());
    }

    // Try parsing as naive datetime and assume UTC
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Ok(naive_dt.and_utc().timestamp_millis());
    }

    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(naive_dt.and_utc().timestamp_millis());
    }

    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis());
    }

    Err(PulsoraError::InvalidData(format!(
        "Invalid timestamp format: {}",
        value
    )))
}

#[cfg(test)]
#[path = "ingestion_test.rs"]
mod ingestion_test;
