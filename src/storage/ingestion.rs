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
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::ipc::reader::StreamReader;
use prost::Message;
use rayon::prelude::*;
use std::io::Cursor;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoRow {
    #[prost(map = "string, string", tag = "1")]
    pub values: HashMap<String, String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoBatch {
    #[prost(message, repeated, tag = "1")]
    pub rows: Vec<ProtoRow>,
}

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

fn process_single_row(
    mut row: HashMap<String, String>,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<(u64, HashMap<String, String>)> {
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

    Ok((actual_id, row))
}

pub fn process_rows_parallel(
    rows: Vec<HashMap<String, String>>,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<Vec<(u64, HashMap<String, String>)>> {
    rows.into_par_iter()
        .map(|row| process_single_row(row, schema, id_manager))
        .collect()
}

pub fn parse_arrow(arrow_data: &[u8]) -> Result<Vec<HashMap<String, String>>> {
    let cursor = Cursor::new(arrow_data);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        PulsoraError::InvalidData(format!("Failed to create Arrow stream reader: {}", e))
    })?;

    let mut all_rows = Vec::new();

    for batch in reader {
        let batch = batch
            .map_err(|e| PulsoraError::InvalidData(format!("Failed to read Arrow batch: {}", e)))?;

        let schema_ref = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut row_map = HashMap::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let field = schema_ref.field(col_idx);
                let col_name = field.name();
                let column = batch.column(col_idx);

                if column.is_null(row_idx) {
                    continue;
                }

                let value_str = match column.data_type() {
                    ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Int64 => {
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Boolean => {
                        let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    _ => {
                        // Fallback for other types: try to cast to string or just skip/error
                        // For now, let's skip unsupported types or try debug format
                        // A robust implementation would handle all types
                        continue;
                    }
                };
                row_map.insert(col_name.clone(), value_str);
            }
            if !row_map.is_empty() {
                all_rows.push(row_map);
            }
        }
    }
    Ok(all_rows)
}

pub fn parse_protobuf(proto_data: &[u8]) -> Result<Vec<HashMap<String, String>>> {
    let batch = ProtoBatch::decode(proto_data)
        .map_err(|e| PulsoraError::InvalidData(format!("Failed to decode Protobuf data: {}", e)))?;
    Ok(batch.rows.into_iter().map(|r| r.values).collect())
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
                    let processed = process_single_row(row, schema, id_manager)?;
                    chunk_rows.push(processed);
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

    for row in rows {
        let processed = process_single_row(row, schema, id_manager)?;
        processed_rows.push(processed);
    }
    Ok(processed_rows)
}

pub fn write_column_block_to_rocksdb(
    db: &Arc<DB>,
    table: &str,
    column_block: &ColumnBlock,
    ids: &[u64],
    min_timestamp: i64,
    max_timestamp: i64,
    timestamps: Option<&[i64]>,
) -> Result<u64> {
    let serialized_block = column_block.serialize()?;
    let mut batch = WriteBatch::default();

    // Generate a unique block ID for this chunk
    let block_id = format!("_block_{}_{}", table, uuid::Uuid::new_v4());

    // Store the compressed block ONCE with the block ID
    batch.put(block_id.as_bytes(), &serialized_block);

    // CRITICAL OPTIMIZATION: Store block-level index for fast range queries
    // Block index key: [table_hash:u32][B][min_timestamp:i64][block_id]
    let mut block_index_key = Vec::with_capacity(13 + block_id.len());
    let table_hash = calculate_table_hash(table);
    block_index_key.extend_from_slice(&table_hash.to_be_bytes());
    block_index_key.push(b'B'); // Block marker
    block_index_key.extend_from_slice(&min_timestamp.to_be_bytes());
    block_index_key.extend_from_slice(block_id.as_bytes());

    // Block index value: [block_id_len][block_id][min_ts][max_ts][row_count]
    let mut block_index_value = Vec::with_capacity(4 + block_id.len() + 16 + 4);
    block_index_value.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
    block_index_value.extend_from_slice(block_id.as_bytes());
    block_index_value.extend_from_slice(&min_timestamp.to_le_bytes());
    block_index_value.extend_from_slice(&max_timestamp.to_le_bytes());
    block_index_value.extend_from_slice(&(column_block.row_count as u32).to_le_bytes());

    batch.put(&block_index_key, &block_index_value);

    // For each row, store references using dual key strategy
    // Note: We don't have the full row map here, so we can't easily generate time_key for every row
    // UNLESS we extracted timestamps separately.
    // For now, we will skip per-row time index if we don't have the timestamp column easily accessible.
    // BUT, we need it for queries.
    // Solution: We should have extracted timestamps alongside IDs.

    // Let's assume we only index by ID for now in this fast path, OR we need to pass timestamps too.
    // For correctness, we should pass timestamps.

    for (row_idx, &id) in ids.iter().enumerate() {
        // Create reference data: [marker][block_id_len][block_id][row_idx]
        let mut ref_data = Vec::with_capacity(1 + 4 + block_id.len() + 4);
        ref_data.push(0xFF); // Reference marker
        ref_data.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
        ref_data.extend_from_slice(block_id.as_bytes());
        ref_data.extend_from_slice(&(row_idx as u32).to_le_bytes());

        // Primary key: [table_hash:u32][id:u64]
        let id_key = generate_id_key(table, id);
        batch.put(&id_key, &ref_data);

        // Time index key: [table_hash:u32][timestamp:i64][id:u64]
        // Only if we have timestamps
        if let Some(ts_list) = timestamps {
            if row_idx < ts_list.len() {
                let ts = ts_list[row_idx];
                let mut time_key = Vec::with_capacity(20);
                let table_hash = calculate_table_hash(table);
                time_key.extend_from_slice(&table_hash.to_be_bytes());
                time_key.extend_from_slice(&ts.to_be_bytes());
                time_key.extend_from_slice(&id.to_be_bytes());
                batch.put(&time_key, &ref_data);
            }
        }
    }

    db.write(batch)?;
    debug!(
        "Wrote column block with {} rows (Fast Path)",
        column_block.row_count
    );

    Ok(column_block.row_count as u64)
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
