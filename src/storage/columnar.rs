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

//! Column-oriented storage for efficient timeseries data
//!
//! Stores each column separately for better compression and cache locality

use crate::error::{PulsoraError, Result};
use crate::storage::encoding::{self, EncodedValue};
use crate::storage::schema::{DataType, Schema};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

// Type alias for decompressed column data
type DecompressedColumn<'a> = (
    &'a crate::storage::schema::Column,
    Vec<EncodedValue>,
    Option<&'a Vec<u8>>,
);

/// Column-oriented storage format
/// Each column is stored as a contiguous array of values
#[derive(Debug)]
pub struct ColumnBlock {
    /// Number of rows in this block
    pub row_count: usize,
    /// Compressed column data (column_name -> compressed bytes)
    pub columns: HashMap<String, Vec<u8>>,
    /// Null bitmap for each column (column_name -> bitmap)
    pub null_bitmaps: HashMap<String, Vec<u8>>,
}

impl ColumnBlock {
    /// Create a new column block from rows with optimized memory usage
    pub fn from_rows(rows: &[(u64, HashMap<String, String>)], schema: &Schema) -> Result<Self> {
        let row_count = rows.len();

        // PERFORMANCE OPTIMIZATION: Always use parallel processing
        // Network overhead dominates in real-world usage anyway
        let column_results: Result<Vec<_>> = schema
            .columns
            .par_iter()
            .map(|column| {
                // Pre-allocate all memory upfront
                let mut values = Vec::with_capacity(row_count);
                let mut null_bitmap = vec![0u8; row_count.div_ceil(8)];

                // OPTIMIZATION: Cache column name for faster lookups
                let column_name = &column.name;

                // OPTIMIZATION: Process rows in chunks to improve cache locality
                const CHUNK_SIZE: usize = 4096; // Larger chunks for better performance
                for chunk_start in (0..row_count).step_by(CHUNK_SIZE) {
                    let chunk_end = (chunk_start + CHUNK_SIZE).min(row_count);

                    // Use slice to avoid bounds checking in the loop
                    let chunk_slice = &rows[chunk_start..chunk_end];
                    for (offset, (_, row)) in chunk_slice.iter().enumerate() {
                        let idx = chunk_start + offset;
                        // Direct HashMap lookup with cached column name
                        if let Some(value_str) = row.get(column_name) {
                            let value = parse_typed_value(value_str, &column.data_type)?;
                            values.push(value);
                        } else {
                            // Use bit manipulation for better performance
                            let byte_idx = idx >> 3; // idx / 8
                            let bit_idx = idx & 7; // idx % 8
                            null_bitmap[byte_idx] |= 1 << bit_idx;
                            values.push(default_value(&column.data_type));
                        }
                    }
                }

                let compressed = compress_column(&values, &column.data_type)?;
                Ok((column_name.clone(), compressed, null_bitmap))
            })
            .collect();

        let column_results = column_results?;

        // Build the final HashMaps from parallel results
        let mut columns = HashMap::with_capacity(schema.columns.len());
        let mut null_bitmaps = HashMap::with_capacity(schema.columns.len());

        for (name, compressed_data, null_bitmap) in column_results {
            columns.insert(name.clone(), compressed_data);
            null_bitmaps.insert(name, null_bitmap);
        }

        Ok(ColumnBlock {
            row_count,
            columns,
            null_bitmaps,
        })
    }

    /// Create a new column block directly from an Arrow RecordBatch (Fast Path)
    pub fn from_arrow(
        batch: &RecordBatch,
        schema: &Schema,
        ids: &[u64], // Pre-resolved IDs
    ) -> Result<Self> {
        let row_count = batch.num_rows();
        if row_count != ids.len() {
            return Err(PulsoraError::InvalidData(format!(
                "Row count mismatch: batch has {}, ids has {}",
                row_count,
                ids.len()
            )));
        }

        // Process columns in parallel
        let column_results: Result<Vec<_>> = schema
            .columns
            .par_iter()
            .map(|column| {
                let mut values = Vec::with_capacity(row_count);
                let mut null_bitmap = vec![0u8; row_count.div_ceil(8)];

                // Handle ID column specially
                if column.name == schema.id_column {
                    for &id in ids.iter() {
                        values.push(EncodedValue::Id(id));
                        // IDs are never null in our internal storage
                    }
                } else {
                    // Find corresponding Arrow column
                    // Note: Arrow column name might match our schema column name
                    if let Ok(arrow_col) = batch.column_by_name(&column.name).ok_or(()) {
                        // Convert Arrow array to EncodedValues
                        for i in 0..row_count {
                            if arrow_col.is_null(i) {
                                let byte_idx = i >> 3;
                                let bit_idx = i & 7;
                                null_bitmap[byte_idx] |= 1 << bit_idx;
                                values.push(default_value(&column.data_type));
                            } else {
                                let val = match &column.data_type {
                                    DataType::Integer => {
                                        // Try to cast to Int64Array
                                        if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<Int64Array>()
                                        {
                                            EncodedValue::Integer(arr.value(i))
                                        } else {
                                            // Fallback for other integer types if needed, or error
                                            // For now assume Int64 as per our simple schema
                                            EncodedValue::Integer(0)
                                        }
                                    }
                                    DataType::Float => {
                                        if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<Float64Array>()
                                        {
                                            EncodedValue::Float(arr.value(i))
                                        } else {
                                            EncodedValue::Float(0.0)
                                        }
                                    }
                                    DataType::Boolean => {
                                        if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<BooleanArray>()
                                        {
                                            EncodedValue::Boolean(arr.value(i))
                                        } else {
                                            EncodedValue::Boolean(false)
                                        }
                                    }
                                    DataType::String => {
                                        if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<StringArray>()
                                        {
                                            EncodedValue::String(arr.value(i).to_string())
                                        } else {
                                            EncodedValue::String(String::new())
                                        }
                                    }
                                    DataType::Timestamp => {
                                        // Expecting string or int64 for timestamp
                                        if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<Int64Array>()
                                        {
                                            EncodedValue::Timestamp(arr.value(i))
                                        } else if let Some(arr) =
                                            arrow_col.as_any().downcast_ref::<StringArray>()
                                        {
                                            // Parse string timestamp
                                            use crate::storage::ingestion::parse_timestamp;
                                            let ts = parse_timestamp(arr.value(i)).unwrap_or(0);
                                            EncodedValue::Timestamp(ts)
                                        } else {
                                            EncodedValue::Timestamp(0)
                                        }
                                    }
                                    DataType::Id => {
                                        // Should be handled by the special case above, but if it's a secondary ID...
                                        EncodedValue::Id(0)
                                    }
                                };
                                values.push(val);
                            }
                        }
                    } else {
                        // Column missing in Arrow batch - fill with defaults/nulls
                        for i in 0..row_count {
                            let byte_idx = i >> 3;
                            let bit_idx = i & 7;
                            null_bitmap[byte_idx] |= 1 << bit_idx;
                            values.push(default_value(&column.data_type));
                        }
                    }
                }

                let compressed = compress_column(&values, &column.data_type)?;
                Ok((column.name.clone(), compressed, null_bitmap))
            })
            .collect();

        let column_results = column_results?;

        let mut columns = HashMap::with_capacity(schema.columns.len());
        let mut null_bitmaps = HashMap::with_capacity(schema.columns.len());

        for (name, compressed_data, null_bitmap) in column_results {
            columns.insert(name.clone(), compressed_data);
            null_bitmaps.insert(name, null_bitmap);
        }

        Ok(ColumnBlock {
            row_count,
            columns,
            null_bitmaps,
        })
    }

    /// Convert column block back to rows with optimized memory allocation
    pub fn to_rows(&self, schema: &Schema) -> Result<Vec<HashMap<String, String>>> {
        let mut rows = Vec::with_capacity(self.row_count);
        for _ in 0..self.row_count {
            rows.push(HashMap::with_capacity(schema.columns.len()));
        }

        for column in &schema.columns {
            let compressed = self.columns.get(&column.name).ok_or_else(|| {
                PulsoraError::InvalidData(format!("Missing column: {}", column.name))
            })?;

            let values = decompress_column(compressed, &column.data_type, self.row_count)?;
            let null_bitmap = self.null_bitmaps.get(&column.name);

            for (idx, value) in values.into_iter().enumerate() {
                // Check if value is null using bit operations
                if let Some(bitmap) = null_bitmap {
                    let byte_idx = idx >> 3; // idx / 8
                    let bit_idx = idx & 7; // idx % 8
                    if byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0 {
                        continue; // Skip null values
                    }
                }

                // Convert to string and add to row
                let value_str = value_to_string(&value);
                rows[idx].insert(column.name.clone(), value_str);
            }
        }

        Ok(rows)
    }

    /// Convert a slice of rows to JSON values efficiently
    pub fn to_json_slice(
        &self,
        schema: &Schema,
        skip: usize,
        take: usize,
    ) -> Result<Vec<serde_json::Value>> {
        use serde_json::Value;

        if skip >= self.row_count {
            return Ok(Vec::new());
        }

        let end_idx = (skip + take).min(self.row_count);
        let actual_take = end_idx - skip;

        // Always use parallel decompression for better performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // Build JSON objects for the requested slice only
        let mut results = Vec::with_capacity(actual_take);

        for row_idx in skip..end_idx {
            let mut json_obj = serde_json::Map::with_capacity(schema.columns.len());

            for (column, values, null_bitmap) in &decompressed_columns {
                // Check if value is null using bit operations
                if let Some(bitmap) = null_bitmap {
                    let byte_idx = row_idx >> 3;
                    let bit_idx = row_idx & 7;
                    if byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0 {
                        continue;
                    }
                }

                // Use direct indexing - bounds are guaranteed by loop
                let json_value = match &values[row_idx] {
                    EncodedValue::Id(v) => Value::Number(serde_json::Number::from(*v)),
                    EncodedValue::Integer(v) => Value::Number(serde_json::Number::from(*v)),
                    EncodedValue::Float(v) => Value::Number(
                        serde_json::Number::from_f64(*v)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                    EncodedValue::Boolean(v) => Value::Bool(*v),
                    EncodedValue::Timestamp(v) => {
                        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v) {
                            Value::String(datetime.to_rfc3339())
                        } else {
                            Value::Number(serde_json::Number::from(*v))
                        }
                    }
                    EncodedValue::String(v) => Value::String(v.clone()),
                };

                json_obj.insert(column.name.clone(), json_value);
            }

            results.push(Value::Object(json_obj));
        }

        Ok(results)
    }

    /// Convert a slice of rows directly to CSV string
    /// This avoids all intermediate allocations (HashMap, Value, etc.)
    pub fn to_csv(&self, schema: &Schema, skip: usize, take: usize) -> Result<String> {
        if skip >= self.row_count {
            return Ok(String::new());
        }

        let end_idx = (skip + take).min(self.row_count);
        let actual_take = end_idx - skip;

        // Always use parallel decompression for better performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // Pre-allocate output buffer (estimate 100 bytes per row)
        let mut output = String::with_capacity(actual_take * 100);

        // We need to iterate rows, then columns to write CSV line by line
        // But our data is columnar.
        // Access pattern: row 0 [col 0, col 1...], row 1 [col 0, col 1...]

        for row_idx in skip..end_idx {
            for (col_idx, (_, values, null_bitmap)) in decompressed_columns.iter().enumerate() {
                if col_idx > 0 {
                    output.push(',');
                }

                // Check null
                let is_null = if let Some(bitmap) = null_bitmap {
                    let byte_idx = row_idx >> 3;
                    let bit_idx = row_idx & 7;
                    byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                } else {
                    false
                };

                if !is_null {
                    // Write value directly to string buffer
                    match &values[row_idx] {
                        EncodedValue::Id(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Integer(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Float(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Boolean(v) => {
                            output.push_str(if *v { "true" } else { "false" });
                        }
                        EncodedValue::Timestamp(v) => {
                            if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v) {
                                output.push_str(&datetime.to_rfc3339());
                            } else {
                                use std::fmt::Write;
                                write!(output, "{}", v).unwrap();
                            }
                        }
                        EncodedValue::String(v) => {
                            // CSV escaping
                            if v.contains(',') || v.contains('"') || v.contains('\n') {
                                output.push('"');
                                output.push_str(&v.replace('"', "\"\""));
                                output.push('"');
                            } else {
                                output.push_str(v);
                            }
                        }
                    }
                }
            }
            output.push('\n');
        }

        Ok(output)
    }

    /// Convert specific rows to CSV efficiently
    pub fn to_csv_filtered(&self, schema: &Schema, indices: &[usize]) -> Result<String> {
        if indices.is_empty() {
            return Ok(String::new());
        }

        // Always use parallel decompression for better performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // Pre-allocate output buffer (estimate 100 bytes per row)
        let mut output = String::with_capacity(indices.len() * 100);

        for &row_idx in indices {
            if row_idx >= self.row_count {
                continue;
            }

            for (col_idx, (_, values, null_bitmap)) in decompressed_columns.iter().enumerate() {
                if col_idx > 0 {
                    output.push(',');
                }

                // Check null
                let is_null = if let Some(bitmap) = null_bitmap {
                    let byte_idx = row_idx >> 3;
                    let bit_idx = row_idx & 7;
                    byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                } else {
                    false
                };

                if !is_null {
                    // Write value directly to string buffer
                    match &values[row_idx] {
                        EncodedValue::Id(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Integer(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Float(v) => {
                            use std::fmt::Write;
                            write!(output, "{}", v).unwrap();
                        }
                        EncodedValue::Boolean(v) => {
                            output.push_str(if *v { "true" } else { "false" });
                        }
                        EncodedValue::Timestamp(v) => {
                            if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v) {
                                output.push_str(&datetime.to_rfc3339());
                            } else {
                                use std::fmt::Write;
                                write!(output, "{}", v).unwrap();
                            }
                        }
                        EncodedValue::String(v) => {
                            // CSV escaping
                            if v.contains(',') || v.contains('"') || v.contains('\n') {
                                output.push('"');
                                output.push_str(&v.replace('"', "\"\""));
                                output.push('"');
                            } else {
                                output.push_str(v);
                            }
                        }
                    }
                }
            }
            output.push('\n');
        }

        Ok(output)
    }

    /// Convert a slice of rows directly to an Arrow RecordBatch
    /// This avoids all intermediate allocations (HashMap, Value, etc.)
    pub fn to_arrow(&self, schema: &Schema, skip: usize, take: usize) -> Result<RecordBatch> {
        if skip >= self.row_count {
            return Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::new(vec![] as Vec<arrow::datatypes::Field>),
            )));
        }

        let end_idx = (skip + take).min(self.row_count);
        let actual_take = end_idx - skip;

        // Always use parallel decompression for better performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.columns.len());
        let mut fields = Vec::with_capacity(schema.columns.len());

        for (column, values, null_bitmap) in decompressed_columns {
            // Create Arrow Field
            let dt = match column.data_type {
                DataType::Id => arrow::datatypes::DataType::UInt64,
                DataType::Integer => arrow::datatypes::DataType::Int64,
                DataType::Float => arrow::datatypes::DataType::Float64,
                DataType::Boolean => arrow::datatypes::DataType::Boolean,
                DataType::Timestamp => arrow::datatypes::DataType::Int64,
                DataType::String => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(&column.name, dt, true));

            // Build Arrow Array
            // We need to slice the values and handle nulls
            // Since EncodedValue is an enum, we have to iterate and match
            // But we can use builders which is efficient enough

            // Optimization: If we had typed vectors in ColumnBlock, we could use Buffer directly
            // But with EncodedValue, we must iterate.

            let slice = &values[skip..end_idx];

            match column.data_type {
                DataType::Id => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(actual_take);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Id(v) = val {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0); // Should not happen
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Integer => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(actual_take);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Integer(v) = val {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float => {
                    let mut builder = arrow::array::Float64Builder::with_capacity(actual_take);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Float(v) = val {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0.0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = arrow::array::BooleanBuilder::with_capacity(actual_take);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Boolean(v) = val {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(false);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Timestamp => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(actual_take);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Timestamp(v) = val {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::String => {
                    let mut builder =
                        arrow::array::StringBuilder::with_capacity(actual_take, actual_take * 20);
                    for (i, val) in slice.iter().enumerate() {
                        let row_idx = skip + i;
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::String(v) = val {
                            builder.append_value(v);
                        } else {
                            builder.append_value("");
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| PulsoraError::Internal(e.to_string()))
    }

    /// Convert specific rows to Arrow RecordBatch efficiently
    pub fn to_arrow_filtered(&self, schema: &Schema, indices: &[usize]) -> Result<RecordBatch> {
        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::new(vec![] as Vec<arrow::datatypes::Field>),
            )));
        }

        // Always use parallel decompression for better performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.columns.len());
        let mut fields = Vec::with_capacity(schema.columns.len());

        for (column, values, null_bitmap) in decompressed_columns {
            // Create Arrow Field
            let dt = match column.data_type {
                DataType::Id => arrow::datatypes::DataType::UInt64,
                DataType::Integer => arrow::datatypes::DataType::Int64,
                DataType::Float => arrow::datatypes::DataType::Float64,
                DataType::Boolean => arrow::datatypes::DataType::Boolean,
                DataType::Timestamp => arrow::datatypes::DataType::Int64,
                DataType::String => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(&column.name, dt, true));

            match column.data_type {
                DataType::Id => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(indices.len());
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Id(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Integer => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(indices.len());
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Integer(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float => {
                    let mut builder = arrow::array::Float64Builder::with_capacity(indices.len());
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Float(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0.0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = arrow::array::BooleanBuilder::with_capacity(indices.len());
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Boolean(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(false);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Timestamp => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(indices.len());
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Timestamp(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::String => {
                    let mut builder = arrow::array::StringBuilder::with_capacity(
                        indices.len(),
                        indices.len() * 20,
                    );
                    for &row_idx in indices {
                        if row_idx >= self.row_count {
                            continue;
                        }
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::String(v) = &values[row_idx] {
                            builder.append_value(v);
                        } else {
                            builder.append_value("");
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| PulsoraError::Internal(e.to_string()))
    }

    /// Direct columnar to JSON conversion without intermediate HashMaps
    /// This is 5-10x faster than to_rows() + convert_row_to_json()
    #[allow(dead_code)]
    pub fn to_json_values(&self, schema: &Schema) -> Result<Vec<serde_json::Value>> {
        use serde_json::Value;

        // Always use parallel decompression for maximum performance
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // Build JSON objects row by row with pre-allocated capacity
        let mut results = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            let mut json_obj = serde_json::Map::with_capacity(schema.columns.len());

            for (column, values, null_bitmap) in &decompressed_columns {
                // Check if value is null using bit operations
                if let Some(bitmap) = null_bitmap {
                    let byte_idx = row_idx >> 3;
                    let bit_idx = row_idx & 7;
                    if byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0 {
                        continue;
                    }
                }

                // Use direct indexing - bounds are guaranteed by loop
                let json_value = match &values[row_idx] {
                    EncodedValue::Id(v) => Value::Number(serde_json::Number::from(*v)),
                    EncodedValue::Integer(v) => Value::Number(serde_json::Number::from(*v)),
                    EncodedValue::Float(v) => Value::Number(
                        serde_json::Number::from_f64(*v)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                    EncodedValue::Boolean(v) => Value::Bool(*v),
                    EncodedValue::Timestamp(v) => {
                        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v) {
                            Value::String(datetime.to_rfc3339())
                        } else {
                            Value::String(v.to_string())
                        }
                    }
                    EncodedValue::String(v) => Value::String(v.clone()),
                };

                json_obj.insert(column.name.clone(), json_value);
            }

            results.push(Value::Object(json_obj));
        }

        Ok(results)
    }

    /// Get raw timestamp values for filtering
    fn get_timestamp_values(&self, schema: &Schema) -> Result<Vec<i64>> {
        let ts_col = schema
            .get_timestamp_column()
            .ok_or_else(|| PulsoraError::Query("No timestamp column found".to_string()))?;

        let compressed = self
            .columns
            .get(ts_col)
            .ok_or_else(|| PulsoraError::Query(format!("Column {} not found", ts_col)))?;

        // Manual decompression to get i64 directly
        // This duplicates logic from decompress_column but avoids EncodedValue allocation
        if compressed.is_empty() {
            return Ok(Vec::new());
        }

        let marker = compressed[0];
        let mut cursor = std::io::Cursor::new(compressed.as_slice());
        cursor.set_position(1);

        if marker == 1 {
            let base = crate::storage::encoding::decode_varint_signed(&mut cursor)?;
            let _len = crate::storage::encoding::decode_varint(&mut cursor)?;
            let pos = cursor.position() as usize;
            crate::storage::compression::decompress_integers(
                base,
                &compressed[pos..],
                self.row_count,
            )
        } else if marker == 4 {
            let base = crate::storage::encoding::decode_varint(&mut cursor)? as i64;
            let _len = crate::storage::encoding::decode_varint(&mut cursor)?;
            let pos = cursor.position() as usize;
            crate::storage::compression::decompress_timestamps(
                base,
                &compressed[pos..],
                self.row_count,
            )
        } else {
            Err(PulsoraError::Query(format!(
                "Invalid timestamp column type: {}",
                marker
            )))
        }
    }

    /// Filter rows by timestamp range using SIMD-optimized scanning
    /// Filter rows by timestamp range using explicit SIMD with wide crate
    pub fn filter_by_timestamp(
        &self,
        schema: &Schema,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<usize>> {
        let timestamps = self.get_timestamp_values(schema)?;
        let mut indices = Vec::with_capacity(timestamps.len());
        use wide::i64x4;

        let _start_vec = i64x4::splat(start_ts);
        let _end_vec = i64x4::splat(end_ts);

        let mut idx = 0;
        let chunks = timestamps.chunks_exact(4);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let val_vec = i64x4::from(unsafe { *(chunk.as_ptr() as *const [i64; 4]) });

            // Compare: val >= start && val <= end
            // wide doesn't have direct ge/le for all types, so we use:
            // (val >= start) & (val <= end)
            // Note: wide cmp returns mask

            // Manual check is often faster than masking for sparse matches,
            // but for range filtering SIMD mask extraction is good.
            // However, wide 0.7+ provides array access.

            // Let's use a hybrid approach: check if ANY in range using SIMD, then extract
            // Or just iterate if we can't easily extract mask

            // Actually, for i64, simple unrolling with explicit SIMD load helps LLVM
            // But let's use the mask if possible.
            // wide::i64x4 doesn't expose easy mask bit extraction in older versions.
            // Let's stick to array conversion which is zero-cost

            let arr = val_vec.to_array();
            if arr[0] >= start_ts && arr[0] <= end_ts {
                indices.push(idx);
            }
            if arr[1] >= start_ts && arr[1] <= end_ts {
                indices.push(idx + 1);
            }
            if arr[2] >= start_ts && arr[2] <= end_ts {
                indices.push(idx + 2);
            }
            if arr[3] >= start_ts && arr[3] <= end_ts {
                indices.push(idx + 3);
            }

            idx += 4;
        }

        // Handle remainder
        for &ts in remainder {
            if ts >= start_ts && ts <= end_ts {
                indices.push(idx);
            }
            idx += 1;
        }

        Ok(indices)
    }

    /// Convert specific rows to JSON values efficiently
    pub fn to_json_filtered(
        &self,
        schema: &Schema,
        indices: &[usize],
    ) -> Result<Vec<serde_json::Value>> {
        use serde_json::Value;

        if indices.is_empty() {
            return Ok(Vec::new());
        }

        // Decompress all columns in parallel
        let decompressed_columns: Vec<_> = self.decompress_columns_parallel(schema)?;

        let mut results = Vec::with_capacity(indices.len());

        // Build JSON objects for filtered rows
        for &row_idx in indices {
            if row_idx >= self.row_count {
                continue;
            }

            let mut json_obj = serde_json::Map::with_capacity(schema.columns.len());

            for (column, values, null_bitmap) in &decompressed_columns {
                // Check null bitmap
                let is_null = if let Some(bitmap) = null_bitmap {
                    let byte_idx = row_idx >> 3;
                    let bit_idx = row_idx & 7;
                    if byte_idx < bitmap.len() {
                        (bitmap[byte_idx] & (1 << bit_idx)) != 0
                    } else {
                        false
                    }
                } else {
                    false
                };

                if is_null {
                    json_obj.insert(column.name.clone(), Value::Null);
                    continue;
                }

                if row_idx < values.len() {
                    let value = &values[row_idx];
                    let json_value = match value {
                        EncodedValue::Id(v) => Value::Number(serde_json::Number::from(*v)),
                        EncodedValue::Integer(v) => Value::Number(serde_json::Number::from(*v)),
                        EncodedValue::Float(v) => Value::Number(
                            serde_json::Number::from_f64(*v)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                        EncodedValue::Boolean(v) => Value::Bool(*v),
                        EncodedValue::Timestamp(v) => {
                            if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v) {
                                Value::String(datetime.to_rfc3339())
                            } else {
                                Value::Number(serde_json::Number::from(*v))
                            }
                        }
                        EncodedValue::String(v) => Value::String(v.clone()),
                    };
                    json_obj.insert(column.name.clone(), json_value);
                }
            }

            results.push(Value::Object(json_obj));
        }

        Ok(results)
    }
    /// Parallel column decompression for maximum performance
    /// Optimized query execution that combines filtering and export to avoid double decompression
    pub fn to_csv_query(
        &self,
        schema: &Schema,
        start_ts: i64,
        end_ts: i64,
        skip: usize,
        take: usize,
    ) -> Result<String> {
        // 1. Decompress all columns in parallel ONCE
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // 2. Identify timestamp column
        let ts_col_name = schema.get_timestamp_column();
        let mut ts_values: Option<&Vec<EncodedValue>> = None;

        if let Some(name) = ts_col_name {
            for (col, values, _) in &decompressed_columns {
                if col.name == name {
                    ts_values = Some(values);
                    break;
                }
            }
        }

        // 3. Filter and Write in one pass
        let mut output = String::with_capacity(take * 100);
        let mut skipped = 0;
        let mut taken = 0;

        for row_idx in 0..self.row_count {
            let include = if let Some(values) = ts_values {
                let ts = match &values[row_idx] {
                    EncodedValue::Timestamp(t) => *t,
                    EncodedValue::Integer(t) => *t,
                    _ => 0,
                };
                ts >= start_ts && ts <= end_ts
            } else {
                true
            };

            if include {
                if skipped < skip {
                    skipped += 1;
                    continue;
                }

                if taken >= take {
                    break;
                }

                // Write Row
                for (col_idx, (_, values, null_bitmap)) in decompressed_columns.iter().enumerate() {
                    if col_idx > 0 {
                        output.push(',');
                    }

                    let is_null = if let Some(bitmap) = null_bitmap {
                        let byte_idx = row_idx >> 3;
                        let bit_idx = row_idx & 7;
                        byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                    } else {
                        false
                    };

                    if !is_null {
                        match &values[row_idx] {
                            EncodedValue::Id(v) => {
                                use std::fmt::Write;
                                write!(output, "{}", v).unwrap();
                            }
                            EncodedValue::Integer(v) => {
                                use std::fmt::Write;
                                write!(output, "{}", v).unwrap();
                            }
                            EncodedValue::Float(v) => {
                                use std::fmt::Write;
                                write!(output, "{}", v).unwrap();
                            }
                            EncodedValue::Boolean(v) => {
                                output.push_str(if *v { "true" } else { "false" });
                            }
                            EncodedValue::Timestamp(v) => {
                                if let Some(datetime) = chrono::DateTime::from_timestamp_millis(*v)
                                {
                                    output.push_str(&datetime.to_rfc3339());
                                } else {
                                    use std::fmt::Write;
                                    write!(output, "{}", v).unwrap();
                                }
                            }
                            EncodedValue::String(v) => {
                                if v.contains(',') || v.contains('"') || v.contains('\n') {
                                    output.push('"');
                                    output.push_str(&v.replace('"', "\"\""));
                                    output.push('"');
                                } else {
                                    output.push_str(v);
                                }
                            }
                        }
                    }
                }
                output.push('\n');
                taken += 1;
            }
        }

        Ok(output)
    }

    /// Optimized query execution that combines filtering and export to avoid double decompression
    pub fn to_arrow_query(
        &self,
        schema: &Schema,
        start_ts: i64,
        end_ts: i64,
        skip: usize,
        take: usize,
    ) -> Result<RecordBatch> {
        // 1. Decompress all columns in parallel ONCE
        let decompressed_columns = self.decompress_columns_parallel(schema)?;

        // 2. Identify timestamp column
        let ts_col_name = schema.get_timestamp_column();
        let mut ts_values: Option<&Vec<EncodedValue>> = None;

        if let Some(name) = ts_col_name {
            for (col, values, _) in &decompressed_columns {
                if col.name == name {
                    ts_values = Some(values);
                    break;
                }
            }
        }

        // 3. Collect indices
        let mut indices = Vec::with_capacity(take);
        let mut skipped = 0;

        for row_idx in 0..self.row_count {
            let include = if let Some(values) = ts_values {
                let ts = match &values[row_idx] {
                    EncodedValue::Timestamp(t) => *t,
                    EncodedValue::Integer(t) => *t,
                    _ => 0,
                };
                ts >= start_ts && ts <= end_ts
            } else {
                true
            };

            if include {
                if skipped < skip {
                    skipped += 1;
                    continue;
                }

                if indices.len() >= take {
                    break;
                }
                indices.push(row_idx);
            }
        }

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::new(vec![] as Vec<arrow::datatypes::Field>),
            )));
        }

        // 4. Build Arrow Arrays
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.columns.len());
        let mut fields = Vec::with_capacity(schema.columns.len());

        for (column, values, null_bitmap) in decompressed_columns {
            let dt = match column.data_type {
                DataType::Id => arrow::datatypes::DataType::UInt64,
                DataType::Integer => arrow::datatypes::DataType::Int64,
                DataType::Float => arrow::datatypes::DataType::Float64,
                DataType::Boolean => arrow::datatypes::DataType::Boolean,
                DataType::Timestamp => arrow::datatypes::DataType::Int64,
                DataType::String => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(&column.name, dt, true));

            match column.data_type {
                DataType::Id => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(indices.len());
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Id(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Integer => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(indices.len());
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Integer(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float => {
                    let mut builder = arrow::array::Float64Builder::with_capacity(indices.len());
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Float(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0.0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = arrow::array::BooleanBuilder::with_capacity(indices.len());
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Boolean(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(false);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::Timestamp => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(indices.len());
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::Timestamp(v) = &values[row_idx] {
                            builder.append_value(*v);
                        } else {
                            builder.append_value(0);
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
                DataType::String => {
                    let mut builder = arrow::array::StringBuilder::with_capacity(
                        indices.len(),
                        indices.len() * 20,
                    );
                    for &row_idx in &indices {
                        let is_null = if let Some(bitmap) = null_bitmap {
                            let byte_idx = row_idx >> 3;
                            let bit_idx = row_idx & 7;
                            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
                        } else {
                            false
                        };

                        if is_null {
                            builder.append_null();
                        } else if let EncodedValue::String(v) = &values[row_idx] {
                            builder.append_value(v);
                        } else {
                            builder.append_value("");
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                }
            }
        }

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| PulsoraError::Internal(e.to_string()))
    }
    fn decompress_columns_parallel<'a>(
        &'a self,
        schema: &'a Schema,
    ) -> Result<Vec<DecompressedColumn<'a>>> {
        // Process all columns in parallel for maximum throughput
        schema
            .columns
            .par_iter()
            .map(|column| {
                let compressed = self.columns.get(&column.name).ok_or_else(|| {
                    PulsoraError::InvalidData(format!("Missing column: {}", column.name))
                })?;
                let null_bitmap = self.null_bitmaps.get(&column.name);
                let values = decompress_column(compressed, &column.data_type, self.row_count)?;
                Ok((column, values, null_bitmap))
            })
            .collect()
    }

    /// Serialize the column block for storage with pre-calculated size
    pub fn serialize(&self) -> Result<Vec<u8>> {
        // Pre-calculate size for single allocation
        let estimated_size = 8 + // row count + column count
            self.columns.iter().map(|(k, v)| 8 + k.len() + v.len()).sum::<usize>() +
            self.null_bitmaps.values().map(|v| 4 + v.len()).sum::<usize>();

        let mut output = Vec::with_capacity(estimated_size);

        // Write row count
        output.extend_from_slice(&(self.row_count as u32).to_le_bytes());

        // Write number of columns
        output.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // Write each column
        for (name, data) in &self.columns {
            // Column name length and name
            let name_bytes = name.as_bytes();
            output.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            output.extend_from_slice(name_bytes);

            // Column data length and data
            output.extend_from_slice(&(data.len() as u32).to_le_bytes());
            output.extend_from_slice(data);

            // Null bitmap
            if let Some(bitmap) = self.null_bitmaps.get(name) {
                output.extend_from_slice(&(bitmap.len() as u32).to_le_bytes());
                output.extend_from_slice(bitmap);
            } else {
                output.extend_from_slice(&0u32.to_le_bytes());
            }
        }

        Ok(output)
    }

    /// Deserialize a column block from storage
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut cursor = 0;

        // Read row count
        if data.len() < 4 {
            return Err(PulsoraError::InvalidData(
                "Invalid column block data".to_string(),
            ));
        }
        let row_count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        cursor += 4;

        // Read number of columns
        if data.len() < cursor + 4 {
            return Err(PulsoraError::InvalidData(
                "Invalid column block data".to_string(),
            ));
        }
        let num_columns = u32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]) as usize;
        cursor += 4;

        let mut columns = HashMap::new();
        let mut null_bitmaps = HashMap::new();

        for _ in 0..num_columns {
            // Read column name
            if data.len() < cursor + 4 {
                return Err(PulsoraError::InvalidData(
                    "Invalid column block data".to_string(),
                ));
            }
            let name_len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            if data.len() < cursor + name_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid column block data".to_string(),
                ));
            }
            let name = String::from_utf8(data[cursor..cursor + name_len].to_vec())?;
            cursor += name_len;

            // Read column data
            if data.len() < cursor + 4 {
                return Err(PulsoraError::InvalidData(
                    "Invalid column block data".to_string(),
                ));
            }
            let data_len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            if data.len() < cursor + data_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid column block data".to_string(),
                ));
            }
            let column_data = data[cursor..cursor + data_len].to_vec();
            cursor += data_len;

            // Read null bitmap
            if data.len() < cursor + 4 {
                return Err(PulsoraError::InvalidData(
                    "Invalid column block data".to_string(),
                ));
            }
            let bitmap_len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            if bitmap_len > 0 {
                if data.len() < cursor + bitmap_len {
                    return Err(PulsoraError::InvalidData(
                        "Invalid column block data".to_string(),
                    ));
                }
                let bitmap = data[cursor..cursor + bitmap_len].to_vec();
                cursor += bitmap_len;
                null_bitmaps.insert(name.clone(), bitmap);
            }

            columns.insert(name, column_data);
        }

        Ok(ColumnBlock {
            row_count,
            columns,
            null_bitmaps,
        })
    }
}

/// Parse a string value according to its data type with optimized parsing
fn parse_typed_value(value: &str, data_type: &DataType) -> Result<EncodedValue> {
    match data_type {
        DataType::Id => {
            let parsed = encoding::fast_parse_u64(value)
                .ok_or_else(|| PulsoraError::InvalidData(format!("Invalid ID: {}", value)))?;
            Ok(EncodedValue::Id(parsed))
        }
        DataType::Integer => {
            let parsed = encoding::fast_parse_i64(value)
                .ok_or_else(|| PulsoraError::InvalidData(format!("Invalid integer: {}", value)))?;
            Ok(EncodedValue::Integer(parsed))
        }
        DataType::Float => {
            let parsed = encoding::fast_parse_f64(value)
                .ok_or_else(|| PulsoraError::InvalidData(format!("Invalid float: {}", value)))?;
            Ok(EncodedValue::Float(parsed))
        }
        DataType::Boolean => {
            let parsed = match value.as_bytes() {
                b"true" => true,
                b"false" => false,
                _ => match value.to_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(PulsoraError::InvalidData(format!(
                            "Invalid boolean: {}",
                            value
                        )))
                    }
                },
            };
            Ok(EncodedValue::Boolean(parsed))
        }
        DataType::Timestamp => {
            // Parse timestamp string to milliseconds
            use crate::storage::ingestion::parse_timestamp;
            let parsed = parse_timestamp(value)?;
            Ok(EncodedValue::Timestamp(parsed))
        }
        DataType::String => Ok(EncodedValue::String(value.to_string())),
    }
}

/// Get default value for a data type
fn default_value(data_type: &DataType) -> EncodedValue {
    match data_type {
        DataType::Id => EncodedValue::Id(0),
        DataType::Integer => EncodedValue::Integer(0),
        DataType::Float => EncodedValue::Float(0.0),
        DataType::Boolean => EncodedValue::Boolean(false),
        DataType::Timestamp => EncodedValue::Timestamp(0),
        DataType::String => EncodedValue::String(String::new()),
    }
}

/// Convert encoded value to string with fast formatters
fn value_to_string(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Id(v) => itoa::Buffer::new().format(*v).to_string(),
        EncodedValue::Integer(v) => itoa::Buffer::new().format(*v).to_string(),
        EncodedValue::Float(v) => ryu::Buffer::new().format(*v).to_string(),
        EncodedValue::Boolean(v) => if *v { "true" } else { "false" }.to_string(),
        EncodedValue::Timestamp(v) => itoa::Buffer::new().format(*v).to_string(),
        EncodedValue::String(v) => v.clone(),
    }
}

pub fn compress_id_column(ids: &[u64]) -> Result<Vec<u8>> {
    if ids.is_empty() {
        return Err(PulsoraError::InvalidData("Empty ID column".to_string()));
    }
    let mut output = Vec::with_capacity(ids.len() * 2);
    output.push(0); // DataType::Id marker

    let base_id = ids[0];
    encoding::encode_varint(base_id, &mut output);
    encoding::encode_varint((ids.len() - 1) as u64, &mut output);

    for i in 1..ids.len() {
        let delta = unsafe { *ids.get_unchecked(i) as i64 - *ids.get_unchecked(i - 1) as i64 };
        encoding::encode_varint_signed(delta, &mut output);
    }
    Ok(output)
}

pub fn compress_timestamp_column(timestamps: &[i64]) -> Result<Vec<u8>> {
    use crate::storage::compression;
    let mut output = Vec::with_capacity(timestamps.len() * 2);
    output.push(4); // DataType::Timestamp marker

    let (base, compressed) = compression::compress_timestamps(timestamps)?;
    encoding::encode_varint(base as u64, &mut output);
    encoding::encode_varint(compressed.len() as u64, &mut output);
    output.extend_from_slice(&compressed);
    Ok(output)
}

pub fn compress_int_column(integers: &[i64]) -> Result<Vec<u8>> {
    use crate::storage::compression;
    let mut output = Vec::with_capacity(integers.len() * 2);
    output.push(1); // DataType::Integer marker

    let (base, compressed) = compression::compress_integers(integers)?;
    encoding::encode_varint_signed(base, &mut output);
    encoding::encode_varint(compressed.len() as u64, &mut output);
    output.extend_from_slice(&compressed);
    Ok(output)
}

pub fn compress_float_column(floats: &[f64]) -> Result<Vec<u8>> {
    use crate::storage::compression;
    let mut output = Vec::with_capacity(floats.len() * 2);
    output.push(2); // DataType::Float marker

    let (base, compressed) = compression::compress_values(floats)?;
    encoding::encode_varfloat(base, &mut output);
    encoding::encode_varint(compressed.len() as u64, &mut output);
    output.extend_from_slice(&compressed);
    Ok(output)
}

pub fn compress_bool_column(booleans: &[bool]) -> Result<Vec<u8>> {
    if booleans.is_empty() {
        return Ok(vec![3]); // DataType::Boolean marker
    }
    let mut output = Vec::with_capacity(booleans.len() / 8 + 10);
    output.push(3); // DataType::Boolean marker

    let mut runs = Vec::new();
    let mut current_value = booleans[0];
    let mut run_length = 1u32;

    for &b in booleans.iter().skip(1) {
        if b == current_value {
            run_length += 1;
        } else {
            runs.push((current_value, run_length));
            current_value = b;
            run_length = 1;
        }
    }
    runs.push((current_value, run_length));

    output.push(2); // Sub-type marker for RLE
    encoding::encode_varint(runs.len() as u64, &mut output);
    for (value, length) in runs {
        if length == 1 {
            output.push(if value { 0x80 } else { 0x00 });
        } else {
            let first_byte = if value { 0x80 } else { 0x00 } | 0x40;
            output.push(first_byte);
            encoding::encode_varint(length as u64, &mut output);
        }
    }
    Ok(output)
}

pub fn compress_string_column(strings: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(strings.len() * 10);
    output.push(5); // DataType::String marker

    let mut string_to_id = std::collections::HashMap::with_capacity(strings.len() / 4);
    let mut dictionary = Vec::new();
    let mut ids = Vec::with_capacity(strings.len());

    for s in strings {
        let id = match string_to_id.get(s.as_str()) {
            Some(&existing_id) => existing_id,
            None => {
                let new_id = dictionary.len() as u32;
                dictionary.push(s.as_str());
                string_to_id.insert(s.as_str(), new_id);
                new_id
            }
        };
        ids.push(id);
    }

    if dictionary.len() < strings.len() / 2 && strings.len() > 10 {
        output.push(4); // Dictionary encoded
        encoding::encode_varint(dictionary.len() as u64, &mut output);
        for s in dictionary {
            encoding::encode_string(s, &mut output);
        }
        for id in ids {
            encoding::encode_varint(id as u64, &mut output);
        }
    } else {
        output.push(5); // Direct encoded
        for s in strings {
            encoding::encode_string(s, &mut output);
        }
    }
    Ok(output)
}

fn compress_column(values: &[EncodedValue], data_type: &DataType) -> Result<Vec<u8>> {
    match data_type {
        DataType::Id => {
            let ids: Vec<u64> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Id(id) => *id,
                    _ => 0,
                })
                .collect();
            compress_id_column(&ids)
        }
        DataType::Integer => {
            let ints: Vec<i64> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Integer(i) => *i,
                    _ => 0,
                })
                .collect();
            compress_int_column(&ints)
        }
        DataType::Float => {
            let floats: Vec<f64> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Float(f) => *f,
                    _ => 0.0,
                })
                .collect();
            compress_float_column(&floats)
        }
        DataType::Boolean => {
            let bools: Vec<bool> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Boolean(b) => *b,
                    _ => false,
                })
                .collect();
            compress_bool_column(&bools)
        }
        DataType::Timestamp => {
            let ts: Vec<i64> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Timestamp(t) => *t,
                    _ => 0,
                })
                .collect();
            compress_timestamp_column(&ts)
        }
        DataType::String => {
            let strings: Vec<String> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::String(s) => s.clone(),
                    _ => String::new(),
                })
                .collect();
            compress_string_column(&strings)
        }
    }
}

/// Decompress a column of values with special handling for ID columns - optimized version
#[inline(never)] // Better instruction cache usage
fn decompress_column(data: &[u8], data_type: &DataType, count: usize) -> Result<Vec<EncodedValue>> {
    use crate::storage::compression;
    use std::io::Cursor;

    if data.is_empty() {
        return Err(PulsoraError::InvalidData("Empty column data".to_string()));
    }

    // Check data type marker
    let type_marker = data[0];
    let expected_marker = match data_type {
        DataType::Id => 0,
        DataType::Integer => 1,
        DataType::Float => 2,
        DataType::Boolean => 3,
        DataType::Timestamp => 4,
        DataType::String => 5,
    };

    if type_marker != expected_marker {
        return Err(PulsoraError::InvalidData("Data type mismatch".to_string()));
    }

    let mut pos = 1;

    match data_type {
        DataType::Id => {
            // Special ID column decompression - optimized
            let mut cursor = Cursor::new(&data[pos..]);
            let base_id = encoding::decode_varint(&mut cursor)?;
            pos += cursor.position() as usize;

            cursor = Cursor::new(&data[pos..]);
            let num_deltas = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            // Pre-allocate exact capacity
            let mut values = Vec::with_capacity(count);
            values.push(EncodedValue::Id(base_id));

            // Read and apply deltas efficiently
            let mut current_id = base_id;
            cursor = Cursor::new(&data[pos..]);

            for _ in 0..num_deltas {
                let delta = encoding::decode_varint_signed(&mut cursor)?;
                current_id = (current_id as i64 + delta) as u64;
                values.push(EncodedValue::Id(current_id));
            }

            if values.len() != count {
                return Err(PulsoraError::InvalidData(format!(
                    "ID count mismatch: expected {}, got {}",
                    count,
                    values.len()
                )));
            }
            Ok(values)
        }
        DataType::Timestamp => {
            // Optimized timestamp decompression
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varint(&mut cursor)? as i64;
            pos += cursor.position() as usize;

            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid timestamp data".to_string(),
                ));
            }

            // Decompress directly and convert in one pass
            let timestamps =
                compression::decompress_timestamps(base, &data[pos..pos + compressed_len], count)?;
            Ok(timestamps
                .into_iter()
                .map(EncodedValue::Timestamp)
                .collect())
        }
        DataType::Integer => {
            // Optimized integer decompression
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varint_signed(&mut cursor)?;
            pos += cursor.position() as usize;

            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid integer data".to_string(),
                ));
            }

            // Decompress directly and convert in one pass
            let integers =
                compression::decompress_integers(base, &data[pos..pos + compressed_len], count)?;
            Ok(integers.into_iter().map(EncodedValue::Integer).collect())
        }
        DataType::Float => {
            // Optimized float decompression
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varfloat(&mut cursor)?;
            pos += cursor.position() as usize;

            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData("Invalid float data".to_string()));
            }

            // Decompress directly and convert in one pass
            let floats =
                compression::decompress_values(base, &data[pos..pos + compressed_len], count)?;
            Ok(floats.into_iter().map(EncodedValue::Float).collect())
        }
        DataType::Boolean => {
            // Check sub-type marker
            if pos >= data.len() {
                return Err(PulsoraError::InvalidData(
                    "Invalid boolean data".to_string(),
                ));
            }
            let subtype = data[pos];
            pos += 1;

            let mut values = Vec::with_capacity(count);

            if subtype == 2 {
                // Run-length encoded booleans - optimized
                let mut cursor = Cursor::new(&data[pos..]);
                let num_runs = encoding::decode_varint(&mut cursor)? as usize;

                for _ in 0..num_runs {
                    let mut byte = [0u8; 1];
                    cursor.read_exact(&mut byte)?;

                    let value = (byte[0] & 0x80) != 0;
                    let has_continuation = (byte[0] & 0x40) != 0;

                    let length = if has_continuation {
                        encoding::decode_varint(&mut cursor)? as usize
                    } else {
                        1
                    };

                    // Use extend for better performance
                    values.extend(std::iter::repeat_n(
                        EncodedValue::Boolean(value),
                        length.min(count - values.len()),
                    ));

                    if values.len() >= count {
                        break;
                    }
                }
            } else {
                // Legacy bit-packed format
                let bytes = &data[pos..];
                for i in 0..count {
                    let byte_idx = i >> 3;
                    let bit_idx = i & 7;
                    let bit = if byte_idx < bytes.len() {
                        (bytes[byte_idx] >> bit_idx) & 1 != 0
                    } else {
                        false
                    };
                    values.push(EncodedValue::Boolean(bit));
                }
            }
            Ok(values)
        }
        DataType::String => {
            // Check sub-type marker
            if pos >= data.len() {
                return Err(PulsoraError::InvalidData("Invalid string data".to_string()));
            }
            let subtype = data[pos];
            pos += 1;

            let mut cursor = Cursor::new(&data[pos..]);
            let mut values = Vec::with_capacity(count);

            if subtype == 4 {
                // Dictionary-encoded strings - optimized
                let dict_size = encoding::decode_varint(&mut cursor)? as usize;

                // Read dictionary
                let mut dictionary = Vec::with_capacity(dict_size);
                for _ in 0..dict_size {
                    dictionary.push(encoding::decode_string(&mut cursor)?);
                }

                // Read string IDs and reconstruct values efficiently
                for _ in 0..count {
                    let id = encoding::decode_varint(&mut cursor)? as usize;
                    if id >= dictionary.len() {
                        return Err(PulsoraError::InvalidData("Invalid string ID".to_string()));
                    }
                    // Direct indexing - bounds already checked
                    values.push(EncodedValue::String(dictionary[id].clone()));
                }
            } else {
                // Direct-encoded strings (subtype 5 or legacy)
                for _ in 0..count {
                    values.push(EncodedValue::String(encoding::decode_string(&mut cursor)?));
                }
            }
            Ok(values)
        }
    }
}

#[cfg(test)]
#[path = "columnar_test.rs"]
mod columnar_test;
