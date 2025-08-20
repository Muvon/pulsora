//! Column-oriented storage for efficient timeseries data
//!
//! Stores each column separately for better compression and cache locality

use crate::error::{PulsoraError, Result};
use crate::storage::encoding::{self, EncodedValue};
use crate::storage::schema::{DataType, Schema};
use std::collections::HashMap;
use std::io::Read;

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
    /// Create a new column block from rows
    pub fn from_rows(rows: &[HashMap<String, String>], schema: &Schema) -> Result<Self> {
        let row_count = rows.len();
        let mut columns = HashMap::new();
        let mut null_bitmaps = HashMap::new();

        // Process each column separately
        for column in &schema.columns {
            let mut values = Vec::with_capacity(row_count);
            let mut null_bitmap = vec![0u8; row_count.div_ceil(8)];

            for (idx, row) in rows.iter().enumerate() {
                if let Some(value_str) = row.get(&column.name) {
                    // Parse value according to type
                    let value = parse_typed_value(value_str, &column.data_type)?;
                    values.push(value);
                } else {
                    // Mark as null in bitmap
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;

                    // Add default value
                    values.push(default_value(&column.data_type));
                }
            }

            // Compress column data
            let compressed = compress_column(&values, &column.data_type)?;
            columns.insert(column.name.clone(), compressed);
            null_bitmaps.insert(column.name.clone(), null_bitmap);
        }

        Ok(ColumnBlock {
            row_count,
            columns,
            null_bitmaps,
        })
    }

    /// Convert column block back to rows
    pub fn to_rows(&self, schema: &Schema) -> Result<Vec<HashMap<String, String>>> {
        let mut rows = vec![HashMap::new(); self.row_count];

        for column in &schema.columns {
            let compressed = self.columns.get(&column.name).ok_or_else(|| {
                PulsoraError::InvalidData(format!("Missing column: {}", column.name))
            })?;

            let values = decompress_column(compressed, &column.data_type, self.row_count)?;
            let null_bitmap = self.null_bitmaps.get(&column.name);

            for (idx, value) in values.into_iter().enumerate() {
                // Check if value is null
                if let Some(bitmap) = null_bitmap {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
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

    /// Serialize the column block for storage
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();

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

/// Parse a string value according to its data type
fn parse_typed_value(value: &str, data_type: &DataType) -> Result<EncodedValue> {
    match data_type {
        DataType::Id => {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| PulsoraError::InvalidData(format!("Invalid ID: {}", value)))?;
            Ok(EncodedValue::Id(parsed))
        }
        DataType::Integer => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| PulsoraError::InvalidData(format!("Invalid integer: {}", value)))?;
            Ok(EncodedValue::Integer(parsed))
        }
        DataType::Float => {
            let parsed = value
                .parse::<f64>()
                .map_err(|_| PulsoraError::InvalidData(format!("Invalid float: {}", value)))?;
            Ok(EncodedValue::Float(parsed))
        }
        DataType::Boolean => {
            let parsed = match value.to_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return Err(PulsoraError::InvalidData(format!(
                        "Invalid boolean: {}",
                        value
                    )))
                }
            };
            Ok(EncodedValue::Boolean(parsed))
        }
        DataType::Timestamp => {
            // Assuming timestamp is already parsed to milliseconds
            let parsed = value
                .parse::<i64>()
                .map_err(|_| PulsoraError::InvalidData(format!("Invalid timestamp: {}", value)))?;
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

/// Convert encoded value to string
fn value_to_string(value: &EncodedValue) -> String {
    match value {
        EncodedValue::Id(v) => v.to_string(),
        EncodedValue::Integer(v) => v.to_string(),
        EncodedValue::Float(v) => v.to_string(),
        EncodedValue::Boolean(v) => v.to_string(),
        EncodedValue::Timestamp(v) => v.to_string(),
        EncodedValue::String(v) => v.clone(),
    }
}

/// Compress a column of values with optimized compression for each data type
fn compress_column(values: &[EncodedValue], data_type: &DataType) -> Result<Vec<u8>> {
    use crate::storage::compression;

    let mut output = Vec::new();

    // Write data type marker
    output.push(match data_type {
        DataType::Id => 0,
        DataType::Integer => 1,
        DataType::Float => 2,
        DataType::Boolean => 3,
        DataType::Timestamp => 4,
        DataType::String => 5,
    });

    match data_type {
        DataType::Id => {
            // ID columns: optimized delta compression for sequential IDs
            let ids: Result<Vec<u64>> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Id(id) => Ok(*id),
                    _ => Err(PulsoraError::InvalidData(
                        "Type mismatch for ID".to_string(),
                    )),
                })
                .collect();

            let ids = ids?;

            if ids.is_empty() {
                return Err(PulsoraError::InvalidData("Empty ID column".to_string()));
            }

            let base_id = ids[0];

            // Write base ID using varint encoding
            encoding::encode_varint(base_id, &mut output);

            // Write number of deltas
            encoding::encode_varint((ids.len() - 1) as u64, &mut output);

            // Write deltas using signed varint encoding
            for i in 1..ids.len() {
                let delta = ids[i] as i64 - ids[i - 1] as i64;
                encoding::encode_varint_signed(delta, &mut output);
            }
        }
        DataType::Timestamp => {
            // Extract timestamps and use Gorilla compression
            let timestamps: Result<Vec<i64>> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Timestamp(t) => Ok(*t),
                    _ => Err(PulsoraError::InvalidData("Type mismatch".to_string())),
                })
                .collect();

            let timestamps = timestamps?;
            let (base, compressed) = compression::compress_timestamps(&timestamps)?;

            // Write base timestamp using varint encoding
            encoding::encode_varint(base as u64, &mut output);
            // Write compressed data length using varint
            encoding::encode_varint(compressed.len() as u64, &mut output);
            // Write compressed data
            output.extend_from_slice(&compressed);
        }
        DataType::Integer => {
            // Regular integer compression with delta + varint
            let integers: Result<Vec<i64>> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Integer(i) => Ok(*i),
                    _ => Err(PulsoraError::InvalidData("Type mismatch".to_string())),
                })
                .collect();

            let integers = integers?;
            let (base, compressed) = compression::compress_integers(&integers)?;

            // Write base value using varint encoding
            encoding::encode_varint_signed(base, &mut output);
            // Write compressed data length using varint
            encoding::encode_varint(compressed.len() as u64, &mut output);
            // Write compressed data
            output.extend_from_slice(&compressed);
        }
        DataType::Float => {
            // Use XOR compression for floats
            let floats: Result<Vec<f64>> = values
                .iter()
                .map(|v| match v {
                    EncodedValue::Float(f) => Ok(*f),
                    _ => Err(PulsoraError::InvalidData("Type mismatch".to_string())),
                })
                .collect();

            let floats = floats?;
            let (base, compressed) = compression::compress_values(&floats)?;

            // Write base value using varfloat encoding (not raw bits!)
            encoding::encode_varfloat(base, &mut output);
            // Write compressed data length
            encoding::encode_varint(compressed.len() as u64, &mut output);
            // Write compressed data
            output.extend_from_slice(&compressed);
        }
        DataType::Boolean => {
            // Use run-length encoding for booleans (great for sparse or repetitive data)
            let mut runs = Vec::new();
            let mut current_value = if let EncodedValue::Boolean(b) = &values[0] {
                *b
            } else {
                return Err(PulsoraError::InvalidData("Type mismatch".to_string()));
            };
            let mut run_length = 1u32;

            for value in values.iter().skip(1) {
                if let EncodedValue::Boolean(b) = value {
                    if *b == current_value {
                        run_length += 1;
                    } else {
                        // Store run: value (1 bit) + length (varint)
                        runs.push((current_value, run_length));
                        current_value = *b;
                        run_length = 1;
                    }
                } else {
                    return Err(PulsoraError::InvalidData("Type mismatch".to_string()));
                }
            }
            // Don't forget the last run
            runs.push((current_value, run_length));

            // Encode runs
            output.push(2); // Sub-type marker for RLE booleans
            encoding::encode_varint(runs.len() as u64, &mut output);
            for (value, length) in runs {
                // Pack value bit with first bit of length for efficiency
                if length == 1 {
                    // Single value - use 1 byte with value in MSB
                    output.push(if value { 0x80 } else { 0x00 });
                } else {
                    // Multiple values - value in MSB, then varint length
                    let first_byte = if value { 0x80 } else { 0x00 } | 0x40; // Set continuation bit
                    output.push(first_byte);
                    encoding::encode_varint(length as u64, &mut output);
                }
            }
        }
        DataType::String => {
            // Check if dictionary encoding would help (many repeated strings)
            let mut unique_strings = std::collections::HashSet::new();
            for value in values {
                if let EncodedValue::String(s) = value {
                    unique_strings.insert(s.clone());
                }
            }

            // Use dictionary only if we have significant repetition
            // If unique strings are less than 50% of total, dictionary helps
            if unique_strings.len() < values.len() / 2 && values.len() > 10 {
                // Dictionary encoding for repeated strings
                output.push(4); // Sub-type marker for dictionary-encoded strings

                let mut dictionary = Vec::new();
                let mut string_to_id = std::collections::HashMap::new();
                let mut ids = Vec::with_capacity(values.len());

                for value in values {
                    if let EncodedValue::String(s) = value {
                        let id = if let Some(&existing_id) = string_to_id.get(s) {
                            existing_id
                        } else {
                            let new_id = dictionary.len() as u32;
                            dictionary.push(s.clone());
                            string_to_id.insert(s.clone(), new_id);
                            new_id
                        };
                        ids.push(id);
                    }
                }

                // Write dictionary size
                encoding::encode_varint(dictionary.len() as u64, &mut output);

                // Write dictionary entries using our encoder
                for s in &dictionary {
                    encoding::encode_string(s, &mut output);
                }

                // Write string IDs using varint encoding
                for id in ids {
                    encoding::encode_varint(id as u64, &mut output);
                }
            } else {
                // Direct encoding for mostly unique strings
                output.push(5); // Sub-type marker for direct strings

                // Just use our string encoder directly
                for value in values {
                    if let EncodedValue::String(v) = value {
                        encoding::encode_string(v, &mut output);
                    } else {
                        return Err(PulsoraError::InvalidData("Type mismatch".to_string()));
                    }
                }
            }
        }
    }

    Ok(output)
}

/// Decompress a column of values with special handling for ID columns
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
    let mut values = Vec::with_capacity(count);

    match data_type {
        DataType::Id => {
            // Special ID column decompression
            let mut cursor = Cursor::new(&data[pos..]);

            // Read base ID
            let base_id = encoding::decode_varint(&mut cursor)?;
            pos += cursor.position() as usize;

            // Read number of deltas
            cursor = Cursor::new(&data[pos..]);
            let num_deltas = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            // First value is the base
            values.push(EncodedValue::Id(base_id));

            // Read and apply deltas
            let mut current_id = base_id;
            cursor = Cursor::new(&data[pos..]);

            for _ in 0..num_deltas {
                let delta = encoding::decode_varint_signed(&mut cursor)?;
                current_id = (current_id as i64 + delta) as u64;
                values.push(EncodedValue::Id(current_id));
            }

            // Verify we have the expected count
            if values.len() != count {
                return Err(PulsoraError::InvalidData(format!(
                    "ID count mismatch: expected {}, got {}",
                    count,
                    values.len()
                )));
            }
        }
        DataType::Timestamp => {
            // Read base timestamp using varint decoder
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varint(&mut cursor)? as i64;
            pos += cursor.position() as usize;

            // Read compressed data length using varint
            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            // Read compressed data
            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid timestamp data".to_string(),
                ));
            }
            let compressed_data = &data[pos..pos + compressed_len];

            // Decompress timestamps
            let timestamps = compression::decompress_timestamps(base, compressed_data, count)?;
            for t in timestamps {
                values.push(EncodedValue::Timestamp(t));
            }
        }
        DataType::Integer => {
            // Regular integer decompression
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varint_signed(&mut cursor)?;
            pos += cursor.position() as usize;

            // Read compressed data length using varint
            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            // Read compressed data
            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData(
                    "Invalid integer data".to_string(),
                ));
            }
            let compressed_data = &data[pos..pos + compressed_len];

            // Decompress integers
            let integers = compression::decompress_integers(base, compressed_data, count)?;
            for i in integers {
                values.push(EncodedValue::Integer(i));
            }
        }
        DataType::Float => {
            // Read base value using varfloat decoder
            let mut cursor = Cursor::new(&data[pos..]);
            let base = encoding::decode_varfloat(&mut cursor)?;
            pos += cursor.position() as usize;

            // Read compressed data length using varint
            cursor = Cursor::new(&data[pos..]);
            let compressed_len = encoding::decode_varint(&mut cursor)? as usize;
            pos += cursor.position() as usize;

            // Read compressed data
            if data.len() < pos + compressed_len {
                return Err(PulsoraError::InvalidData("Invalid float data".to_string()));
            }
            let compressed_data = &data[pos..pos + compressed_len];

            // Decompress values
            let floats = compression::decompress_values(base, compressed_data, count)?;
            for f in floats {
                values.push(EncodedValue::Float(f));
            }
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

            if subtype == 2 {
                // Run-length encoded booleans
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

                    for _ in 0..length {
                        if values.len() >= count {
                            break;
                        }
                        values.push(EncodedValue::Boolean(value));
                    }
                }
            } else {
                // Legacy bit-packed format (shouldn't happen with new data)
                let mut byte_idx = 0;
                let mut bit_idx = 0;
                let bytes = &data[pos..];

                for _ in 0..count {
                    if byte_idx >= bytes.len() {
                        values.push(EncodedValue::Boolean(false));
                    } else {
                        let bit = (bytes[byte_idx] >> bit_idx) & 1;
                        values.push(EncodedValue::Boolean(bit != 0));

                        bit_idx += 1;
                        if bit_idx == 8 {
                            byte_idx += 1;
                            bit_idx = 0;
                        }
                    }
                }
            }
        }
        DataType::String => {
            // Check sub-type marker
            if pos >= data.len() {
                return Err(PulsoraError::InvalidData("Invalid string data".to_string()));
            }
            let subtype = data[pos];
            pos += 1;

            let mut cursor = Cursor::new(&data[pos..]);

            if subtype == 4 {
                // Dictionary-encoded strings
                let dict_size = encoding::decode_varint(&mut cursor)? as usize;

                // Read dictionary
                let mut dictionary = Vec::with_capacity(dict_size);
                for _ in 0..dict_size {
                    let s = encoding::decode_string(&mut cursor)?;
                    dictionary.push(s);
                }

                // Read string IDs and reconstruct values
                for _ in 0..count {
                    let id = encoding::decode_varint(&mut cursor)? as usize;
                    if id >= dictionary.len() {
                        return Err(PulsoraError::InvalidData("Invalid string ID".to_string()));
                    }
                    values.push(EncodedValue::String(dictionary[id].clone()));
                }
            } else if subtype == 5 {
                // Direct-encoded strings
                for _ in 0..count {
                    let v = encoding::decode_string(&mut cursor)?;
                    values.push(EncodedValue::String(v));
                }
            } else {
                // Legacy format or unknown subtype
                for _ in 0..count {
                    let v = encoding::decode_string(&mut cursor)?;
                    values.push(EncodedValue::String(v));
                }
            }
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_block_roundtrip() {
        // Create test schema
        let columns = vec![
            crate::storage::schema::Column {
                name: "timestamp".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
            },
            crate::storage::schema::Column {
                name: "price".to_string(),
                data_type: DataType::Float,
                nullable: false,
            },
            crate::storage::schema::Column {
                name: "volume".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ];

        let schema = Schema {
            table_name: "test".to_string(),
            columns: columns.clone(),
            column_order: vec![
                "timestamp".to_string(),
                "price".to_string(),
                "volume".to_string(),
            ],
            timestamp_column: Some("timestamp".to_string()),
            id_column: "id".to_string(),
            created_at: chrono::Utc::now(),
        };

        // Create test rows
        let rows = vec![
            [
                ("timestamp", "1704067200000"),
                ("price", "100.5"),
                ("volume", "1000"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            [
                ("timestamp", "1704067201000"),
                ("price", "100.6"),
                ("volume", "1500"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            [
                ("timestamp", "1704067202000"),
                ("price", "100.4"),
                ("volume", "2000"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ];

        // Create column block
        let block = ColumnBlock::from_rows(&rows, &schema).unwrap();

        // Serialize and deserialize
        let serialized = block.serialize().unwrap();
        let deserialized = ColumnBlock::deserialize(&serialized).unwrap();

        // Convert back to rows
        let recovered_rows = deserialized.to_rows(&schema).unwrap();

        // Check that we got the same data back
        assert_eq!(rows.len(), recovered_rows.len());
        for (original, recovered) in rows.iter().zip(recovered_rows.iter()) {
            assert_eq!(original, recovered);
        }

        // Check compression ratio
        let original_size = rows.len() * 3 * 8; // 3 columns, ~8 bytes each
        let compressed_size = serialized.len();
        println!(
            "Column compression: {} -> {} bytes ({:.1}x)",
            original_size,
            compressed_size,
            original_size as f64 / compressed_size as f64
        );
    }

    #[test]
    fn test_id_column_compression() {
        // Create schema with ID column
        let columns = vec![
            crate::storage::schema::Column {
                name: "id".to_string(),
                data_type: DataType::Id,
                nullable: false,
            },
            crate::storage::schema::Column {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
            },
        ];

        let schema = Schema {
            table_name: "test".to_string(),
            columns: columns.clone(),
            column_order: vec!["id".to_string(), "name".to_string()],
            timestamp_column: None,
            id_column: "id".to_string(),
            created_at: chrono::Utc::now(),
        };

        // Create test rows with sequential IDs (should compress well)
        let rows: Vec<HashMap<String, String>> = (1..=100)
            .map(|i| {
                [
                    ("id".to_string(), i.to_string()),
                    ("name".to_string(), format!("User{}", i)),
                ]
                .iter()
                .cloned()
                .collect()
            })
            .collect();

        // Create column block
        let block = ColumnBlock::from_rows(&rows, &schema).unwrap();

        // Serialize and deserialize
        let serialized = block.serialize().unwrap();
        let deserialized = ColumnBlock::deserialize(&serialized).unwrap();

        // Convert back to rows
        let recovered_rows = deserialized.to_rows(&schema).unwrap();

        // Verify all IDs are correct
        for (i, row) in recovered_rows.iter().enumerate() {
            let expected_id = (i + 1).to_string();
            assert_eq!(row["id"], expected_id);
            assert_eq!(row["name"], format!("User{}", i + 1));
        }

        // Check that ID compression is effective
        let uncompressed_id_size = 100 * 8; // 100 u64 IDs
        println!(
            "ID compression: {} bytes uncompressed -> {} bytes total block",
            uncompressed_id_size,
            serialized.len()
        );

        // Sequential IDs should compress very well
        assert!(
            serialized.len() < uncompressed_id_size / 2,
            "ID compression should be effective"
        );
    }

    #[test]
    fn test_non_sequential_id_compression() {
        // Create schema with ID column
        let columns = vec![
            crate::storage::schema::Column {
                name: "id".to_string(),
                data_type: DataType::Id,
                nullable: false,
            },
            crate::storage::schema::Column {
                name: "value".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ];

        let schema = Schema {
            table_name: "test".to_string(),
            columns: columns.clone(),
            column_order: vec!["id".to_string(), "value".to_string()],
            timestamp_column: None,
            id_column: "id".to_string(),
            created_at: chrono::Utc::now(),
        };

        // Create test rows with non-sequential IDs
        let ids = [1, 100, 1000, 10000, 100000];
        let rows: Vec<HashMap<String, String>> = ids
            .iter()
            .map(|&id| {
                [
                    ("id".to_string(), id.to_string()),
                    ("value".to_string(), (id * 10).to_string()),
                ]
                .iter()
                .cloned()
                .collect()
            })
            .collect();

        // Create column block
        let block = ColumnBlock::from_rows(&rows, &schema).unwrap();

        // Serialize and deserialize
        let serialized = block.serialize().unwrap();
        let deserialized = ColumnBlock::deserialize(&serialized).unwrap();

        // Convert back to rows
        let recovered_rows = deserialized.to_rows(&schema).unwrap();

        // Verify all data is correct
        for (original, recovered) in rows.iter().zip(recovered_rows.iter()) {
            assert_eq!(original["id"], recovered["id"]);
            assert_eq!(original["value"], recovered["value"]);
        }
    }
}
