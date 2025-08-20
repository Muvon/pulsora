//! Efficient encoding/decoding for timeseries data
//!
//! This module provides variable-length encoding for different data types
//! optimized for timeseries storage.

use crate::error::{PulsoraError, Result};
use std::collections::HashMap;
use std::io::{Cursor, Read};

/// Encode an unsigned integer using variable-length encoding
pub fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Decode an unsigned integer from variable-length encoding
pub fn decode_varint(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        let mut byte = [0u8; 1];
        input.read_exact(&mut byte)?;

        if shift >= 70 {
            return Err(PulsoraError::InvalidData("Varint too long".to_string()));
        }

        let value = (byte[0] & 0x7F) as u64;
        result |= value << shift;

        if byte[0] & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    Ok(result)
}

/// Encode a signed integer using zigzag encoding + varint
pub fn encode_varint_signed(value: i64, output: &mut Vec<u8>) {
    // Zigzag encoding: positive numbers map to even, negative to odd
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag, output);
}

/// Decode a signed integer from zigzag + varint encoding
pub fn decode_varint_signed(input: &mut Cursor<&[u8]>) -> Result<i64> {
    let zigzag = decode_varint(input)?;
    // Reverse zigzag encoding
    Ok(((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64))
}

/// Encode a float using variable-length encoding
/// Store the raw bits as a varint for compression
pub fn encode_varfloat(value: f64, output: &mut Vec<u8>) {
    let bits = value.to_bits();
    encode_varint(bits, output);
}

/// Decode a float from variable-length encoding
pub fn decode_varfloat(input: &mut Cursor<&[u8]>) -> Result<f64> {
    let bits = decode_varint(input)?;
    Ok(f64::from_bits(bits))
}

/// Encode a string with optional compression
pub fn encode_string(value: &str, output: &mut Vec<u8>) {
    let bytes = value.as_bytes();
    encode_varint(bytes.len() as u64, output);

    // For short strings, don't compress
    if bytes.len() < 32 {
        output.extend_from_slice(bytes);
    } else {
        // For longer strings, use simple compression
        // In production, you might want to use lz4 or zstd
        output.extend_from_slice(bytes);
    }
}

/// Decode a string
pub fn decode_string(input: &mut Cursor<&[u8]>) -> Result<String> {
    let len = decode_varint(input)? as usize;
    let mut buffer = vec![0u8; len];
    input.read_exact(&mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

/// Encode a boolean as a single byte
pub fn encode_bool(value: bool, output: &mut Vec<u8>) {
    output.push(if value { 1 } else { 0 });
}

/// Decode a boolean
pub fn decode_bool(input: &mut Cursor<&[u8]>) -> Result<bool> {
    let mut byte = [0u8; 1];
    input.read_exact(&mut byte)?;
    Ok(byte[0] != 0)
}

/// Encode a timestamp (milliseconds since epoch) as varint
pub fn encode_timestamp(value: i64, output: &mut Vec<u8>) {
    // Timestamps are always positive in our use case
    encode_varint(value as u64, output);
}

/// Decode a timestamp
pub fn decode_timestamp(input: &mut Cursor<&[u8]>) -> Result<i64> {
    Ok(decode_varint(input)? as i64)
}

/// Value types for encoding
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64),
}

impl EncodedValue {
    /// Encode the value based on its type
    pub fn encode(&self, output: &mut Vec<u8>) {
        match self {
            EncodedValue::Integer(v) => {
                output.push(0); // Type marker
                encode_varint_signed(*v, output);
            }
            EncodedValue::Float(v) => {
                output.push(1); // Type marker
                encode_varfloat(*v, output);
            }
            EncodedValue::String(v) => {
                output.push(2); // Type marker
                encode_string(v, output);
            }
            EncodedValue::Boolean(v) => {
                output.push(3); // Type marker
                encode_bool(*v, output);
            }
            EncodedValue::Timestamp(v) => {
                output.push(4); // Type marker
                encode_timestamp(*v, output);
            }
        }
    }

    /// Decode a value from bytes
    pub fn decode(input: &mut Cursor<&[u8]>) -> Result<Self> {
        let mut type_marker = [0u8; 1];
        input.read_exact(&mut type_marker)?;

        match type_marker[0] {
            0 => Ok(EncodedValue::Integer(decode_varint_signed(input)?)),
            1 => Ok(EncodedValue::Float(decode_varfloat(input)?)),
            2 => Ok(EncodedValue::String(decode_string(input)?)),
            3 => Ok(EncodedValue::Boolean(decode_bool(input)?)),
            4 => Ok(EncodedValue::Timestamp(decode_timestamp(input)?)),
            _ => Err(PulsoraError::InvalidData(format!(
                "Invalid type marker: {}",
                type_marker[0]
            ))),
        }
    }
}

/// Encode a row of data based on schema types
pub fn encode_row(row: &HashMap<String, EncodedValue>, column_order: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::new();

    // Encode number of columns
    encode_varint(column_order.len() as u64, &mut output);

    // Encode each column value in order
    for column_name in column_order {
        if let Some(value) = row.get(column_name) {
            output.push(1); // Present marker
            value.encode(&mut output);
        } else {
            output.push(0); // Null marker
        }
    }

    Ok(output)
}

/// Decode a row of data
pub fn decode_row(data: &[u8], column_order: &[String]) -> Result<HashMap<String, EncodedValue>> {
    let mut cursor = Cursor::new(data);
    let mut row = HashMap::new();

    // Decode number of columns
    let num_columns = decode_varint(&mut cursor)? as usize;

    if num_columns != column_order.len() {
        return Err(PulsoraError::InvalidData(format!(
            "Column count mismatch: expected {}, got {}",
            column_order.len(),
            num_columns
        )));
    }

    // Decode each column value
    for column_name in column_order {
        let mut present_marker = [0u8; 1];
        cursor.read_exact(&mut present_marker)?;

        if present_marker[0] == 1 {
            let value = EncodedValue::decode(&mut cursor)?;
            row.insert(column_name.clone(), value);
        }
        // Skip null values
    }

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let test_cases = vec![0u64, 127, 128, 255, 256, 16383, 16384, u64::MAX];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_varint(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_varint(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for value: {}", value);
        }
    }

    #[test]
    fn test_varint_signed_encoding() {
        let test_cases = vec![0i64, 1, -1, 127, -128, 32767, -32768, i64::MAX, i64::MIN];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_varint_signed(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_varint_signed(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for value: {}", value);
        }
    }

    #[test]
    fn test_varfloat_encoding() {
        let test_cases = vec![
            0.0,
            1.0,
            -1.0,
            127.0,
            -128.0,
            3.14159,
            -2.71828,
            1e10,
            -1e-10,
            f64::MAX,
            f64::MIN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_varfloat(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_varfloat(&mut cursor).unwrap();

            if value.is_nan() {
                assert!(decoded.is_nan(), "Failed for NaN");
            } else {
                assert_eq!(value, decoded, "Failed for value: {}", value);
            }
        }
    }

    #[test]
    fn test_varfloat_nan() {
        let value = f64::NAN;
        let mut encoded = Vec::new();
        encode_varfloat(value, &mut encoded);

        let mut cursor = Cursor::new(encoded.as_slice());
        let decoded = decode_varfloat(&mut cursor).unwrap();

        assert!(decoded.is_nan(), "Failed to encode/decode NaN");
    }

    #[test]
    fn test_string_encoding() {
        let test_cases = vec![
            "",
            "a",
            "hello",
            "Hello, World!",
            "A longer string that might benefit from compression in the future",
            "Special chars: 你好 🦀 émojis",
        ];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_string(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_string(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for string: {}", value);
        }
    }

    #[test]
    fn test_bool_encoding() {
        for value in [true, false] {
            let mut encoded = Vec::new();
            encode_bool(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_bool(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for bool: {}", value);
        }
    }

    #[test]
    fn test_timestamp_encoding() {
        let test_cases = vec![
            0i64,
            1000000000000, // 2001-09-09
            1704067200000, // 2024-01-01
            9999999999999, // Far future
        ];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_timestamp(value, &mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = decode_timestamp(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for timestamp: {}", value);
        }
    }

    #[test]
    fn test_encoded_value() {
        let test_values = vec![
            EncodedValue::Integer(42),
            EncodedValue::Integer(-42),
            EncodedValue::Float(3.14159),
            EncodedValue::String("Hello, World!".to_string()),
            EncodedValue::Boolean(true),
            EncodedValue::Boolean(false),
            EncodedValue::Timestamp(1704067200000),
        ];

        for value in test_values {
            let mut encoded = Vec::new();
            value.encode(&mut encoded);

            let mut cursor = Cursor::new(encoded.as_slice());
            let decoded = EncodedValue::decode(&mut cursor).unwrap();

            assert_eq!(value, decoded, "Failed for value: {:?}", value);
        }
    }

    #[test]
    fn test_row_encoding() {
        let column_order = vec![
            "id".to_string(),
            "price".to_string(),
            "volume".to_string(),
            "active".to_string(),
            "timestamp".to_string(),
        ];

        let mut row = HashMap::new();
        row.insert("id".to_string(), EncodedValue::Integer(123));
        row.insert("price".to_string(), EncodedValue::Float(99.99));
        row.insert("volume".to_string(), EncodedValue::Integer(1000));
        row.insert("active".to_string(), EncodedValue::Boolean(true));
        row.insert(
            "timestamp".to_string(),
            EncodedValue::Timestamp(1704067200000),
        );

        let encoded = encode_row(&row, &column_order).unwrap();
        let decoded = decode_row(&encoded, &column_order).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_row_with_nulls() {
        let column_order = vec![
            "id".to_string(),
            "optional_field".to_string(),
            "value".to_string(),
        ];

        let mut row = HashMap::new();
        row.insert("id".to_string(), EncodedValue::Integer(1));
        // optional_field is missing (null)
        row.insert(
            "value".to_string(),
            EncodedValue::String("test".to_string()),
        );

        let encoded = encode_row(&row, &column_order).unwrap();
        let decoded = decode_row(&encoded, &column_order).unwrap();

        assert_eq!(row, decoded);
        assert!(!decoded.contains_key("optional_field"));
    }

    #[test]
    fn test_encoding_efficiency() {
        // Test that small integers are encoded efficiently
        let mut encoded = Vec::new();
        encode_varint(127, &mut encoded);
        assert_eq!(encoded.len(), 1, "Small integer should use 1 byte");

        encoded.clear();
        encode_varint(128, &mut encoded);
        assert_eq!(encoded.len(), 2, "128 should use 2 bytes");
    }
}
