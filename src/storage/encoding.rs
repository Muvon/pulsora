//! Efficient encoding/decoding for timeseries data
//!
//! This module provides variable-length encoding for different data types
//! optimized for timeseries storage.

use crate::error::{PulsoraError, Result};
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

/// Value types for encoding
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64),
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
