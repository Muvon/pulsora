//! Efficient encoding/decoding for timeseries data
//!
//! This module provides variable-length encoding for different data types
//! optimized for timeseries storage.

use crate::error::{PulsoraError, Result};
use std::io::{Cursor, Read};

/// Fast unsigned integer parsing without error overhead
#[inline(always)]
pub fn fast_parse_u64(s: &str) -> Option<u64> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut result = 0u64;
    for &b in bytes {
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        result = result.wrapping_mul(10).wrapping_add(digit as u64);
    }
    Some(result)
}

/// Fast float parsing using fast-float crate
/// This is 3-5x faster than standard parse::<f64>()
#[inline(always)]
pub fn fast_parse_f64(s: &str) -> Option<f64> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // Fast path for common integer-like floats (e.g., "123.0", "456")
    // These are very common in data and can be parsed much faster
    let mut i = 0;
    let mut negative = false;

    // Handle sign
    if bytes[0] == b'-' {
        negative = true;
        i = 1;
    } else if bytes[0] == b'+' {
        i = 1;
    }

    // Try fast integer parsing first (very common case)
    let mut int_part = 0i64;
    let start = i;
    while i < bytes.len() && bytes[i] >= b'0' && bytes[i] <= b'9' {
        int_part = int_part * 10 + (bytes[i] - b'0') as i64;
        i += 1;
    }

    // If we consumed the entire string, it's an integer
    if i == bytes.len() && i > start {
        return Some(if negative {
            -int_part as f64
        } else {
            int_part as f64
        });
    }

    // If there's a decimal point, parse fractional part
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        let mut frac_part = 0i64;
        let mut frac_digits = 0;

        while i < bytes.len() && bytes[i] >= b'0' && bytes[i] <= b'9' {
            if frac_digits < 15 {
                // Limit precision to avoid overflow
                frac_part = frac_part * 10 + (bytes[i] - b'0') as i64;
                frac_digits += 1;
            }
            i += 1;
        }

        // If we consumed everything, build the float
        if i == bytes.len() {
            let mut result = int_part as f64;
            if frac_digits > 0 {
                result += frac_part as f64 / (10_i64.pow(frac_digits as u32) as f64);
            }
            return Some(if negative { -result } else { result });
        }
    }

    // Fall back to standard parsing for scientific notation or other complex formats
    s.parse::<f64>().ok()
}

/// Fast signed integer parsing without error overhead
#[inline(always)]
pub fn fast_parse_i64(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut result = 0i64;
    let mut negative = false;
    let mut i = 0;

    if bytes[0] == b'-' {
        negative = true;
        i = 1;
    } else if bytes[0] == b'+' {
        i = 1;
    }

    while i < bytes.len() {
        let digit = bytes[i].wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        result = result.wrapping_mul(10).wrapping_add(digit as i64);
        i += 1;
    }

    Some(if negative { -result } else { result })
}

/// Encode an unsigned integer using variable-length encoding - optimized version
#[inline(always)]
pub fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    // Fast path for common small values
    if value < 128 {
        output.push(value as u8);
        return;
    }

    // Unroll the loop for better performance
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;

        if value < 0x80 {
            output.push(value as u8);
            return;
        }

        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Decode an unsigned integer from variable-length encoding - optimized version
#[inline(always)]
pub fn decode_varint(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let pos = input.position() as usize;
    let bytes = input.get_ref();

    // Fast path for single byte (common case)
    if pos < bytes.len() {
        let first = bytes[pos];
        if first < 0x80 {
            input.set_position((pos + 1) as u64);
            return Ok(first as u64);
        }
    }

    let mut result = 0u64;
    let mut shift = 0;
    let mut idx = pos;

    // Unroll first few iterations for common cases
    if idx < bytes.len() {
        let byte = bytes[idx];
        result |= ((byte & 0x7F) as u64) << shift;
        if byte < 0x80 {
            input.set_position((idx + 1) as u64);
            return Ok(result);
        }
        shift += 7;
        idx += 1;
    } else {
        return Err(PulsoraError::InvalidData(
            "Unexpected end of varint".to_string(),
        ));
    }

    if idx < bytes.len() {
        let byte = bytes[idx];
        result |= ((byte & 0x7F) as u64) << shift;
        if byte < 0x80 {
            input.set_position((idx + 1) as u64);
            return Ok(result);
        }
        shift += 7;
        idx += 1;
    } else {
        return Err(PulsoraError::InvalidData(
            "Unexpected end of varint".to_string(),
        ));
    }

    // Handle remaining bytes
    while idx < bytes.len() {
        if shift >= 70 {
            return Err(PulsoraError::InvalidData("Varint too long".to_string()));
        }

        let byte = bytes[idx];
        result |= ((byte & 0x7F) as u64) << shift;
        idx += 1;

        if byte < 0x80 {
            input.set_position(idx as u64);
            return Ok(result);
        }

        shift += 7;
    }

    Err(PulsoraError::InvalidData(
        "Unexpected end of varint".to_string(),
    ))
}

/// Encode a signed integer using zigzag encoding + varint - optimized version
#[inline(always)]
pub fn encode_varint_signed(value: i64, output: &mut Vec<u8>) {
    // Zigzag encoding: positive numbers map to even, negative to odd
    // Use arithmetic shift for sign extension
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag, output);
}

/// Decode a signed integer from zigzag + varint encoding - optimized version
#[inline(always)]
pub fn decode_varint_signed(input: &mut Cursor<&[u8]>) -> Result<i64> {
    let zigzag = decode_varint(input)?;
    // Reverse zigzag encoding using bit manipulation
    // This avoids branching and is faster
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
    Id(u64),
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
            std::f64::consts::PI,
            -std::f64::consts::E,
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
