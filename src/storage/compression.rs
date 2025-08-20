//! Delta-of-delta compression with XOR for timeseries data
//!
//! Based on Facebook's Gorilla paper, optimized for market data
//! Combines varint encoding with XOR compression for maximum efficiency

use crate::error::{PulsoraError, Result};
use crate::storage::encoding;
use std::io::Cursor;

/// Bit writer for efficient bit-level operations
struct BitWriter {
    data: Vec<u8>,
    current_byte: u8,
    bits_in_current: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            current_byte: 0,
            bits_in_current: 0,
        }
    }

    fn write_bits(&mut self, value: u64, bits: u8) {
        let mut bits_to_write = bits;

        while bits_to_write > 0 {
            let available = 8 - self.bits_in_current;
            let write_now = bits_to_write.min(available);

            // Extract bits to write
            let mask = (1u64 << write_now) - 1;
            let bits_value = ((value >> (bits_to_write - write_now)) & mask) as u8;

            // Write to current byte
            self.current_byte |= bits_value << (available - write_now);
            self.bits_in_current += write_now;

            if self.bits_in_current == 8 {
                self.data.push(self.current_byte);
                self.current_byte = 0;
                self.bits_in_current = 0;
            }

            bits_to_write -= write_now;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits_in_current > 0 {
            self.data.push(self.current_byte);
        }
        self.data
    }
}

/// Bit reader for decompression
struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    fn read_bits(&mut self, bits: u8) -> Result<u64> {
        let mut result = 0u64;
        let mut bits_to_read = bits;

        while bits_to_read > 0 {
            if self.byte_pos >= self.data.len() {
                return Err(PulsoraError::InvalidData(
                    "Unexpected end of compressed data".to_string(),
                ));
            }

            let available = 8 - self.bit_pos;
            let read_now = bits_to_read.min(available);

            // Extract bits from current byte
            let mask = ((1u16 << read_now) - 1) as u8;
            let bits_value = (self.data[self.byte_pos] >> (available - read_now)) & mask;

            result = (result << read_now) | (bits_value as u64);

            self.bit_pos += read_now;
            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
            }

            bits_to_read -= read_now;
        }

        Ok(result)
    }
}

/// Compress timestamps using varint + delta-of-delta encoding
/// This combines varint encoding with delta compression for maximum efficiency
pub fn compress_timestamps(timestamps: &[i64]) -> Result<(i64, Vec<u8>)> {
    if timestamps.is_empty() {
        return Ok((0, Vec::new()));
    }

    let base = timestamps[0];
    let mut output = Vec::new();

    if timestamps.len() == 1 {
        return Ok((base, output));
    }

    // First pass: compute deltas
    let mut deltas = Vec::with_capacity(timestamps.len() - 1);
    let mut prev_timestamp = timestamps[0];

    for &timestamp in timestamps.iter().skip(1) {
        let delta = timestamp - prev_timestamp;
        deltas.push(delta);
        prev_timestamp = timestamp;
    }

    // Second pass: compute delta-of-deltas and encode with varint
    let mut prev_delta = deltas[0];
    encoding::encode_varint_signed(prev_delta, &mut output);

    for &delta in deltas.iter().skip(1) {
        let delta_of_delta = delta - prev_delta;

        // Use varint encoding for delta-of-delta
        // This is more efficient than fixed-size bit packing for most cases
        encoding::encode_varint_signed(delta_of_delta, &mut output);
        prev_delta = delta;
    }

    Ok((base, output))
}

/// Decompress timestamps using varint + delta-of-delta decoding
pub fn decompress_timestamps(base: i64, data: &[u8], count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut timestamps = vec![base];
    if count == 1 {
        return Ok(timestamps);
    }

    let mut cursor = Cursor::new(data);

    // Read first delta
    let mut prev_delta = encoding::decode_varint_signed(&mut cursor)?;
    timestamps.push(base + prev_delta);

    // Read remaining delta-of-deltas
    for _ in 2..count {
        let delta_of_delta = encoding::decode_varint_signed(&mut cursor)?;
        let delta = prev_delta + delta_of_delta;
        let last_timestamp = *timestamps.last().unwrap();
        timestamps.push(last_timestamp + delta);
        prev_delta = delta;
    }

    Ok(timestamps)
}

/// Compress float values using XOR with previous value + varint encoding
/// This provides excellent compression for slowly changing values like prices
pub fn compress_values(values: &[f64]) -> Result<(f64, Vec<u8>)> {
    if values.is_empty() {
        return Ok((0.0, Vec::new()));
    }

    let base = values[0];
    let mut writer = BitWriter::new();

    if values.len() == 1 {
        return Ok((base, writer.finish()));
    }

    let mut prev_bits = base.to_bits();
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;

    for &value in values.iter().skip(1) {
        let curr_bits = value.to_bits();
        let xor = prev_bits ^ curr_bits;

        if xor == 0 {
            // Same value - just write 1 bit (0)
            writer.write_bits(0, 1);
        } else {
            writer.write_bits(1, 1);

            let leading_zeros = xor.leading_zeros() as u8;
            let trailing_zeros = xor.trailing_zeros() as u8;

            // Check if we can use the same bit window as before (common case)
            if leading_zeros >= prev_leading && trailing_zeros >= prev_trailing {
                // Control bit 0 = reuse previous window
                writer.write_bits(0, 1);
                let significant_bits = 64 - prev_leading - prev_trailing;
                if significant_bits > 0 {
                    writer.write_bits(xor >> prev_trailing, significant_bits);
                }
            } else {
                // Control bit 1 = new window
                writer.write_bits(1, 1);

                // Store new window bounds (5 bits each for leading/trailing)
                writer.write_bits(leading_zeros as u64, 5);
                writer.write_bits(trailing_zeros as u64, 5);

                let significant_bits = 64 - leading_zeros - trailing_zeros;
                if significant_bits > 0 {
                    writer.write_bits(xor >> trailing_zeros, significant_bits);
                }

                prev_leading = leading_zeros;
                prev_trailing = trailing_zeros;
            }
        }

        prev_bits = curr_bits;
    }

    Ok((base, writer.finish()))
}

/// Decompress float values
pub fn decompress_values(base: f64, data: &[u8], count: usize) -> Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut values = vec![base];
    if count == 1 {
        return Ok(values);
    }

    let mut reader = BitReader::new(data);
    let mut prev_bits = base.to_bits();
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;

    for _ in 1..count {
        let first_bit = reader.read_bits(1)?;

        let curr_bits = if first_bit == 0 {
            // Same value
            prev_bits
        } else {
            // Different value - read control bit
            let control = reader.read_bits(1)?;

            let xor = if control == 0 {
                // Reuse previous window
                let significant_bits = 64 - prev_leading - prev_trailing;
                if significant_bits > 0 {
                    let xor_middle = reader.read_bits(significant_bits)?;
                    xor_middle << prev_trailing
                } else {
                    0
                }
            } else {
                // New window
                let leading_zeros = reader.read_bits(5)? as u8;
                let trailing_zeros = reader.read_bits(5)? as u8;

                let significant_bits = 64 - leading_zeros - trailing_zeros;
                let xor = if significant_bits > 0 {
                    let xor_middle = reader.read_bits(significant_bits)?;
                    xor_middle << trailing_zeros
                } else {
                    0
                };

                prev_leading = leading_zeros;
                prev_trailing = trailing_zeros;
                xor
            };

            prev_bits ^ xor
        };

        values.push(f64::from_bits(curr_bits));
        prev_bits = curr_bits;
    }

    Ok(values)
}

/// Compress integer values using delta + varint encoding
/// Much more efficient than treating integers as floats
pub fn compress_integers(values: &[i64]) -> Result<(i64, Vec<u8>)> {
    if values.is_empty() {
        return Ok((0, Vec::new()));
    }

    let base = values[0];
    let mut output = Vec::new();

    if values.len() == 1 {
        return Ok((base, output));
    }

    // Use delta encoding with varint
    let mut prev_value = base;
    for &value in values.iter().skip(1) {
        let delta = value - prev_value;
        encoding::encode_varint_signed(delta, &mut output);
        prev_value = value;
    }

    Ok((base, output))
}

/// Decompress integer values
pub fn decompress_integers(base: i64, data: &[u8], count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut values = vec![base];
    if count == 1 {
        return Ok(values);
    }

    let mut cursor = Cursor::new(data);
    let mut prev_value = base;

    for _ in 1..count {
        let delta = encoding::decode_varint_signed(&mut cursor)?;
        let value = prev_value + delta;
        values.push(value);
        prev_value = value;
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_compression() {
        // Typical market data timestamps (1ms intervals with some jitter)
        let timestamps = vec![
            1704067200000,
            1704067200001,
            1704067200002,
            1704067200003,
            1704067200005, // Skip one
            1704067200006,
            1704067200007,
        ];

        let (base, compressed) = compress_timestamps(&timestamps).unwrap();
        let decompressed = decompress_timestamps(base, &compressed, timestamps.len()).unwrap();

        assert_eq!(timestamps, decompressed);

        // Check compression ratio
        let original_size = timestamps.len() * 8; // 8 bytes per i64
        let compressed_size = compressed.len() + 8; // Include base timestamp
        println!(
            "Timestamp compression: {} -> {} bytes ({:.1}x)",
            original_size,
            compressed_size,
            original_size as f64 / compressed_size as f64
        );

        assert!(compressed_size < original_size / 2); // Should achieve at least 2x compression
    }

    #[test]
    fn test_value_compression() {
        // Typical market prices with small variations
        let values = vec![
            100.50, 100.51, 100.51, // Same
            100.52, 100.50, 100.49, 100.51,
        ];

        let (base, compressed) = compress_values(&values).unwrap();
        let decompressed = decompress_values(base, &compressed, values.len()).unwrap();

        assert_eq!(values, decompressed);

        // Check compression ratio
        let original_size = values.len() * 8; // 8 bytes per f64
        let compressed_size = compressed.len() + 8; // Include base value
        println!(
            "Value compression: {} -> {} bytes ({:.1}x)",
            original_size,
            compressed_size,
            original_size as f64 / compressed_size as f64
        );
    }

    #[test]
    fn test_combined_compression() {
        let timestamps: Vec<i64> = (0..1000).map(|i| 1704067200000 + i).collect();
        let values: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64 * 0.01).sin()).collect();

        // Test timestamp compression
        let (ts_base, ts_compressed) = compress_timestamps(&timestamps).unwrap();
        let dec_timestamps =
            decompress_timestamps(ts_base, &ts_compressed, timestamps.len()).unwrap();
        assert_eq!(timestamps, dec_timestamps);

        // Test value compression
        let (val_base, val_compressed) = compress_values(&values).unwrap();
        let dec_values = decompress_values(val_base, &val_compressed, values.len()).unwrap();
        assert_eq!(values, dec_values);

        let original_size = (timestamps.len() + values.len()) * 8;
        let compressed_size = ts_compressed.len() + val_compressed.len() + 16; // Include base values

        println!(
            "Combined compression: {} -> {} bytes ({:.1}x)",
            original_size,
            compressed_size,
            original_size as f64 / compressed_size as f64
        );

        // For this data pattern (sequential timestamps, smooth sine wave),
        // we expect good compression
        assert!(compressed_size < original_size); // Should achieve some compression
    }

    #[test]
    fn test_integer_compression() {
        // Test integer compression with typical patterns
        let values = vec![1000, 1001, 1001, 1002, 1000, 999, 1001, 1003, 1005, 1004];

        let (base, compressed) = compress_integers(&values).unwrap();
        let decompressed = decompress_integers(base, &compressed, values.len()).unwrap();

        assert_eq!(values, decompressed);

        let original_size = values.len() * 8;
        let compressed_size = compressed.len() + 8;

        println!(
            "Integer compression: {} -> {} bytes ({:.1}x)",
            original_size,
            compressed_size,
            original_size as f64 / compressed_size as f64
        );

        // Small deltas should compress very well
        assert!(compressed_size < original_size / 2);
    }
}
