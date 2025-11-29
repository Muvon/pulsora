//! Delta-of-delta compression with XOR for timeseries data
//!
//! Based on Facebook's Gorilla paper, optimized for market data
//! Combines varint encoding with XOR compression for maximum efficiency

use crate::error::{PulsoraError, Result};
use crate::storage::encoding;
use std::io::Cursor;

/// Bit writer for efficient bit-level operations - optimized version
struct BitWriter {
    data: Vec<u8>,
    current_byte: u8,
    bits_in_current: u8,
}

impl BitWriter {
    #[inline(always)]
    fn new_with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            current_byte: 0,
            bits_in_current: 0,
        }
    }

    #[inline(always)]
    fn write_bits(&mut self, value: u64, bits: u8) {
        // Fast path for single bit writes
        if bits == 1 {
            self.current_byte |= ((value & 1) as u8) << (7 - self.bits_in_current);
            self.bits_in_current += 1;
            if self.bits_in_current == 8 {
                self.data.push(self.current_byte);
                self.current_byte = 0;
                self.bits_in_current = 0;
            }
            return;
        }

        let mut bits_to_write = bits;
        let mut value_to_write = value;

        while bits_to_write > 0 {
            let available = 8 - self.bits_in_current;
            let write_now = bits_to_write.min(available);

            // Extract bits to write using optimized bit operations
            let shift = bits_to_write.saturating_sub(write_now);
            let mask = ((1u64 << write_now) - 1) as u8;
            let bits_value = ((value_to_write >> shift) & (mask as u64)) as u8;

            // Write to current byte
            self.current_byte |= bits_value << (available - write_now);
            self.bits_in_current += write_now;

            if self.bits_in_current == 8 {
                self.data.push(self.current_byte);
                self.current_byte = 0;
                self.bits_in_current = 0;
            }

            bits_to_write -= write_now;
            value_to_write &= (1u64 << shift) - 1; // Clear written bits
        }
    }

    #[inline(always)]
    fn finish(mut self) -> Vec<u8> {
        if self.bits_in_current > 0 {
            self.data.push(self.current_byte);
        }
        self.data
    }
}

/// Bit reader for decompression - optimized with prefetching and better cache usage
struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
    // Cache current and next bytes for better performance
    current_byte: u8,
    next_byte: u8,
    cached: bool,
}

impl<'a> BitReader<'a> {
    #[inline(always)]
    fn new(data: &'a [u8]) -> Self {
        let mut reader = Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
            current_byte: 0,
            next_byte: 0,
            cached: false,
        };
        reader.prefetch();
        reader
    }

    #[inline(always)]
    fn prefetch(&mut self) {
        if self.byte_pos < self.data.len() {
            // Use unsafe for performance - we already checked bounds
            self.current_byte = unsafe { *self.data.get_unchecked(self.byte_pos) };
            self.next_byte = if self.byte_pos + 1 < self.data.len() {
                unsafe { *self.data.get_unchecked(self.byte_pos + 1) }
            } else {
                0
            };
            self.cached = true;
        }
    }

    #[inline(always)]
    fn read_bits(&mut self, bits: u8) -> Result<u64> {
        if bits == 0 {
            return Ok(0);
        }

        // Ultra-fast path for single bit reads (most common case)
        if bits == 1 {
            if !self.cached && self.byte_pos >= self.data.len() {
                return Err(PulsoraError::InvalidData(
                    "Unexpected end of compressed data".to_string(),
                ));
            }

            // Use cached byte for faster access
            let bit = (self.current_byte >> (7 - self.bit_pos)) & 1;
            self.bit_pos += 1;
            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
                self.current_byte = self.next_byte;
                // Prefetch next byte
                if self.byte_pos + 1 < self.data.len() {
                    self.next_byte = unsafe { *self.data.get_unchecked(self.byte_pos + 1) };
                } else {
                    self.cached = self.byte_pos < self.data.len();
                }
            }
            return Ok(bit as u64);
        }

        // Fast path for common bit counts (2-8 bits within same byte)
        if bits <= 8 && self.bit_pos + bits <= 8 {
            if !self.cached && self.byte_pos >= self.data.len() {
                return Err(PulsoraError::InvalidData(
                    "Unexpected end of compressed data".to_string(),
                ));
            }

            let available = 8 - self.bit_pos;
            let mask = ((1u16 << bits) - 1) as u8;
            let result = ((self.current_byte >> (available - bits)) & mask) as u64;

            self.bit_pos += bits;
            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
                self.current_byte = self.next_byte;
                if self.byte_pos + 1 < self.data.len() {
                    self.next_byte = unsafe { *self.data.get_unchecked(self.byte_pos + 1) };
                } else {
                    self.cached = self.byte_pos < self.data.len();
                }
            }
            return Ok(result);
        }

        // Optimized multi-bit reads with better cache usage
        let mut result = 0u64;
        let mut bits_to_read = bits;

        while bits_to_read > 0 {
            if !self.cached && self.byte_pos >= self.data.len() {
                return Err(PulsoraError::InvalidData(
                    "Unexpected end of compressed data".to_string(),
                ));
            }

            let available = 8 - self.bit_pos;
            let read_now = bits_to_read.min(available);

            // Use cached byte
            let mask = ((1u16 << read_now) - 1) as u8;
            let bits_value = (self.current_byte >> (available - read_now)) & mask;

            result = (result << read_now) | (bits_value as u64);

            self.bit_pos += read_now;
            if self.bit_pos == 8 {
                self.byte_pos += 1;
                self.bit_pos = 0;
                self.current_byte = self.next_byte;
                if self.byte_pos + 1 < self.data.len() {
                    self.next_byte = unsafe { *self.data.get_unchecked(self.byte_pos + 1) };
                } else {
                    self.cached = self.byte_pos < self.data.len();
                }
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

    if timestamps.len() == 1 {
        return Ok((base, Vec::new()));
    }

    // PERFORMANCE OPTIMIZATION: Single pass, no intermediate vector
    // Pre-allocate output buffer
    let mut output = Vec::with_capacity(timestamps.len() * 2);

    // Compute and encode in a single pass
    let mut prev_timestamp = timestamps[1]; // Start from second timestamp
    let mut prev_delta = timestamps[1] - timestamps[0];

    // Write first delta
    encoding::encode_varint_signed(prev_delta, &mut output);

    // Process remaining timestamps in single pass
    for &timestamp in timestamps.iter().skip(2) {
        let delta = timestamp - prev_timestamp;
        let delta_of_delta = delta - prev_delta;

        // Encode delta-of-delta directly
        encoding::encode_varint_signed(delta_of_delta, &mut output);

        prev_timestamp = timestamp;
        prev_delta = delta;
    }

    Ok((base, output))
}

/// Decompress timestamps using varint + delta-of-delta decoding - optimized version
#[inline(never)]
pub fn decompress_timestamps(base: i64, data: &[u8], count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    // Pre-allocate exact capacity
    let mut timestamps = Vec::with_capacity(count);
    timestamps.push(base);

    if count == 1 {
        return Ok(timestamps);
    }

    let mut cursor = Cursor::new(data);

    // Read first delta
    let mut prev_delta = encoding::decode_varint_signed(&mut cursor)?;
    timestamps.push(base + prev_delta);

    // Read remaining delta-of-deltas with optimized loop
    let mut last_timestamp = timestamps[1];
    for _ in 2..count {
        let delta_of_delta = encoding::decode_varint_signed(&mut cursor)?;
        let delta = prev_delta + delta_of_delta;
        last_timestamp += delta;
        timestamps.push(last_timestamp);
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

    if values.len() == 1 {
        return Ok((base, Vec::new()));
    }

    // PERFORMANCE OPTIMIZATION: Pre-allocate with better estimate
    // XOR compression typically needs ~2-4 bytes per value
    let mut writer = BitWriter::new_with_capacity(values.len() * 4);

    let mut prev_bits = base.to_bits();
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;

    // OPTIMIZATION: Process values in chunks for better branch prediction
    // and cache locality
    const CHUNK_SIZE: usize = 1024;

    for chunk in values[1..].chunks(CHUNK_SIZE) {
        for &value in chunk {
            let curr_bits = value.to_bits();

            // Use XOR for delta compression
            let xor = prev_bits ^ curr_bits;

            if xor == 0 {
                // Same value - just write 1 bit (0)
                writer.write_bits(0, 1);
            } else {
                writer.write_bits(1, 1);

                // Use intrinsics for counting leading/trailing zeros when available
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
    }

    Ok((base, writer.finish()))
}

/// Decompress float values - optimized with pre-allocation and better cache usage
#[inline(never)] // Prevent inlining for better instruction cache usage
pub fn decompress_values(base: f64, data: &[u8], count: usize) -> Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    // Pre-allocate exact capacity
    let mut values = Vec::with_capacity(count);
    values.push(base);

    if count == 1 {
        return Ok(values);
    }

    let mut reader = BitReader::new(data);
    let mut prev_bits = base.to_bits();
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;

    // Process remaining values
    for _ in 1..count {
        let first_bit = reader.read_bits(1)?;

        // Optimize the most common case (same value) first
        if first_bit == 0 {
            values.push(f64::from_bits(prev_bits));
            continue;
        }

        // Different value - read control bit
        let control = reader.read_bits(1)?;

        let xor = if control == 0 {
            // Reuse previous window - optimized path
            let significant_bits = 64 - prev_leading - prev_trailing;
            if significant_bits > 0 {
                reader.read_bits(significant_bits)? << prev_trailing
            } else {
                0
            }
        } else {
            // New window - read leading and trailing zeros
            let leading_zeros = reader.read_bits(5)? as u8;
            let trailing_zeros = reader.read_bits(5)? as u8;

            let significant_bits = 64 - leading_zeros - trailing_zeros;
            let xor_middle = if significant_bits > 0 {
                reader.read_bits(significant_bits)?
            } else {
                0
            };

            // Update window for next iteration
            prev_leading = leading_zeros;
            prev_trailing = trailing_zeros;

            xor_middle << trailing_zeros
        };

        prev_bits ^= xor;
        values.push(f64::from_bits(prev_bits));
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

    if values.len() == 1 {
        return Ok((base, Vec::new()));
    }

    // PERFORMANCE OPTIMIZATION: Pre-calculate exact size needed
    // This avoids reallocation during encoding
    let mut output = Vec::with_capacity(values.len() * 3); // Generous pre-allocation

    // OPTIMIZATION: Process in chunks for better CPU cache utilization
    // This improves performance for large arrays
    const CHUNK_SIZE: usize = 4096;
    let mut prev_value = base;

    for chunk in values[1..].chunks(CHUNK_SIZE) {
        for &value in chunk {
            let delta = value - prev_value;
            encoding::encode_varint_signed(delta, &mut output);
            prev_value = value;
        }
    }

    // Shrink to fit to release excess memory
    output.shrink_to_fit();
    Ok((base, output))
}

/// Decompress integer values - optimized version
#[inline(never)]
pub fn decompress_integers(base: i64, data: &[u8], count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    // Pre-allocate exact capacity
    let mut values = Vec::with_capacity(count);
    values.push(base);

    if count == 1 {
        return Ok(values);
    }

    let mut cursor = Cursor::new(data);
    let mut prev_value = base;

    // Unroll loop for better performance
    let mut i = 1;
    while i < count {
        // Process 4 values at a time when possible
        if i + 3 < count {
            let delta1 = encoding::decode_varint_signed(&mut cursor)?;
            let value1 = prev_value + delta1;
            values.push(value1);

            let delta2 = encoding::decode_varint_signed(&mut cursor)?;
            let value2 = value1 + delta2;
            values.push(value2);

            let delta3 = encoding::decode_varint_signed(&mut cursor)?;
            let value3 = value2 + delta3;
            values.push(value3);

            let delta4 = encoding::decode_varint_signed(&mut cursor)?;
            let value4 = value3 + delta4;
            values.push(value4);

            prev_value = value4;
            i += 4;
        } else {
            // Handle remaining values
            let delta = encoding::decode_varint_signed(&mut cursor)?;
            let value = prev_value + delta;
            values.push(value);
            prev_value = value;
            i += 1;
        }
    }

    Ok(values)
}

#[cfg(test)]
#[path = "compression_test.rs"]
mod compression_test;
