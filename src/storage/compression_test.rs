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
    let dec_timestamps = decompress_timestamps(ts_base, &ts_compressed, timestamps.len()).unwrap();
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
