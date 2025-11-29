use super::*;
use std::io::Cursor;

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
