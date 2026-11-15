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

#[test]
fn test_fast_parse_f64_matches_std_parser() {
	let cases = [
		"1.761",
		"62938.01",
		"0.1",
		"0.3",
		"-0.7",
		"+2.5",
		"123",
		"123.",
		"-0",
		"-0.0",
		"0.000001",
		"999999999999999",
		"1234567.12345678",
		"9007199254740993",
		"123456789012345678901234567890",
		"0.1234567890123456789",
		"1e-7",
		"2.5E3",
		"inf",
	];
	for case in cases {
		let expected: f64 = case.parse().unwrap();
		let parsed = fast_parse_f64(case).unwrap();
		assert_eq!(parsed.to_bits(), expected.to_bits(), "{case}");
	}
}

#[test]
fn test_fast_parse_f64_matches_std_parser_on_generated_decimals() {
	// Deterministic generator: every integer/fraction split of 1..=15 digits.
	let mut state = 0x9E37_79B9_7F4A_7C15u64;
	for _ in 0..200_000 {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		let digits = 1 + (state % 15) as u32;
		let mantissa = (state >> 8) % 10u64.pow(digits);
		let frac_digits = ((state >> 40) % u64::from(digits + 1)) as usize;
		let text = format!("{mantissa:0width$}", width = digits as usize);
		let (int_part, frac_part) = text.split_at(text.len() - frac_digits);
		let case = format!("{int_part}.{frac_part}");
		let expected: f64 = case.parse().unwrap();
		assert_eq!(fast_parse_f64(&case).unwrap().to_bits(), expected.to_bits(), "{case}");
	}
}

#[test]
fn test_fast_parse_f64_rejects_malformed_text() {
	for case in ["", "-", "+", ".", "1.2.3", "12a", "--1"] {
		assert_eq!(fast_parse_f64(case), None, "{case:?}");
	}
}
