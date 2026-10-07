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

    // Create test rows with IDs
    let rows: Vec<(u64, HashMap<String, String>)> = vec![
        (
            1,
            [
                ("timestamp", "1704067200000"),
                ("price", "100.5"),
                ("volume", "1000"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ),
        (
            2,
            [
                ("timestamp", "1704067201000"),
                ("price", "100.6"),
                ("volume", "1500"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ),
        (
            3,
            [
                ("timestamp", "1704067202000"),
                ("price", "100.4"),
                ("volume", "2000"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ),
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
    for ((_, original), recovered) in rows.iter().zip(recovered_rows.iter()) {
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
    let rows: Vec<(u64, HashMap<String, String>)> = (1..=100)
        .map(|i| {
            (
                i as u64,
                [
                    ("id".to_string(), i.to_string()),
                    ("name".to_string(), format!("User{}", i)),
                ]
                .iter()
                .cloned()
                .collect(),
            )
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

    // Check that the total serialized size is reasonable
    // The block includes both ID and name columns plus metadata
    let total_uncompressed = 100 * (8 + 10); // 100 IDs (8 bytes each) + 100 names (~10 bytes each)
    println!(
        "Total compression: {} bytes estimated uncompressed -> {} bytes actual",
        total_uncompressed,
        serialized.len()
    );

    // The serialized block should be smaller than uncompressed data
    assert!(
        serialized.len() < total_uncompressed,
        "Overall compression should provide benefit: {} bytes -> {} bytes",
        total_uncompressed,
        serialized.len()
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
    let rows: Vec<(u64, HashMap<String, String>)> = ids
        .iter()
        .map(|&id| {
            (
                id,
                [
                    ("id".to_string(), id.to_string()),
                    ("value".to_string(), (id * 10).to_string()),
                ]
                .iter()
                .cloned()
                .collect(),
            )
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
    for ((_, original), recovered) in rows.iter().zip(recovered_rows.iter()) {
        assert_eq!(original["id"], recovered["id"]);
        assert_eq!(original["value"], recovered["value"]);
    }
}

#[test]
fn test_typed_compaction_preserves_nulls_and_float_bits() {
    use crate::storage::schema::Column;
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: DataType::Id,
            nullable: false,
        },
        Column {
            name: "price".into(),
            data_type: DataType::Float,
            nullable: true,
        },
        Column {
            name: "label".into(),
            data_type: DataType::String,
            nullable: true,
        },
    ];
    let schema = Schema {
        table_name: "typed".into(),
        column_order: columns.iter().map(|col| col.name.clone()).collect(),
        columns,
        timestamp_column: None,
        id_column: "id".into(),
        created_at: chrono::Utc::now(),
    };
    let prices = [
        EncodedValue::Float(-0.0),
        EncodedValue::Float(f64::from_bits(0x7ff8000000000042)),
        EncodedValue::Float(1.2345678901234567),
    ];
    let mut columns = HashMap::new();
    columns.insert(
        "id".into(),
        compress_column(
            &[
                EncodedValue::Id(1),
                EncodedValue::Id(2),
                EncodedValue::Id(3),
            ],
            &DataType::Id,
        )
        .unwrap(),
    );
    columns.insert(
        "price".into(),
        compress_column(&prices, &DataType::Float).unwrap(),
    );
    columns.insert(
        "label".into(),
        compress_column(
            &[
                EncodedValue::String("discard".into()),
                EncodedValue::String("".into()),
                EncodedValue::String("keep".into()),
            ],
            &DataType::String,
        )
        .unwrap(),
    );
    let block = ColumnBlock {
        row_count: 3,
        columns,
        null_bitmaps: HashMap::from([("label".into(), vec![2])]),
    };
    let compacted = ColumnBlock::compact(&[(block, vec![1, 2])], &schema).unwrap();
    let restored = ColumnBlock::deserialize(&compacted.serialize().unwrap()).unwrap();
    assert_eq!(restored.get_id_values(&schema).unwrap(), vec![2, 3]);
    assert_eq!(restored.null_bitmaps["label"], vec![1]);
    let values = decompress_column(&restored.columns["price"], &DataType::Float, 2).unwrap();
    match &values[0] {
        EncodedValue::Float(value) => assert_eq!(value.to_bits(), 0x7ff8000000000042),
        _ => panic!("expected float"),
    }
    match &values[1] {
        EncodedValue::Float(value) => assert_eq!(value.to_bits(), 1.2345678901234567f64.to_bits()),
        _ => panic!("expected float"),
    }
}

#[test]
fn test_compacted_timestamp_bounds_ignore_live_nulls() {
    use crate::storage::schema::Column;
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: DataType::Id,
            nullable: false,
        },
        Column {
            name: "timestamp".into(),
            data_type: DataType::Timestamp,
            nullable: true,
        },
    ];
    let schema = Schema {
        table_name: "null_ts".into(),
        column_order: columns.iter().map(|col| col.name.clone()).collect(),
        columns,
        timestamp_column: Some("timestamp".into()),
        id_column: "id".into(),
        created_at: chrono::Utc::now(),
    };
    let rows = vec![
        (1, HashMap::from([("id".into(), "1".into())])),
        (
            2,
            HashMap::from([
                ("id".into(), "2".into()),
                ("timestamp".into(), "1704067200000".into()),
            ]),
        ),
    ];
    let block = ColumnBlock::from_rows(&rows, &schema).unwrap();
    let compacted = ColumnBlock::compact(&[(block, vec![0, 1])], &schema).unwrap();
    assert_eq!(
        compacted.timestamp_bounds(&schema).unwrap(),
        Some((1704067200000, 1704067200000))
    );
    assert_eq!(compacted.null_bitmaps["timestamp"], vec![1]);
}
