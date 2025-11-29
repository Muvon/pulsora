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
use std::sync::RwLock;
use tempfile::TempDir;

fn create_test_db() -> (Arc<DB>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(DB::open_default(temp_dir.path()).unwrap());
    (db, temp_dir)
}

fn create_test_schema() -> Schema {
    use crate::storage::schema::{Column, DataType};

    Schema {
        table_name: "test_table".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Id,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
            },
            Column {
                name: "value".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
            },
        ],
        column_order: vec![
            "id".to_string(),
            "name".to_string(),
            "value".to_string(),
            "timestamp".to_string(),
        ],
        timestamp_column: Some("timestamp".to_string()),
        id_column: "id".to_string(),
        created_at: Utc::now(),
    }
}

#[test]
fn test_replace_semantics() {
    let (db, _temp) = create_test_db();
    let schema = create_test_schema();
    let mut id_managers = IdManagerRegistry::new(db.clone());

    // Insert initial row with ID 1
    let initial_rows = vec![[
        ("id".to_string(), "1".to_string()),
        ("name".to_string(), "Alice".to_string()),
        ("value".to_string(), "100".to_string()),
        ("timestamp".to_string(), "2024-01-01T00:00:00Z".to_string()),
    ]
    .iter()
    .cloned()
    .collect()];

    let result = insert_rows(
        &db,
        "test_table",
        &schema,
        &mut id_managers,
        initial_rows,
        100,
    );
    if let Err(e) = &result {
        eprintln!("Error inserting initial rows: {:?}", e);
    }
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Replace row with same ID but different data
    let replacement_rows = vec![[
        ("id".to_string(), "1".to_string()),
        ("name".to_string(), "Bob".to_string()),
        ("value".to_string(), "200".to_string()),
        ("timestamp".to_string(), "2024-01-02T00:00:00Z".to_string()),
    ]
    .iter()
    .cloned()
    .collect()];

    let result = insert_rows(
        &db,
        "test_table",
        &schema,
        &mut id_managers,
        replacement_rows,
        100,
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Query should return the latest version (Bob, not Alice)
    use crate::storage::query::get_row_by_id;
    let row = get_row_by_id(&db, "test_table", &schema, 1).unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.get("name").unwrap(), "Bob");
    assert_eq!(row.get("value").unwrap(), "200");
}

#[test]
fn test_auto_increment_with_replace() {
    let (db, _temp) = create_test_db();
    let schema = create_test_schema();
    let mut id_managers = IdManagerRegistry::new(db.clone());

    // Insert rows with auto-increment IDs
    let rows = vec![
        [
            ("name".to_string(), "User1".to_string()),
            ("value".to_string(), "10".to_string()),
            ("timestamp".to_string(), "2024-01-01T00:00:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect(),
        [
            ("name".to_string(), "User2".to_string()),
            ("value".to_string(), "20".to_string()),
            ("timestamp".to_string(), "2024-01-01T00:01:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect(),
    ];

    let result = insert_rows(&db, "test_table", &schema, &mut id_managers, rows, 100);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    // Insert a row with known ID and verify replace works
    let replacement = vec![[
        ("id".to_string(), "999".to_string()),
        ("name".to_string(), "Known".to_string()),
        ("value".to_string(), "999".to_string()),
        ("timestamp".to_string(), "2024-01-01T00:02:00Z".to_string()),
    ]
    .iter()
    .cloned()
    .collect()];

    insert_rows(
        &db,
        "test_table",
        &schema,
        &mut id_managers,
        replacement.clone(),
        100,
    )
    .unwrap();

    // Now replace it
    let replacement2 = vec![[
        ("id".to_string(), "999".to_string()),
        ("name".to_string(), "Replaced".to_string()),
        ("value".to_string(), "1000".to_string()),
        ("timestamp".to_string(), "2024-01-01T00:03:00Z".to_string()),
    ]
    .iter()
    .cloned()
    .collect()];

    insert_rows(
        &db,
        "test_table",
        &schema,
        &mut id_managers,
        replacement2,
        100,
    )
    .unwrap();

    use crate::storage::query::get_row_by_id;
    let row = get_row_by_id(&db, "test_table", &schema, 999)
        .unwrap()
        .unwrap();
    assert_eq!(row.get("name").unwrap(), "Replaced");
    assert_eq!(row.get("value").unwrap(), "1000");
}

#[test]
fn test_concurrent_replace() {
    use std::sync::Arc;
    use std::thread;

    let (db, _temp) = create_test_db();
    let schema = Arc::new(create_test_schema());
    let id_managers = Arc::new(RwLock::new(IdManagerRegistry::new(db.clone())));

    // Spawn multiple threads that try to replace the same ID
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let db = db.clone();
            let schema = schema.clone();
            let id_managers = id_managers.clone();

            thread::spawn(move || {
                let rows = vec![[
                    ("id".to_string(), "100".to_string()),
                    ("name".to_string(), format!("Thread{}", i)),
                    ("value".to_string(), i.to_string()),
                    (
                        "timestamp".to_string(),
                        format!("2024-01-01T00:00:{:02}Z", i),
                    ),
                ]
                .iter()
                .cloned()
                .collect()];

                let mut managers = id_managers.write().unwrap();
                insert_rows(&db, "test_table", &schema, &mut managers, rows, 100)
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    // The last write should win (append-only with latest version)
    use crate::storage::query::get_row_by_id;
    let row = get_row_by_id(&db, "test_table", &schema, 100).unwrap();
    assert!(row.is_some());
    // We can't predict which thread wins, but we should have exactly one result
}

#[test]
fn test_parse_csv() {
    let csv_data =
        "timestamp,price,volume\n2024-01-01 10:00:00,100.5,1000\n2024-01-01 10:01:00,101.0,1500";
    let rows = parse_csv(csv_data).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("price"), Some(&"100.5".to_string()));
    assert_eq!(rows[1].get("volume"), Some(&"1500".to_string()));
}

#[test]
fn test_parse_timestamp() {
    assert!(parse_timestamp("2024-01-01 10:00:00").is_ok());
    assert!(parse_timestamp("2024-01-01T10:00:00Z").is_ok());
    assert!(parse_timestamp("1704110400").is_ok()); // Unix timestamp
    assert!(parse_timestamp("invalid").is_err());
}
#[test]
fn test_parse_protobuf() {
    let mut values1 = HashMap::new();
    values1.insert("key1".to_string(), "value1".to_string());
    values1.insert("key2".to_string(), "value2".to_string());

    let mut values2 = HashMap::new();
    values2.insert("key3".to_string(), "value3".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values: values1 }, ProtoRow { values: values2 }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();

    let rows = parse_protobuf(&buf).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("key1"), Some(&"value1".to_string()));
    assert_eq!(rows[0].get("key2"), Some(&"value2".to_string()));
    assert_eq!(rows[1].get("key3"), Some(&"value3".to_string()));
}

#[test]
fn test_parse_arrow() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids = Int64Array::from(vec![1, 2]);
    let names = StringArray::from(vec!["Alice", "Bob"]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let rows = parse_arrow(&buf).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&"1".to_string()));
    assert_eq!(rows[0].get("name"), Some(&"Alice".to_string()));
    assert_eq!(rows[1].get("id"), Some(&"2".to_string()));
    assert_eq!(rows[1].get("name"), Some(&"Bob".to_string()));
}
