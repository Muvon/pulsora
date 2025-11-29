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

#![allow(clippy::approx_constant)]
#![allow(clippy::bool_assert_comparison)]

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use prost::Message;
use pulsora::config::Config;
use pulsora::storage::ingestion::{ProtoBatch, ProtoRow};
use pulsora::storage::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

async fn create_test_engine() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_csv_all_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "csv_all_types";

    let csv = "id,timestamp,int_col,float_col,str_col,bool_col\n\
               1,1704067200000,42,3.14,hello,true\n\
               2,1704067201000,-100,2.71,world,false\n\
               3,1704067202000,0,0.0,test,true";

    let stats = engine.ingest_csv(table, csv.to_string()).await.unwrap();
    assert_eq!(stats.rows_inserted, 3);

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify all types preserved
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("int_col").unwrap().as_i64().unwrap(), 42);
    assert!((row1.get("float_col").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
    assert_eq!(row1.get("str_col").unwrap().as_str().unwrap(), "hello");
    assert_eq!(row1.get("bool_col").unwrap().as_bool().unwrap(), true);
}

#[tokio::test]
async fn test_protobuf_all_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "protobuf_all_types";

    // Create protobuf data with all types
    let mut values1 = HashMap::new();
    values1.insert("id".to_string(), "1".to_string());
    values1.insert("timestamp".to_string(), "1704067200000".to_string());
    values1.insert("int_col".to_string(), "42".to_string());
    values1.insert("float_col".to_string(), "3.14".to_string());
    values1.insert("str_col".to_string(), "hello".to_string());
    values1.insert("bool_col".to_string(), "true".to_string());

    let mut values2 = HashMap::new();
    values2.insert("id".to_string(), "2".to_string());
    values2.insert("timestamp".to_string(), "1704067201000".to_string());
    values2.insert("int_col".to_string(), "-100".to_string());
    values2.insert("float_col".to_string(), "2.71".to_string());
    values2.insert("str_col".to_string(), "world".to_string());
    values2.insert("bool_col".to_string(), "false".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values: values1 }, ProtoRow { values: values2 }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();

    let stats = engine.ingest_protobuf(table, buf).await.unwrap();
    assert_eq!(stats.rows_inserted, 2);

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);

    // Verify all types preserved
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("int_col").unwrap().as_i64().unwrap(), 42);
    assert!((row1.get("float_col").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
    assert_eq!(row1.get("str_col").unwrap().as_str().unwrap(), "hello");
    assert_eq!(row1.get("bool_col").unwrap().as_bool().unwrap(), true);
}

#[tokio::test]
async fn test_arrow_all_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "arrow_all_types";

    // Create Arrow data with all types
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("int_col", DataType::Int64, false),
        Field::new("float_col", DataType::Float64, false),
        Field::new("str_col", DataType::Utf8, false),
        Field::new("bool_col", DataType::Boolean, false),
    ]));

    let ids = Int64Array::from(vec![1, 2, 3]);
    let timestamps = Int64Array::from(vec![1704067200000, 1704067201000, 1704067202000]);
    let ints = Int64Array::from(vec![42, -100, 0]);
    let floats = Float64Array::from(vec![3.14, 2.71, 0.0]);
    let strings = StringArray::from(vec!["hello", "world", "test"]);
    let bools = BooleanArray::from(vec![true, false, true]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(timestamps),
            Arc::new(ints),
            Arc::new(floats),
            Arc::new(strings),
            Arc::new(bools),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let stats = engine.ingest_arrow(table, buf).await.unwrap();
    assert_eq!(stats.rows_inserted, 3);

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify all types preserved
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("int_col").unwrap().as_i64().unwrap(), 42);
    assert!((row1.get("float_col").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
    assert_eq!(row1.get("str_col").unwrap().as_str().unwrap(), "hello");
    assert_eq!(row1.get("bool_col").unwrap().as_bool().unwrap(), true);
}

#[tokio::test]
async fn test_format_interoperability() {
    let (engine, _temp) = create_test_engine().await;
    let table = "format_interop";

    // 1. Ingest via CSV
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Update via Protobuf
    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("timestamp".to_string(), "1704067201000".to_string());
    values.insert("value".to_string(), "200".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();
    engine.ingest_protobuf(table, buf).await.unwrap();

    // 3. Query and verify latest value
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 200);

    // 4. Add another row via Arrow
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ids = Int64Array::from(vec![2]);
    let timestamps = Int64Array::from(vec![1704067202000]);
    let values_arr = Int64Array::from(vec![300]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(timestamps), Arc::new(values_arr)],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    engine.ingest_arrow(table, buf).await.unwrap();

    // 5. Final query - should have 2 rows
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_csv_with_empty_strings() {
    let (engine, _temp) = create_test_engine().await;
    let table = "csv_empty_strings";

    let csv = "id,timestamp,str_col\n\
               1,1704067200000,\n\
               2,1704067201000,not_empty";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);

    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    let str_val = row1.get("str_col").unwrap().as_str().unwrap();
    assert_eq!(str_val, "");
}

#[tokio::test]
async fn test_protobuf_unicode_strings() {
    let (engine, _temp) = create_test_engine().await;
    let table = "protobuf_unicode";

    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("timestamp".to_string(), "1704067200000".to_string());
    values.insert("text".to_string(), "Hello 世界 🚀".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();

    engine.ingest_protobuf(table, buf).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("text").unwrap().as_str().unwrap(),
        "Hello 世界 🚀"
    );
}

#[tokio::test]
async fn test_arrow_negative_integers() {
    let (engine, _temp) = create_test_engine().await;
    let table = "arrow_negative";

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ids = Int64Array::from(vec![1, 2, 3]);
    let timestamps = Int64Array::from(vec![1704067200000, 1704067201000, 1704067202000]);
    let values = Int64Array::from(vec![-100, -200, -300]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(timestamps), Arc::new(values)],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    engine.ingest_arrow(table, buf).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    for (i, expected) in [(-100i64), (-200i64), (-300i64)].iter().enumerate() {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == (i + 1) as u64)
            .unwrap();
        assert_eq!(row.get("value").unwrap().as_i64().unwrap(), *expected);
    }
}

#[tokio::test]
async fn test_large_batch_all_formats() {
    let (engine, _temp) = create_test_engine().await;

    // Test CSV with 100 rows
    let table_csv = "large_csv";
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=100 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table_csv, csv).await.unwrap();
    let results = engine
        .query(table_csv, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 100);

    // Test Protobuf with 100 rows
    let table_proto = "large_proto";
    let mut rows = Vec::new();
    for i in 1..=100 {
        let mut values = HashMap::new();
        values.insert("id".to_string(), i.to_string());
        values.insert("timestamp".to_string(), (1704067200000i64 + i).to_string());
        values.insert("value".to_string(), (i * 10).to_string());
        rows.push(ProtoRow { values });
    }
    let batch = ProtoBatch { rows };
    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();
    engine.ingest_protobuf(table_proto, buf).await.unwrap();
    let results = engine
        .query(table_proto, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 100);
}
