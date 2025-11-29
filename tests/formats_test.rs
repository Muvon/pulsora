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

use arrow::array::{Int64Array, StringArray};
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
async fn test_ingest_protobuf() {
    let (engine, _temp) = create_test_engine().await;
    let table = "proto_test";

    // 1. Create Protobuf data
    let mut values1 = HashMap::new();
    values1.insert("id".to_string(), "1".to_string());
    values1.insert("timestamp".to_string(), "1704067200000".to_string());
    values1.insert("value".to_string(), "100".to_string());

    let mut values2 = HashMap::new();
    values2.insert("id".to_string(), "2".to_string());
    values2.insert("timestamp".to_string(), "1704067201000".to_string());
    values2.insert("value".to_string(), "200".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values: values1 }, ProtoRow { values: values2 }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();

    // 2. Ingest
    let stats = engine.ingest_protobuf(table, buf).await.unwrap();
    assert_eq!(stats.rows_inserted, 2);

    // 3. Query to verify
    let results = engine.query(table, None, None, None, None).await.unwrap();

    assert_eq!(results.len(), 2);

    // Verify row 1
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("value").unwrap().as_i64().unwrap(), 100);

    // Verify row 2
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(row2.get("value").unwrap().as_i64().unwrap(), 200);
}

#[tokio::test]
async fn test_ingest_arrow() {
    let (engine, _temp) = create_test_engine().await;
    let table = "arrow_test";

    // 1. Create Arrow data
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
    ]));

    let ids = Int64Array::from(vec![1, 2]);
    let timestamps = Int64Array::from(vec![1704067200000, 1704067201000]);
    let symbols = StringArray::from(vec!["AAPL", "GOOGL"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(timestamps), Arc::new(symbols)],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    // 2. Ingest
    let stats = engine.ingest_arrow(table, buf).await.unwrap();
    assert_eq!(stats.rows_inserted, 2);

    // 3. Query to verify
    let results = engine.query(table, None, None, None, None).await.unwrap();

    assert_eq!(results.len(), 2);

    // Verify row 1
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("symbol").unwrap().as_str().unwrap(), "AAPL");
    // Verify row 2
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(row2.get("symbol").unwrap().as_str().unwrap(), "GOOGL");
}

#[tokio::test]
async fn test_query_csv() {
    let (engine, _temp) = create_test_engine().await;
    let table = "csv_query_test";

    // 1. Ingest data
    let csv_data = "id,timestamp,value\n1,1704067200000,100\n2,1704067201000,200";
    engine
        .ingest_csv(table, csv_data.to_string())
        .await
        .unwrap();

    // 2. Query data
    let results = engine.query(table, None, None, None, None).await.unwrap();

    // 3. Convert to CSV (simulating server logic)
    let schema_json = engine.get_schema(table).await.unwrap();
    let schema: pulsora::storage::schema::Schema = serde_json::from_value(schema_json).unwrap();

    let mut wtr = csv::Writer::from_writer(Vec::new());

    // Write headers
    let headers: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    wtr.write_record(&headers).unwrap();

    // Write rows
    for row in results {
        let record: Vec<String> = headers
            .iter()
            .map(|header| {
                row.get(header)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => String::new(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        _ => v.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        wtr.write_record(&record).unwrap();
    }

    let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

    // 4. Verify CSV content
    assert!(
        data.contains("id,timestamp,value")
            || data.contains("timestamp,id,value")
            || data.contains("value,id,timestamp")
    ); // Order depends on schema
    assert!(data.contains("1,1704067200000,100"));
    assert!(data.contains("2,1704067201000,200"));
}
