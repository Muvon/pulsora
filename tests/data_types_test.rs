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

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use tempfile::TempDir;

async fn create_test_engine() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_integer_type_edge_cases() {
    let (engine, _temp) = create_test_engine().await;
    let table = "integer_test";

    // Test with min, max, zero, negative values
    let csv = format!(
        "id,timestamp,int_val\n\
         1,1704067200000,{}\n\
         2,1704067201000,{}\n\
         3,1704067202000,0\n\
         4,1704067203000,-12345\n\
         5,1704067204000,42",
        i64::MAX,
        i64::MIN
    );

    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 5);

    // Verify max value
    let row_max = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row_max.get("int_val").unwrap().as_i64().unwrap(), i64::MAX);

    // Verify min value
    let row_min = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(row_min.get("int_val").unwrap().as_i64().unwrap(), i64::MIN);

    // Verify zero
    let row_zero = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    assert_eq!(row_zero.get("int_val").unwrap().as_i64().unwrap(), 0);

    // Verify negative
    let row_neg = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 4)
        .unwrap();
    assert_eq!(row_neg.get("int_val").unwrap().as_i64().unwrap(), -12345);

    // Verify positive
    let row_pos = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 5)
        .unwrap();
    assert_eq!(row_pos.get("int_val").unwrap().as_i64().unwrap(), 42);
}

#[tokio::test]
async fn test_float_type_edge_cases() {
    let (engine, _temp) = create_test_engine().await;
    let table = "float_test";

    // Test with various float values
    let csv = "id,timestamp,float_val\n\
               1,1704067200000,3.14159265359\n\
               2,1704067201000,-2.71828\n\
               3,1704067202000,0.0\n\
               4,1704067203000,1.23e10\n\
               5,1704067204000,-9.87e-5\n\
               6,1704067205000,999999999.999999";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 6);

    // Verify positive float
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    let val1 = row1.get("float_val").unwrap().as_f64().unwrap();
    assert!((val1 - 3.14159265359).abs() < 1e-9);

    // Verify negative float
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    let val2 = row2.get("float_val").unwrap().as_f64().unwrap();
    assert!((val2 - (-2.71828)).abs() < 1e-5);

    // Verify zero
    let row3 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    assert_eq!(row3.get("float_val").unwrap().as_f64().unwrap(), 0.0);

    // Verify scientific notation positive
    let row4 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 4)
        .unwrap();
    let val4 = row4.get("float_val").unwrap().as_f64().unwrap();
    assert!((val4 - 1.23e10).abs() < 1e3);

    // Verify scientific notation negative exponent
    let row5 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 5)
        .unwrap();
    let val5 = row5.get("float_val").unwrap().as_f64().unwrap();
    assert!((val5 - (-9.87e-5)).abs() < 1e-10);
}

#[tokio::test]
async fn test_string_type_edge_cases() {
    let (engine, _temp) = create_test_engine().await;
    let table = "string_test";

    // Test with various string values including unicode and special chars
    let csv = "id,timestamp,str_val\n\
               1,1704067200000,simple\n\
               2,1704067201000,with spaces and punctuation!\n\
               3,1704067202000,unicode: 你好世界 🚀\n\
               4,1704067203000,\"quoted,with,commas\"\n\
               5,1704067204000,very_long_string_that_tests_compression_and_storage_efficiency_with_many_characters_repeated_multiple_times_to_ensure_proper_handling";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 5);

    // Verify simple string
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("str_val").unwrap().as_str().unwrap(), "simple");

    // Verify string with spaces
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(
        row2.get("str_val").unwrap().as_str().unwrap(),
        "with spaces and punctuation!"
    );

    // Verify unicode
    let row3 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    assert_eq!(
        row3.get("str_val").unwrap().as_str().unwrap(),
        "unicode: 你好世界 🚀"
    );

    // Verify quoted with commas
    let row4 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 4)
        .unwrap();
    assert_eq!(
        row4.get("str_val").unwrap().as_str().unwrap(),
        "quoted,with,commas"
    );

    // Verify long string
    let row5 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 5)
        .unwrap();
    assert!(row5.get("str_val").unwrap().as_str().unwrap().len() > 100);
}

#[tokio::test]
async fn test_boolean_type() {
    let (engine, _temp) = create_test_engine().await;
    let table = "boolean_test";

    let csv = "id,timestamp,bool_val\n\
               1,1704067200000,true\n\
               2,1704067201000,false\n\
               3,1704067202000,True\n\
               4,1704067203000,FALSE";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 4);

    // Helper to get bool value (handles both bool and string)
    let get_bool = |val: &serde_json::Value| -> bool {
        val.as_bool().unwrap_or_else(|| {
            val.as_str()
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(false)
        })
    };

    // Verify true
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(get_bool(row1.get("bool_val").unwrap()), true);

    // Verify false
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(get_bool(row2.get("bool_val").unwrap()), false);

    // Verify case insensitive True
    let row3 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    assert_eq!(get_bool(row3.get("bool_val").unwrap()), true);

    // Verify case insensitive FALSE
    let row4 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 4)
        .unwrap();
    assert_eq!(get_bool(row4.get("bool_val").unwrap()), false);
}

#[tokio::test]
async fn test_timestamp_formats() {
    let (engine, _temp) = create_test_engine().await;
    let table = "timestamp_test";

    // Test various timestamp formats
    let csv = "id,ts_unix,ts_rfc3339,ts_naive\n\
               1,1704067200,2024-01-01T00:00:00Z,2024-01-01 12:00:00\n\
               2,1704067201000,2024-01-01T00:00:01Z,2024-01-02 13:30:45";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);

    // Verify all timestamp columns are present and parsed
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert!(row1.get("ts_unix").is_some());
    assert!(row1.get("ts_rfc3339").is_some());
    assert!(row1.get("ts_naive").is_some());
}

#[tokio::test]
async fn test_mixed_data_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "mixed_types_test";

    // Test table with all data types
    let csv = "id,timestamp,int_col,float_col,str_col,bool_col\n\
               1,1704067200000,42,3.14,hello,true\n\
               2,1704067201000,-100,2.71,world,false\n\
               3,1704067202000,0,0.0,test,true";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify first row has all types correctly
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("int_col").unwrap().as_i64().unwrap(), 42);
    assert!((row1.get("float_col").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
    assert_eq!(row1.get("str_col").unwrap().as_str().unwrap(), "hello");
    assert_eq!(row1.get("bool_col").unwrap().as_bool().unwrap(), true);

    // Verify second row
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(row2.get("int_col").unwrap().as_i64().unwrap(), -100);
    assert_eq!(row2.get("bool_col").unwrap().as_bool().unwrap(), false);
}

#[tokio::test]
async fn test_schema_inference_accuracy() {
    let (engine, _temp) = create_test_engine().await;
    let table = "schema_inference_test";

    // Ingest data and let schema be inferred
    let csv = "id,timestamp,auto_int,auto_float,auto_str,auto_bool\n\
               1,1704067200000,123,45.67,text,true\n\
               2,1704067201000,456,89.01,more,false";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Get schema
    let schema_json = engine.get_schema(table).await.unwrap();
    let schema: pulsora::storage::schema::Schema = serde_json::from_value(schema_json).unwrap();

    // Verify inferred types
    let int_col = schema
        .columns
        .iter()
        .find(|c| c.name == "auto_int")
        .unwrap();
    assert_eq!(
        int_col.data_type,
        pulsora::storage::schema::DataType::Integer
    );

    let float_col = schema
        .columns
        .iter()
        .find(|c| c.name == "auto_float")
        .unwrap();
    assert_eq!(
        float_col.data_type,
        pulsora::storage::schema::DataType::Float
    );

    let str_col = schema
        .columns
        .iter()
        .find(|c| c.name == "auto_str")
        .unwrap();
    assert_eq!(
        str_col.data_type,
        pulsora::storage::schema::DataType::String
    );

    let bool_col = schema
        .columns
        .iter()
        .find(|c| c.name == "auto_bool")
        .unwrap();
    assert_eq!(
        bool_col.data_type,
        pulsora::storage::schema::DataType::Boolean
    );
}

#[tokio::test]
async fn test_id_column_auto_generation() {
    let (engine, _temp) = create_test_engine().await;
    let table = "id_auto_test";

    // Ingest without ID column
    let csv = "timestamp,value\n\
               1704067200000,100\n\
               1704067201000,200\n\
               1704067202000,300";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify IDs were auto-generated and are unique
    let id1 = results[0].get("id").unwrap().as_u64().unwrap();
    let id2 = results[1].get("id").unwrap().as_u64().unwrap();
    let id3 = results[2].get("id").unwrap().as_u64().unwrap();

    assert!(id1 > 0);
    assert!(id2 > 0);
    assert!(id3 > 0);
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);
}
