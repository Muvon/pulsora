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

#![allow(clippy::unnecessary_cast)]
#![allow(clippy::useless_conversion)]

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
async fn test_integer_delta_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "int_compression";

    // Sequential integers (good for delta encoding)
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=100 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, 1000 + i));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Query back and verify exact values
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        assert_eq!(row.get("value").unwrap().as_i64().unwrap(), 1000 + i as i64);
    }
}

#[tokio::test]
async fn test_integer_negative_values_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "int_negative";

    // Mix of positive and negative
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=100 {
        let value: i64 = if i % 2 == 0 { i.into() } else { -(i as i64) };

        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, value));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    // Verify negative values preserved
    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = if i % 2 == 0 { i as i64 } else { -(i as i64) };
        assert_eq!(row.get("value").unwrap().as_i64().unwrap(), expected);
    }
}

#[tokio::test]
async fn test_float_xor_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "float_compression";

    // Various float patterns
    let mut csv = String::from("id,timestamp,price\n");
    for i in 1..=100 {
        let price = 100.0 + (i as f64) * 0.123456;
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, price));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    // Verify float precision preserved
    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = 100.0 + (i as f64) * 0.123456;
        let actual = row.get("price").unwrap().as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-6,
            "Float precision lost for row {}",
            i
        );
    }
}

#[tokio::test]
async fn test_float_special_values() {
    let (engine, _temp) = create_test_engine().await;
    let table = "float_special";

    let csv = "id,timestamp,value\n\
               1,1704067200000,0.0\n\
               2,1704067201000,-0.0\n\
               3,1704067202000,1.7976931348623157e308\n\
               4,1704067203000,2.2250738585072014e-308";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 4);

    // Verify special values
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("value").unwrap().as_f64().unwrap(), 0.0);

    let row3 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    let val3 = row3.get("value").unwrap().as_f64().unwrap();
    assert!(val3 > 1e308);
}

#[tokio::test]
async fn test_string_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "string_compression";

    // Various string patterns
    let mut csv = String::from("id,timestamp,text\n");
    for i in 1..=100 {
        let text = format!("test_string_number_{}_with_some_repeated_content", i);
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, text));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    // Verify strings preserved exactly
    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = format!("test_string_number_{}_with_some_repeated_content", i);
        assert_eq!(row.get("text").unwrap().as_str().unwrap(), expected);
    }
}

#[tokio::test]
async fn test_string_unicode_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "string_unicode";

    let mut csv = String::from("id,timestamp,text\n");
    let unicode_strings = [
        "Hello 世界",
        "Привет мир",
        "مرحبا بالعالم",
        "🚀🌟💻",
        "Ñoño español",
    ];

    for (i, text) in unicode_strings.iter().enumerate() {
        csv.push_str(&format!(
            "{},{},{}\n",
            i + 1,
            1704067200000i64 + i as i64,
            text
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), unicode_strings.len());

    // Verify unicode preserved
    for (i, expected) in unicode_strings.iter().enumerate() {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == (i + 1) as u64)
            .unwrap();
        assert_eq!(row.get("text").unwrap().as_str().unwrap(), *expected);
    }
}

#[tokio::test]
async fn test_boolean_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "bool_compression";

    // Alternating boolean pattern
    let mut csv = String::from("id,timestamp,flag\n");
    for i in 1..=100 {
        let flag = i % 2 == 0;
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, flag));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    // Verify boolean values
    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = i % 2 == 0;
        assert_eq!(row.get("flag").unwrap().as_bool().unwrap(), expected);
    }
}

#[tokio::test]
async fn test_timestamp_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "timestamp_compression";

    // Sequential timestamps
    let mut csv = String::from("id,timestamp,event_time\n");
    for i in 1..=100 {
        let event_time = 1704067200000i64 + (i * 1000);
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, event_time));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 100);

    // Verify timestamps preserved
    for i in 1..=100 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = 1704067200000i64 + (i as i64 * 1000);
        let actual = row.get("event_time").unwrap().as_i64().unwrap();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn test_mixed_types_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "mixed_compression";

    // Large dataset with all types
    let mut csv = String::from("id,timestamp,int_col,float_col,str_col,bool_col\n");
    for i in 1..=200 {
        csv.push_str(&format!(
            "{},{},{},{},text_{},{}\n",
            i,
            1704067200000i64 + i,
            i * 10,
            (i as f64) * 1.5,
            i,
            i % 2 == 0
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 200);

    // Verify all types preserved correctly
    for i in 1..=200 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();

        assert_eq!(
            row.get("int_col").unwrap().as_i64().unwrap(),
            (i as i64) * 10
        );

        let float_val = row.get("float_col").unwrap().as_f64().unwrap();
        let expected_float = (i as f64) * 1.5;
        assert!((float_val - expected_float).abs() < 1e-6);

        assert_eq!(
            row.get("str_col").unwrap().as_str().unwrap(),
            format!("text_{}", i)
        );
        assert_eq!(row.get("bool_col").unwrap().as_bool().unwrap(), i % 2 == 0);
    }
}

#[tokio::test]
async fn test_compression_after_multiple_flushes() {
    let (engine, _temp) = create_test_engine().await;
    let table = "multi_flush";

    // Insert in batches to trigger multiple flushes
    for batch in 0..5 {
        let mut csv = String::from("id,timestamp,value\n");
        for i in 1..=50 {
            let id = batch * 50 + i;
            csv.push_str(&format!("{},{},{}\n", id, 1704067200000i64 + id, id * 10));
        }
        engine.ingest_csv(table, csv).await.unwrap();

        // Small delay to allow flush
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Query all data
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 250);

    // Verify data integrity across flushes
    for i in 1..=250 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        assert_eq!(row.get("value").unwrap().as_i64().unwrap(), (i as i64) * 10);
    }
}

#[tokio::test]
async fn test_id_preservation_through_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "id_preservation";

    // Insert with specific IDs
    let mut csv = String::from("id,timestamp,value\n");
    let ids = vec![1, 100, 1000, 10000, 100000, 1000000];
    for (idx, id) in ids.iter().enumerate() {
        csv.push_str(&format!(
            "{},{},{}\n",
            id,
            1704067200000i64 + idx as i64,
            idx * 10
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), ids.len());

    // Verify all IDs preserved exactly
    for id in ids {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == id)
            .unwrap();
        assert!(row.get("value").is_some());
    }
}

#[tokio::test]
async fn test_empty_string_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "empty_strings";

    let mut csv = String::from("id,timestamp,text\n");
    for i in 1..=50 {
        let text = if i % 2 == 0 { "" } else { "content" };
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, text));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 50);

    // Verify empty strings preserved
    for i in 1..=50 {
        let row = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == i)
            .unwrap();
        let expected = if i % 2 == 0 { "" } else { "content" };
        assert_eq!(row.get("text").unwrap().as_str().unwrap(), expected);
    }
}
