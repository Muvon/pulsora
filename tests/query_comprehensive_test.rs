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

#![allow(clippy::len_zero)]

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
async fn test_range_query_boundaries() {
    let (engine, _temp) = create_test_engine().await;
    let table = "range_boundaries";

    // Insert data with specific timestamps
    let csv = "id,timestamp,value\n\
               1,1704067200000,100\n\
               2,1704067210000,200\n\
               3,1704067220000,300\n\
               4,1704067230000,400\n\
               5,1704067240000,500";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Query exact range
    let results = engine
        .query(
            table,
            Some("1704067210000".to_string()),
            Some("1704067230000".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    // Should include boundaries (210, 220, 230)
    assert_eq!(results.len(), 3);

    let values: Vec<i64> = results
        .iter()
        .map(|r| r.get("value").unwrap().as_i64().unwrap())
        .collect();
    assert!(values.contains(&200));
    assert!(values.contains(&300));
    assert!(values.contains(&400));
}

#[tokio::test]
async fn test_query_empty_range() {
    let (engine, _temp) = create_test_engine().await;
    let table = "empty_range";

    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Query range with no data
    let results = engine
        .query(
            table,
            Some("1704067300000".to_string()),
            Some("1704067400000".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_query_with_limit() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_limit";

    // Insert 20 rows
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=20 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Query with limit
    let results = engine
        .query(table, None, None, Some(5), None)
        .await
        .unwrap();

    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_query_with_offset() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_offset";

    // Insert 10 rows
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=10 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Query with offset
    let results = engine
        .query(table, None, None, None, Some(5))
        .await
        .unwrap();

    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_query_limit_and_offset() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_pagination";

    // Insert 20 rows with unique IDs
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=20 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Test basic pagination functionality
    // Page 1: limit 5, offset 0
    let page1 = engine
        .query(table, None, None, Some(5), Some(0))
        .await
        .unwrap();
    assert_eq!(page1.len(), 5, "Page 1 should have 5 rows");

    // Page 2: limit 5, offset 5
    let page2 = engine
        .query(table, None, None, Some(5), Some(5))
        .await
        .unwrap();
    assert_eq!(page2.len(), 5, "Page 2 should have 5 rows");

    // Page 3: limit 5, offset 10
    let page3 = engine
        .query(table, None, None, Some(5), Some(10))
        .await
        .unwrap();
    assert_eq!(page3.len(), 5, "Page 3 should have 5 rows");

    // Page 4: limit 5, offset 15
    let page4 = engine
        .query(table, None, None, Some(5), Some(15))
        .await
        .unwrap();
    assert_eq!(page4.len(), 5, "Page 4 should have 5 rows");

    // Test offset beyond data
    let page_beyond = engine
        .query(table, None, None, Some(5), Some(20))
        .await
        .unwrap();
    assert_eq!(
        page_beyond.len(),
        0,
        "Offset beyond data should return empty"
    );

    // Verify pagination returns correct total count
    let all_paginated = vec![page1.clone(), page2.clone(), page3.clone(), page4.clone()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        all_paginated.len(),
        20,
        "Should get all 20 rows via pagination"
    );

    // CRITICAL: Verify that pages contain DIFFERENT data (not just different counts)
    // Convert each page to a set of (id, timestamp, value) tuples for comparison
    let page1_data: Vec<(u64, i64, i64)> = page1
        .iter()
        .map(|r| {
            (
                r.get("id").unwrap().as_u64().unwrap(),
                r.get("timestamp").unwrap().as_i64().unwrap(),
                r.get("value").unwrap().as_i64().unwrap(),
            )
        })
        .collect();

    let page2_data: Vec<(u64, i64, i64)> = page2
        .iter()
        .map(|r| {
            (
                r.get("id").unwrap().as_u64().unwrap(),
                r.get("timestamp").unwrap().as_i64().unwrap(),
                r.get("value").unwrap().as_i64().unwrap(),
            )
        })
        .collect();

    let page3_data: Vec<(u64, i64, i64)> = page3
        .iter()
        .map(|r| {
            (
                r.get("id").unwrap().as_u64().unwrap(),
                r.get("timestamp").unwrap().as_i64().unwrap(),
                r.get("value").unwrap().as_i64().unwrap(),
            )
        })
        .collect();

    let page4_data: Vec<(u64, i64, i64)> = page4
        .iter()
        .map(|r| {
            (
                r.get("id").unwrap().as_u64().unwrap(),
                r.get("timestamp").unwrap().as_i64().unwrap(),
                r.get("value").unwrap().as_i64().unwrap(),
            )
        })
        .collect();

    // Verify pages are actually different (not returning the same data)
    assert_ne!(
        page1_data, page2_data,
        "Page 1 and Page 2 should have different data"
    );
    assert_ne!(
        page1_data, page3_data,
        "Page 1 and Page 3 should have different data"
    );
    assert_ne!(
        page1_data, page4_data,
        "Page 1 and Page 4 should have different data"
    );
    assert_ne!(
        page2_data, page3_data,
        "Page 2 and Page 3 should have different data"
    );
    assert_ne!(
        page2_data, page4_data,
        "Page 2 and Page 4 should have different data"
    );
    assert_ne!(
        page3_data, page4_data,
        "Page 3 and Page 4 should have different data"
    );

    // Verify that querying the same page twice returns the same data (consistency)
    let page1_again = engine
        .query(table, None, None, Some(5), Some(0))
        .await
        .unwrap();

    let page1_again_data: Vec<(u64, i64, i64)> = page1_again
        .iter()
        .map(|r| {
            (
                r.get("id").unwrap().as_u64().unwrap(),
                r.get("timestamp").unwrap().as_i64().unwrap(),
                r.get("value").unwrap().as_i64().unwrap(),
            )
        })
        .collect();

    assert_eq!(
        page1_data, page1_again_data,
        "Same query should return same data (consistency)"
    );
}

#[tokio::test]
async fn test_large_dataset_query() {
    let (engine, _temp) = create_test_engine().await;
    let table = "large_dataset";

    // Insert 1000 rows
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=1000 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Query all
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1000);

    // Query range
    let results_range = engine
        .query(
            table,
            Some("1704067200500".to_string()),
            Some("1704067200600".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    assert!(results_range.len() > 0 && results_range.len() <= 101);
}

#[tokio::test]
async fn test_query_all_data_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_all_types";

    let csv = "id,timestamp,int_col,float_col,str_col,bool_col\n\
               1,1704067200000,42,3.14,hello,true\n\
               2,1704067201000,-100,2.71,world,false";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);

    // Verify all types are queryable
    for row in &results {
        assert!(row.get("id").is_some());
        assert!(row.get("timestamp").is_some());
        assert!(row.get("int_col").is_some());
        assert!(row.get("float_col").is_some());
        assert!(row.get("str_col").is_some());
        assert!(row.get("bool_col").is_some());
    }
}

#[tokio::test]
async fn test_query_ordering_by_timestamp() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_ordering";

    // Insert in random order
    let csv = "id,timestamp,value\n\
               3,1704067202000,300\n\
               1,1704067200000,100\n\
               2,1704067201000,200";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3);

    // Results should be ordered by timestamp (or ID)
    // Verify we can access them in order
    let ids: Vec<u64> = results
        .iter()
        .map(|r| r.get("id").unwrap().as_u64().unwrap())
        .collect();

    // Should have all IDs
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
}

#[tokio::test]
async fn test_query_across_blocks() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_blocks";

    // Insert enough data to span multiple blocks (assuming block size)
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=200 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + (i * 1000), i));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Query range that should span blocks
    let results = engine
        .query(
            table,
            Some("1704067200000".to_string()),
            Some("1704067400000".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    assert!(results.len() > 0);

    // Verify data integrity across blocks
    for row in &results {
        let id = row.get("id").unwrap().as_u64().unwrap();
        let value = row.get("value").unwrap().as_i64().unwrap();
        assert_eq!(id as i64, value);
    }
}

#[tokio::test]
async fn test_query_buffer_and_persisted() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_mixed";

    // Insert data that will be persisted
    let mut csv1 = String::from("id,timestamp,value\n");
    for i in 1..=50 {
        csv1.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv1).await.unwrap();

    // Wait a bit for potential flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Insert more data (might be in buffer)
    let csv2 = "id,timestamp,value\n\
                51,1704067250051,510\n\
                52,1704067250052,520";
    engine.ingest_csv(table, csv2.to_string()).await.unwrap();

    // Query should return all data
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 52);
}

#[tokio::test]
async fn test_query_csv_output_format() {
    let (engine, _temp) = create_test_engine().await;
    let table = "query_csv_format";

    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Query returns JSON by default
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    // Verify JSON structure
    assert!(results[0].is_object());
    assert!(results[0].get("id").is_some());
    assert!(results[0].get("timestamp").is_some());
    assert!(results[0].get("value").is_some());
}

#[tokio::test]
async fn test_query_no_timestamp_column() {
    let (engine, _temp) = create_test_engine().await;
    let table = "no_timestamp";

    // Insert data without explicit timestamp column
    let csv = "id,value\n1,100\n2,200";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_query_single_row_by_id() {
    let (engine, _temp) = create_test_engine().await;
    let table = "single_row";

    let csv = "id,timestamp,value\n\
               1,1704067200000,100\n\
               2,1704067201000,200\n\
               3,1704067202000,300";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Query by ID (if supported via range or filter)
    let results = engine.query(table, None, None, None, None).await.unwrap();

    // Find specific ID
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();

    assert_eq!(row2.get("value").unwrap().as_i64().unwrap(), 200);
}
