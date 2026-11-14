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

use axum::{
	body::Body,
	http::{Request, StatusCode},
};
use pulsora::config::Config;
use pulsora::error::PulsoraError;
use pulsora::server::create_app;
use pulsora::storage::wal::WriteAheadLog;
use pulsora::storage::StorageEngine;
use tempfile::TempDir;
use tower::ServiceExt;

fn test_config(temp_dir: &TempDir) -> Config {
	let mut config = Config::default();
	config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
	config.storage.wal_enabled = true;
	config.storage.buffer_size = 1000;
	config.storage.flush_interval_ms = 0;
	config
}

/// A table with two blocks on disk (one of them replacing a row of the other)
/// and one row still in the buffer.
async fn fill_table(engine: &StorageEngine, table: &str) {
	let first = "id,timestamp,value\n1,1704067200000,100\n2,1704067201000,200";
	engine.ingest_csv(table, first.to_string()).await.unwrap();
	engine.flush_table(table).await.unwrap();
	let second = "id,timestamp,value\n2,1704067201000,250\n3,1704067202000,300";
	engine.ingest_csv(table, second.to_string()).await.unwrap();
	assert_eq!(engine.get_table_count(table).await.unwrap(), 3);
}

#[tokio::test]
async fn test_drop_removes_table() {
	let temp_dir = TempDir::new().unwrap();
	let engine = StorageEngine::new(&test_config(&temp_dir)).await.unwrap();
	let table = "drop_basic";
	fill_table(&engine, table).await;

	engine.drop_table(table).await.unwrap();

	assert!(matches!(
		engine.query(table, None, None, None, None).await,
		Err(PulsoraError::TableNotFound(_))
	));
	assert!(matches!(
		engine.get_schema(table).await,
		Err(PulsoraError::TableNotFound(_))
	));
	assert!(matches!(
		engine.get_table_count(table).await,
		Err(PulsoraError::TableNotFound(_))
	));
	assert!(!engine.schemas.read().await.list_tables().contains(&table.to_string()));
	assert!(matches!(
		engine.drop_table(table).await,
		Err(PulsoraError::TableNotFound(_))
	));
}

#[tokio::test]
async fn test_drop_leaves_other_tables() {
	let temp_dir = TempDir::new().unwrap();
	let engine = StorageEngine::new(&test_config(&temp_dir)).await.unwrap();
	fill_table(&engine, "drop_target").await;
	fill_table(&engine, "drop_neighbour").await;

	engine.drop_table("drop_target").await.unwrap();

	assert_eq!(engine.get_table_count("drop_neighbour").await.unwrap(), 3);
	let rows = engine.query("drop_neighbour", None, None, None, None).await.unwrap();
	assert_eq!(rows.len(), 3);
	let replaced = rows.iter().find(|row| row["id"].as_u64() == Some(2)).unwrap();
	assert_eq!(replaced["value"].as_i64(), Some(250));
}

#[tokio::test]
async fn test_recreate_with_another_schema() {
	let temp_dir = TempDir::new().unwrap();
	let engine = StorageEngine::new(&test_config(&temp_dir)).await.unwrap();
	let table = "drop_recreate";
	fill_table(&engine, table).await;
	engine.drop_table(table).await.unwrap();

	// Same ids, different columns: nothing of the old table may come back.
	let csv = "id,timestamp,label\n2,1704067201000,alpha\n9,1704067209000,beta";
	engine.ingest_csv(table, csv.to_string()).await.unwrap();
	engine.flush_table(table).await.unwrap();

	assert_eq!(engine.get_table_count(table).await.unwrap(), 2);
	let rows = engine.query(table, None, None, None, None).await.unwrap();
	assert_eq!(rows.len(), 2);
	for row in &rows {
		assert!(row.get("value").is_none());
		assert!(row["label"].is_string());
	}
	assert!(engine.get_row_by_id_json(table, 1).await.unwrap().is_none());
	assert!(engine.get_row_by_id_json(table, 3).await.unwrap().is_none());
}

#[tokio::test]
async fn test_drop_survives_restart() {
	let temp_dir = TempDir::new().unwrap();
	let config = test_config(&temp_dir);
	let table = "drop_restart";
	let buffered_table = "drop_restart_buffered";
	let wal_file = WriteAheadLog::file_path(&config.storage.data_dir, buffered_table);
	{
		let engine = StorageEngine::new(&config).await.unwrap();
		fill_table(&engine, table).await;
		// The first ingest of a table stays in the buffer and the
		// write-ahead log until a flush.
		let buffered = "id,timestamp,value\n4,1704067203000,400";
		engine.ingest_csv(buffered_table, buffered.to_string()).await.unwrap();
		assert!(wal_file.exists());
		engine.drop_table(table).await.unwrap();
		engine.drop_table(buffered_table).await.unwrap();
		assert!(!wal_file.exists());
	}

	let engine = StorageEngine::new(&config).await.unwrap();
	for dropped in [table, buffered_table] {
		assert!(matches!(
			engine.query(dropped, None, None, None, None).await,
			Err(PulsoraError::TableNotFound(_))
		));
	}

	// A table created under the old name starts empty.
	let csv = "id,timestamp,value\n7,1704067207000,700";
	engine.ingest_csv(table, csv.to_string()).await.unwrap();
	assert_eq!(engine.get_table_count(table).await.unwrap(), 1);
}

#[tokio::test]
async fn test_drop_table_route() {
	let temp_dir = TempDir::new().unwrap();
	let app = create_app(test_config(&temp_dir)).await.unwrap();
	let table = "drop_route";

	let ingest = Request::builder()
		.method("POST")
		.uri(format!("/tables/{table}/ingest"))
		.header("Content-Type", "text/csv")
		.body(Body::from("id,timestamp,value\n1,1704067200000,100"))
		.unwrap();
	assert_eq!(app.clone().oneshot(ingest).await.unwrap().status(), StatusCode::OK);

	let drop_request = || {
		Request::builder()
			.method("DELETE")
			.uri(format!("/tables/{table}"))
			.body(Body::empty())
			.unwrap()
	};
	assert_eq!(
		app.clone().oneshot(drop_request()).await.unwrap().status(),
		StatusCode::OK
	);
	assert_eq!(
		app.clone().oneshot(drop_request()).await.unwrap().status(),
		StatusCode::NOT_FOUND
	);

	let schema = Request::builder()
		.uri(format!("/tables/{table}/schema"))
		.body(Body::empty())
		.unwrap();
	assert_eq!(app.oneshot(schema).await.unwrap().status(), StatusCode::NOT_FOUND);
}
