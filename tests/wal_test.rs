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

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;

async fn create_test_engine(wal_enabled: bool) -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = wal_enabled;
    config.storage.buffer_size = 10;
    config.storage.flush_interval_ms = 0; // Batch only mode to test persistence

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_wal_durability_on_crash() {
    let (engine, temp_dir) = create_test_engine(true).await;
    let table = "wal_test_durability";

    // 1. Ingest data (should go to WAL + Buffer)
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Verify it's in memory
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    // 3. Simulate "Crash" by dropping engine and creating new one on same dir
    drop(engine);

    // 4. Restart engine
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = true;
    config.storage.buffer_size = 10;
    config.storage.flush_interval_ms = 0;

    let engine_recovered = StorageEngine::new(&config).await.unwrap();

    // 5. Verify data recovered from WAL
    let results_recovered = engine_recovered
        .query(table, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(results_recovered.len(), 1, "Should recover 1 row from WAL");
    assert_eq!(
        results_recovered[0].get("value").unwrap().as_i64().unwrap(),
        100
    );
}

#[tokio::test]
async fn test_wal_cleanup_after_flush() {
    let (engine, _temp) = create_test_engine(true).await;
    let table = "wal_test_cleanup";

    // 1. Ingest data (buffer size 10)
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=15 {
        csv.push_str(&format!(
            "{},{},{}\n",
            i,
            1704067200000i64 + (i as i64),
            i * 10
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // 2. This should have triggered a flush for first 10 rows
    // The WAL should have been truncated and now only contains 5 rows

    // We can't easily inspect file size here without knowing the hash,
    // but we can verify behavior by querying.

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 15);
}

#[tokio::test]
async fn test_wal_disabled_data_loss() {
    let (engine, temp_dir) = create_test_engine(false).await;
    let table = "wal_test_loss";

    // 1. Ingest data (WAL disabled)
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Verify in memory
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    // 3. Crash
    drop(engine);

    // 4. Restart
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = false; // Still disabled

    let engine_recovered = StorageEngine::new(&config).await.unwrap();

    // 5. Verify data LOST (because it was in RAM only and WAL was off)
    // Note: Table might not even exist if schema wasn't persisted?
    // Schema is persisted to RocksDB immediately on creation, so table exists.
    // But rows were in buffer.

    let results_recovered = engine_recovered.query(table, None, None, None, None).await;

    // If table exists (schema saved), query returns empty.
    // If schema wasn't saved (it is), it would error.
    if let Ok(rows) = results_recovered {
        assert_eq!(rows.len(), 0, "Should have lost data with WAL disabled");
    }
}

fn long_interval_config(data_dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = data_dir.to_string_lossy().to_string();
    config.storage.wal_enabled = true;
    config.storage.buffer_size = 10_000;
    config.storage.flush_interval_ms = 60_000;
    config
}

/// A long flush interval keeps acknowledged rows in the WAL-backed buffer:
/// they must be readable before any flush and survive a restart that never
/// flushed, with a re-ingested id resolving to its latest value.
#[tokio::test]
async fn test_long_flush_interval_rows_survive_restart() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::new(&long_interval_config(temp_dir.path()))
        .await
        .unwrap();

    engine
        .ingest_csv(
            "wal_long_a",
            "id,timestamp,value\n1,1704067200000,10\n2,1704067201000,20".to_string(),
        )
        .await
        .unwrap();
    engine
        .ingest_csv(
            "wal_long_a",
            "id,timestamp,value\n1,1704067200000,11".to_string(),
        )
        .await
        .unwrap();
    engine
        .ingest_csv(
            "wal_long_b",
            "id,timestamp,value\n7,1704067205000,70".to_string(),
        )
        .await
        .unwrap();

    // Readable straight from the buffer, latest copy only.
    let rows = engine
        .query("wal_long_a", None, None, None, None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let row = engine
        .get_row_by_id_json("wal_long_a", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row["value"].as_i64().unwrap(), 11);

    drop(engine);

    let engine = StorageEngine::new(&long_interval_config(temp_dir.path()))
        .await
        .unwrap();
    let a = engine
        .query("wal_long_a", None, None, None, None)
        .await
        .unwrap();
    assert_eq!(a.len(), 2, "both rows of table a recovered");
    let row = engine
        .get_row_by_id_json("wal_long_a", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row["value"].as_i64().unwrap(), 11, "latest copy recovered");
    let b = engine
        .query("wal_long_b", None, None, None, None)
        .await
        .unwrap();
    assert_eq!(b.len(), 1, "table b recovered");

    // Recovered rows flush into blocks and stay readable.
    engine.flush_table("wal_long_a").await.unwrap();
    let row = engine
        .get_row_by_id_json("wal_long_a", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row["value"].as_i64().unwrap(), 11);
    let a = engine
        .query("wal_long_a", None, None, None, None)
        .await
        .unwrap();
    assert_eq!(a.len(), 2);
}

/// A real `pulsora` process; dropping it SIGKILLs it, as a container stop does.
struct Server(Child);

impl Server {
    fn start(config_path: &Path, port: u16) -> Self {
        let server = Server(
            Command::new(env!("CARGO_BIN_EXE_pulsora"))
                .arg("-c")
                .arg(config_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        for _ in 0..200 {
            if http(port, "GET", "/health", "").is_ok() {
                return server;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("pulsora did not start listening on {}", port);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

/// One HTTP/1.1 round trip; the crate has no HTTP client and this test must
/// talk to a real server process.
fn http(
    port: u16,
    method: &str,
    path: &str,
    body: &str,
) -> std::io::Result<(u16, serde_json::Value)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Ok((status, serde_json::from_str(body).unwrap()))
}

/// Containers stop Pulsora with SIGKILL (it does not handle SIGTERM), so a
/// row is only safe if it is on disk when its ingest is acknowledged — not
/// after the next flush or the WAL writer catching up. Concurrent clients
/// outpace the per-table WAL writer, so the kill lands while it has a backlog.
#[test]
fn test_acknowledged_rows_survive_sigkill() {
    const CLIENTS: i64 = 8;
    const BATCHES: i64 = 10;
    const ROWS_PER_BATCH: i64 = 50;
    let tables = ["kill_a", "kill_b"];

    let temp_dir = TempDir::new().unwrap();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config = long_interval_config(&temp_dir.path().join("data"));
    config.server.host = "127.0.0.1".to_string();
    config.server.port = port;
    let config_path = temp_dir.path().join("pulsora.toml");
    std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();

    let server = Server::start(&config_path, port);
    std::thread::scope(|scope| {
        for client in 0..CLIENTS {
            scope.spawn(move || {
                for batch in 0..BATCHES {
                    for table in tables {
                        let first = (client * BATCHES + batch) * ROWS_PER_BATCH;
                        let mut csv = String::from("id,timestamp,value\n");
                        for id in first + 1..=first + ROWS_PER_BATCH {
                            csv.push_str(&format!("{},{},{}\n", id, 1704067200000i64 + id, id));
                        }
                        let (status, _) =
                            http(port, "POST", &format!("/tables/{}/ingest", table), &csv).unwrap();
                        assert_eq!(status, 200);
                    }
                }
            });
        }
    });
    drop(server);

    let server = Server::start(&config_path, port);
    for table in tables {
        let (status, body) = http(
            port,
            "GET",
            &format!("/tables/{}/query?limit=10000", table),
            "",
        )
        .unwrap();
        assert_eq!(status, 200);
        assert_eq!(
            body["data"].as_array().unwrap().len() as i64,
            CLIENTS * BATCHES * ROWS_PER_BATCH,
            "{}: every acknowledged row recovered",
            table
        );
    }
    drop(server);
}
