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

#![cfg(unix)]

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// How long the server gets to come up and, after SIGTERM, to exit.
const DEADLINE: Duration = Duration::from_secs(10);

fn request(port: u16, head: &str, body: &str) -> std::io::Result<String> {
	let mut stream = TcpStream::connect(("127.0.0.1", port))?;
	write!(
		stream,
		"{head} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
		body.len()
	)?;
	let mut response = String::new();
	stream.read_to_string(&mut response)?;
	Ok(response)
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
	let started = Instant::now();
	loop {
		if let Some(status) = child.try_wait().unwrap() {
			return status;
		}
		if started.elapsed() > DEADLINE {
			child.kill().unwrap();
			panic!("server still running {DEADLINE:?} after SIGTERM");
		}
		std::thread::sleep(Duration::from_millis(50));
	}
}

/// SIGTERM stops the server with exit code 0, and rows that were only in the
/// buffer (no write-ahead log, no flush timer) are on disk afterwards.
#[tokio::test]
async fn test_sigterm_stops_server_and_flushes_buffered_rows() {
	let temp_dir = TempDir::new().unwrap();
	let data_dir = temp_dir.path().join("data");
	let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
	let mut config = Config::default();
	config.server.host = "127.0.0.1".to_string();
	config.server.port = port;
	config.storage.data_dir = data_dir.to_string_lossy().to_string();
	config.storage.flush_interval_ms = 0;
	config.storage.wal_enabled = false;
	let config_path = temp_dir.path().join("pulsora.toml");
	std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();

	let mut server = Command::new(env!("CARGO_BIN_EXE_pulsora"))
		.arg("--config")
		.arg(&config_path)
		.spawn()
		.unwrap();

	let started = Instant::now();
	while request(port, "GET /health", "").is_err() {
		assert!(started.elapsed() < DEADLINE, "server did not come up");
		std::thread::sleep(Duration::from_millis(50));
	}
	let csv = "id,timestamp,value\n1,1704067200000,100\n2,1704067201000,200\n";
	let response = request(port, "POST /tables/shutdown_rows/ingest", csv).unwrap();
	assert!(response.starts_with("HTTP/1.1 200"), "{response}");

	let killed = Command::new("kill")
		.arg("-TERM")
		.arg(server.id().to_string())
		.status()
		.unwrap();
	assert!(killed.success());
	let status = wait_for_exit(&mut server);
	assert!(status.success(), "server exited with {status}");

	let engine = StorageEngine::new(&config).await.unwrap();
	assert_eq!(engine.get_table_count("shutdown_rows").await.unwrap(), 2);
}
