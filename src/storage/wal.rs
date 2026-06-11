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

use crate::error::{PulsoraError, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::{mpsc, Mutex};

#[derive(Debug)]
enum WalOp {
    Write(Vec<u8>),
    Truncate,
}

/// Write-Ahead Log writer.
///
/// The writer runs on a dedicated **`std::thread`** with a `std::sync::mpsc`
/// channel — deliberately *not* a `tokio::spawn`-ed task. The WAL only does
/// blocking I/O (`write_all`, `sync_data`); wrapping that in a tokio task
/// tied the WAL's lifetime to whichever runtime was current at construction
/// time, and the standard pattern of "create a Runtime, ingest, drop the
/// Runtime, keep the engine" then aborted the task via runtime shutdown.
/// That abort path races with tokio's internal JoinHandle accounting and
/// panics with "JoinHandle polled after completion" once enough cycles
/// stack up (benches, test loops, anything that creates short-lived
/// runtimes).
///
/// Running the writer as a plain OS thread sidesteps all of that. The
/// thread's only awaitable is `Receiver::recv()` (blocking on the channel)
/// — when the last `WriteAheadLog` (and therefore the last `Sender`)
/// drops, `recv()` returns `Err` and the thread exits cleanly. No runtime
/// involvement, no shutdown race.
///
/// The `Sender` lives behind a `Mutex` because `std::sync::mpsc::Sender`
/// is `Send` but `!Sync`, and we need `WriteAheadLog` itself to be `Sync`
/// so that `TableBuffer` (which contains `Option<WriteAheadLog>`) stays
/// `Sync` and the engine's `Arc<RwLock<HashMap<String, TableBuffer>>>`
/// remains shareable across tokio tasks. The mutex is only ever contended
/// trivially — callers already hold the engine's buffers write-lock, so
/// in practice only one thread sends on this channel at a time. The
/// uncontended mutex acquire is a few nanoseconds, dominated by the
/// channel send itself.
#[derive(Debug)]
pub struct WriteAheadLog {
    sender: Mutex<mpsc::Sender<WalOp>>,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &str, table: &str) -> Result<Self> {
        let wal_dir = PathBuf::from(data_dir).join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let table_hash = crate::storage::calculate_table_hash(table);
        let path = wal_dir.join(format!("{}.wal", table_hash));
        let path_clone = path.clone();

        let (sender, receiver) = mpsc::channel::<WalOp>();

        std::thread::Builder::new()
            .name(format!("wal-writer-{}", table_hash))
            .spawn(move || {
                let mut file = match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path_clone)
                {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!(
                            "WAL writer for {} failed to open file: {}",
                            path_clone.display(),
                            e
                        );
                        return;
                    }
                };

                // recv() returns Err(_) exactly when every Sender has dropped.
                // The WriteAheadLog owns the only Sender — when it goes out
                // of scope (engine teardown), this loop exits naturally.
                while let Ok(op) = receiver.recv() {
                    match op {
                        WalOp::Write(data) => {
                            if let Err(e) = file.write_all(&data) {
                                tracing::error!("Failed to write to WAL: {}", e);
                            }
                            // sync_data fsyncs file contents (not metadata)
                            // — durability guarantee for crash recovery.
                            if let Err(e) = file.sync_data() {
                                tracing::error!("Failed to sync WAL: {}", e);
                            }
                        }
                        WalOp::Truncate => {
                            if let Err(e) = file.set_len(0) {
                                tracing::error!("Failed to truncate WAL: {}", e);
                            }
                            if let Err(e) = file.sync_all() {
                                tracing::error!("Failed to sync WAL after truncate: {}", e);
                            }
                        }
                    }
                }
            })
            .map_err(|e| {
                PulsoraError::Ingestion(format!("Failed to spawn WAL writer thread: {}", e))
            })?;

        Ok(Self {
            sender: Mutex::new(sender),
            path,
        })
    }

    fn send(&self, op: WalOp) -> Result<()> {
        // Mutex acquire is uncontended in practice — the engine's buffers
        // write-lock already serializes WAL access — but exists so that the
        // Sender's !Sync bound doesn't poison TableBuffer's Sync.
        let guard = self
            .sender
            .lock()
            .map_err(|_| PulsoraError::Ingestion("WAL sender mutex poisoned".to_string()))?;
        guard
            .send(op)
            .map_err(|_| PulsoraError::Ingestion("WAL writer thread is dead".to_string()))
    }

    pub fn append_batch(&self, rows: &[(u64, HashMap<String, String>)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut buffer = Vec::new();
        for (id, row) in rows {
            let json = serde_json::to_vec(&(id, row)).map_err(|e| {
                PulsoraError::Ingestion(format!("Failed to serialize WAL entry: {}", e))
            })?;
            let len = json.len() as u32;
            buffer.extend_from_slice(&len.to_le_bytes());
            buffer.extend_from_slice(&json);
        }

        self.send(WalOp::Write(buffer))
    }

    pub fn replay(&self) -> Result<Vec<(u64, HashMap<String, String>)>> {
        // Re-open file for reading from start
        // Note: This might race if background writer is writing, but replay is usually done at startup.
        let mut reader = BufReader::new(File::open(&self.path)?);
        let mut rows = Vec::new();
        let mut len_buf = [0u8; 4];

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_buf) as usize;
                    // A torn tail record (crash mid-append) is normal WAL
                    // wear, not corruption of the recovered prefix: records
                    // are length-prefixed and appended sequentially, so every
                    // complete record before it is intact. Recover those and
                    // stop — aborting startup here would take the whole
                    // engine down over an expected crash artifact.
                    let mut data_buf = vec![0u8; len];
                    if let Err(e) = reader.read_exact(&mut data_buf) {
                        tracing::warn!(
                            "WAL {}: torn tail record ({}), recovering {} complete rows",
                            self.path.display(),
                            e,
                            rows.len()
                        );
                        break;
                    }
                    match serde_json::from_slice::<(u64, HashMap<String, String>)>(&data_buf) {
                        Ok(entry) => rows.push(entry),
                        Err(e) => {
                            tracing::warn!(
                                "WAL {}: undecodable tail record ({}), recovering {} complete rows",
                                self.path.display(),
                                e,
                                rows.len()
                            );
                            break;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(PulsoraError::Ingestion(format!(
                        "Failed to read WAL: {}",
                        e
                    )))
                }
            }
        }

        Ok(rows)
    }

    pub fn truncate(&self) -> Result<()> {
        self.send(WalOp::Truncate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_creation() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let table = "test_wal_creation";

        let wal = WriteAheadLog::new(data_dir, table);
        assert!(wal.is_ok());

        let wal_path = temp_dir.path().join("wal").join(format!(
            "{}.wal",
            crate::storage::calculate_table_hash(table)
        ));

        // Wait for async file creation
        let mut exists = false;
        for _ in 0..10 {
            if wal_path.exists() {
                exists = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(exists, "WAL file was not created");
    }

    #[tokio::test]
    async fn test_wal_append_and_replay() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let table = "test_wal_append";

        let wal = WriteAheadLog::new(data_dir, table).unwrap();

        let mut row1 = HashMap::new();
        row1.insert("col1".to_string(), "val1".to_string());

        let mut row2 = HashMap::new();
        row2.insert("col1".to_string(), "val2".to_string());

        let rows = vec![(1, row1.clone()), (2, row2.clone())];

        wal.append_batch(&rows).unwrap();

        // Wait for async write
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Replay
        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].0, 1);
        assert_eq!(replayed[0].1, row1);
        assert_eq!(replayed[1].0, 2);
        assert_eq!(replayed[1].1, row2);
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();
        let table = "test_wal_truncate";

        let wal = WriteAheadLog::new(data_dir, table).unwrap();

        let mut row = HashMap::new();
        row.insert("col".to_string(), "val".to_string());
        wal.append_batch(&[(1, row)]).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 1);

        wal.truncate().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let replayed_after = wal.replay().unwrap();
        assert_eq!(replayed_after.len(), 0);
    }
}
