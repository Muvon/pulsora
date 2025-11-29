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
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

#[derive(Debug)]
enum WalOp {
    Write(Vec<u8>),
    Truncate,
}

#[derive(Debug)]
pub struct WriteAheadLog {
    sender: mpsc::UnboundedSender<WalOp>,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &str, table: &str) -> Result<Self> {
        let wal_dir = PathBuf::from(data_dir).join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let table_hash = crate::storage::calculate_table_hash(table);
        let path = wal_dir.join(format!("{}.wal", table_hash));
        let path_clone = path.clone();

        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Spawn background writer
        tokio::spawn(async move {
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path_clone)
                .await
                .expect("Failed to open WAL file");

            while let Some(op) = receiver.recv().await {
                match op {
                    WalOp::Write(data) => {
                        if let Err(e) = file.write_all(&data).await {
                            tracing::error!("Failed to write to WAL: {}", e);
                        }
                        // We sync periodically or rely on OS?
                        // For max throughput, we rely on OS page cache + periodic sync or just write.
                        // If we want strict durability, we should sync.
                        // But "Async WAL" usually implies eventual durability or group commit.
                        // Let's sync every write for now but since it's async it won't block ingestion.
                        if let Err(e) = file.sync_data().await {
                            tracing::error!("Failed to sync WAL: {}", e);
                        }
                    }
                    WalOp::Truncate => {
                        if let Err(e) = file.set_len(0).await {
                            tracing::error!("Failed to truncate WAL: {}", e);
                        }
                        if let Err(e) = file.sync_all().await {
                            tracing::error!("Failed to sync WAL after truncate: {}", e);
                        }
                    }
                }
            }
        });

        Ok(Self { sender, path })
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

        self.sender
            .send(WalOp::Write(buffer))
            .map_err(|_| PulsoraError::Ingestion("Failed to send to WAL writer".to_string()))?;

        Ok(())
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
                    let mut data_buf = vec![0u8; len];
                    reader.read_exact(&mut data_buf)?;

                    let entry: (u64, HashMap<String, String>) = serde_json::from_slice(&data_buf)
                        .map_err(|e| {
                        PulsoraError::Ingestion(format!("Failed to deserialize WAL entry: {}", e))
                    })?;
                    rows.push(entry);
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
        self.sender
            .send(WalOp::Truncate)
            .map_err(|_| PulsoraError::Ingestion("Failed to send to WAL writer".to_string()))?;
        Ok(())
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
