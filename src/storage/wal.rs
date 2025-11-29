use crate::error::{PulsoraError, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct WriteAheadLog {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &str, table: &str) -> Result<Self> {
        let wal_dir = PathBuf::from(data_dir).join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let table_hash = crate::storage::calculate_table_hash(table);
        let path = wal_dir.join(format!("{}.wal", table_hash));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path,
        })
    }

    pub fn append(&self, id: u64, row: &HashMap<String, String>) -> Result<()> {
        let json = serde_json::to_vec(&(id, row)).map_err(|e| {
            PulsoraError::Ingestion(format!("Failed to serialize WAL entry: {}", e))
        })?;

        let len = json.len() as u32;
        let mut file = self.file.lock().unwrap();

        file.write_all(&len.to_le_bytes())?;
        file.write_all(&json)?;
        file.flush()?; // Ensure durability

        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<(u64, HashMap<String, String>)>> {
        let _file = self.file.lock().unwrap();
        // Re-open file for reading from start
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
        let file = self.file.lock().unwrap();
        file.set_len(0)?;
        file.sync_all()?;
        Ok(())
    }
}
