use crate::storage::schema::Schema;
use crate::storage::wal::WriteAheadLog;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug)]
pub struct TableBuffer {
    pub rows: HashMap<u64, HashMap<String, String>>,
    pub last_flush: Instant,
    pub schema: Schema,
    pub wal: Option<WriteAheadLog>,
}

impl TableBuffer {
    pub fn new(schema: Schema, wal: Option<WriteAheadLog>) -> Self {
        Self {
            rows: HashMap::new(),
            last_flush: Instant::now(),
            schema,
            wal,
        }
    }

    pub fn push(&mut self, id: u64, row: HashMap<String, String>) -> crate::error::Result<()> {
        if let Some(wal) = &self.wal {
            wal.append(id, &row)?;
        }
        self.rows.insert(id, row);
        Ok(())
    }
    pub fn push_batch(
        &mut self,
        rows: Vec<(u64, HashMap<String, String>)>,
    ) -> crate::error::Result<()> {
        if let Some(wal) = &self.wal {
            wal.append_batch(&rows)?;
        }
        for (id, row) in rows {
            self.rows.insert(id, row);
        }
        Ok(())
    }

    pub fn should_flush(&self, buffer_size: usize, flush_interval_ms: u64) -> bool {
        if self.rows.len() >= buffer_size {
            return true;
        }
        if flush_interval_ms > 0
            && self.last_flush.elapsed().as_millis() as u64 >= flush_interval_ms
            && !self.rows.is_empty()
        {
            return true;
        }
        false
    }

    pub fn get_rows(&self) -> Vec<(u64, HashMap<String, String>)> {
        self.rows.clone().into_iter().collect()
    }

    pub fn clear(&mut self) -> crate::error::Result<()> {
        if let Some(wal) = &self.wal {
            wal.truncate()?;
        }
        self.rows.clear();
        self.last_flush = Instant::now();
        Ok(())
    }
}
