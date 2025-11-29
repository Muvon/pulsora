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
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
