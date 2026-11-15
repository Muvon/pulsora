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
use tempfile::TempDir;

/// Decimals that `int_part + frac_part / 10^n` rounds one unit in the last
/// place away from the nearest f64.
const VALUES: [&str; 6] = ["1.761", "62938.01", "0.3", "1864.6976840044", "0.06993", "109495.561"];

/// A float written as CSV text reads back as the f64 nearest to that text,
/// on the first ingest (which creates the table) and on a later one.
#[tokio::test]
async fn test_csv_float_reads_back_as_nearest_f64() {
	let temp_dir = TempDir::new().unwrap();
	let mut config = Config::default();
	config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
	config.storage.flush_interval_ms = 0;
	let engine = StorageEngine::new(&config).await.unwrap();
	let table = "float_roundtrip";

	for pass in 0..2u64 {
		let mut csv = String::from("id,timestamp,value\n");
		for (i, value) in VALUES.iter().enumerate() {
			let id = pass * 100 + i as u64 + 1;
			csv.push_str(&format!("{id},{},{value}\n", 1704067200000 + id * 1000));
		}
		engine.ingest_csv(table, csv).await.unwrap();
		engine.flush_table(table).await.unwrap();

		for (i, value) in VALUES.iter().enumerate() {
			let id = pass * 100 + i as u64 + 1;
			let row = engine.get_row_by_id_json(table, id).await.unwrap().unwrap();
			let expected: f64 = value.parse().unwrap();
			let stored = row["value"].as_f64().unwrap();
			assert_eq!(
				stored.to_bits(),
				expected.to_bits(),
				"pass {pass}: {value} read back as {stored}"
			);
		}
	}
}
