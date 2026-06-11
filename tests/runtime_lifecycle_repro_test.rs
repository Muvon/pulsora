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

//! Reproducer for the `JoinHandle polled after completion` panic seen during
//! `cargo bench --bench query` at `concurrent_queries/concurrent_reads/8`.
//!
//! The failing benchmark has this exact shape (see `benches/query.rs`):
//!
//!   bench_concurrent_queries() {
//!       // ---- OUTER runtime lives for the whole benchmark group ----
//!       let outer_rt = Runtime::new();
//!       let schema = outer_rt.block_on(...);
//!
//!       group.bench_with_input(..., |b, ...| {
//!           b.iter_batched(
//!               || {
//!                   // setup: TWO nested runtimes created and dropped
//!                   //        per criterion sample, with a StorageEngine
//!                   //        outliving them.
//!                   let (storage, _temp) = create_test_storage_with_data(1000);
//!                   //     ↑ this fn creates its own Runtime, ingests via
//!                   //       block_on, then drops the Runtime. The
//!                   //       StorageEngine has background `tokio::spawn`d
//!                   //       flush/WAL tasks that get aborted by that drop.
//!                   let rt = Runtime::new();
//!                   let schema = rt.block_on(...);
//!                   (storage, schema)
//!                   // both runtimes drop here (in unspecified order
//!                   //                          relative to criterion's
//!                   //                          internal batching).
//!               },
//!               |(storage, schema)| {
//!                   // iter: fan out N std::thread workers running the
//!                   //       sync execute_query(). No tokio runtime
//!                   //       directly involved in the iter itself.
//!                   ...
//!               },
//!               BatchSize::SmallInput,
//!           )
//!       });
//!   }
//!
//! Criterion runs the setup + iter pair hundreds of times per sample group
//! (warmup + 100 measurement samples), so we need volume to hit the race.
//! The two test functions below replay the same lifecycle:
//!
//!   * `repro_short` — quick smoke test, 50 iterations.
//!   * `repro_long`  — full bench-scale volume, ITERATIONS_LONG cycles,
//!     ignored by default so it doesn't slow the test
//!     suite. Run explicitly with:
//!
//!         cargo test --test runtime_lifecycle_repro_test -- \
//!             --ignored --nocapture --test-threads=1
//!
//! Tunables are at the top of the file. If neither shape reproduces in a
//! few attempts, the actual trigger is criterion's measurement scheduling
//! (the harness can sometimes execute batches on worker threads it owns),
//! and we'll need to instrument inside the running bench itself.

use pulsora::config::Config;
use pulsora::storage::{query, StorageEngine};
use tempfile::TempDir;

const ITERATIONS_SHORT: usize = 50;
const ITERATIONS_LONG: usize = 600;
const THREADS: usize = 8;
const ROWS: usize = 1000;

fn build_csv(rows: usize) -> String {
    let mut csv = String::from("timestamp,symbol,price,volume\n");
    for i in 0..rows {
        let total = i;
        let hours = (10 + (total / 3600)) % 24;
        let minutes = (total % 3600) / 60;
        let seconds = total % 60;
        csv.push_str(&format!(
            "2024-01-01 {:02}:{:02}:{:02},AAPL,{:.2},{}\n",
            hours,
            minutes,
            seconds,
            150.0 + (i as f64 * 0.01),
            1000 + i
        ));
    }
    csv
}

/// Byte-for-byte equivalent of `create_test_storage_with_data` in
/// `benches/query.rs`: build a runtime, `block_on` the engine creation +
/// ingest, then **drop** the runtime while returning the engine.
fn create_test_storage_with_data(rows: usize) -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = rt.block_on(async {
        let storage = StorageEngine::new(&config).await.unwrap();
        storage
            .ingest_csv("benchmark_table", build_csv(rows))
            .await
            .unwrap();
        storage
    });
    // rt drops here — background flush task + per-table WAL writer task
    // were spawned on this rt and now get aborted while the StorageEngine
    // (which holds the Arc<DB> they reference) lives on.
    (storage, temp_dir)
}

/// One bench iteration's worth of work, structured exactly like the
/// failing bench's `iter_batched` setup + iter closure.
fn one_iteration() {
    // SETUP — mirrors the bench's `iter_batched` setup closure verbatim.
    let (storage, _temp_dir) = create_test_storage_with_data(ROWS);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = rt.block_on(async {
        let schemas = storage.schemas.read().await;
        schemas.get_schema("benchmark_table").unwrap().clone()
    });
    // `rt` drops here (end of setup closure scope).
    drop(rt);
    // `_temp_dir` drops here in the bench too — same shape.
    // In the bench, only `(storage, schema)` is returned and criterion
    // then runs the iter closure with that tuple. We do the same below.

    // ITER — fan out THREADS sync queries.
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let storage = storage.clone();
            let schema = schema.clone();
            let pool = pool.clone();
            std::thread::spawn(move || {
                query::execute_query(
                    &storage.db,
                    "benchmark_table",
                    &schema,
                    None,
                    None,
                    Some(100),
                    None,
                    &pool,
                )
                .unwrap()
            })
        })
        .collect();

    for h in handles {
        let _ = h.join();
    }

    // (storage, schema) drop at end of one_iteration scope. _temp_dir
    // already dropped above.
}

/// Hold a long-lived OUTER runtime for the duration of the loop — exactly
/// what `bench_concurrent_queries` does at the function level around its
/// per-iteration drop/recreate cycle.
fn run_with_outer_runtime(iterations: usize) {
    let outer_rt = tokio::runtime::Runtime::new().unwrap();

    // Use the outer runtime once for symmetry with the bench's initial
    // `rt.block_on` (which fetches the schema for the bench setup).
    outer_rt.block_on(async { tokio::task::yield_now().await });

    for i in 0..iterations {
        if i % 50 == 0 {
            eprintln!("  iter {}/{}", i + 1, iterations);
        }
        one_iteration();
    }

    drop(outer_rt);
}

#[test]
fn repro_short() {
    eprintln!("short reproducer ({} iterations)", ITERATIONS_SHORT);
    run_with_outer_runtime(ITERATIONS_SHORT);
}

#[test]
#[ignore = "long-running reproducer; run explicitly with `--ignored`"]
fn repro_long() {
    eprintln!("long reproducer ({} iterations)", ITERATIONS_LONG);
    run_with_outer_runtime(ITERATIONS_LONG);
}
