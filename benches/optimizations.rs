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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pulsora::storage::encoding::{fast_parse_i64, fast_parse_u64};
use std::collections::HashMap;

fn benchmark_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parsing");

    // Test fast_parse_u64 vs standard parse
    group.bench_function("fast_parse_u64", |b| {
        b.iter(|| fast_parse_u64(black_box("1234567890")))
    });

    group.bench_function("std_parse_u64", |b| {
        b.iter(|| black_box("1234567890").parse::<u64>())
    });

    // Test fast_parse_i64 vs standard parse
    group.bench_function("fast_parse_i64", |b| {
        b.iter(|| fast_parse_i64(black_box("-1234567890")))
    });

    group.bench_function("std_parse_i64", |b| {
        b.iter(|| black_box("-1234567890").parse::<i64>())
    });

    group.finish();
}

fn benchmark_collections(c: &mut Criterion) {
    let mut group = c.benchmark_group("collections");

    // Test HashMap with capacity vs without
    group.bench_function("hashmap_with_capacity", |b| {
        b.iter(|| {
            let mut map = HashMap::with_capacity(100);
            for i in 0..100 {
                map.insert(i.to_string(), i);
            }
            black_box(map)
        })
    });

    group.bench_function("hashmap_no_capacity", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..100 {
                map.insert(i.to_string(), i);
            }
            black_box(map)
        })
    });

    group.finish();
}

fn benchmark_formatting(c: &mut Criterion) {
    let mut group = c.benchmark_group("formatting");

    // Test itoa vs to_string for integers
    group.bench_function("itoa_format", |b| {
        let value = 1234567890u64;
        b.iter(|| itoa::Buffer::new().format(black_box(value)).to_string())
    });

    group.bench_function("std_to_string", |b| {
        let value = 1234567890u64;
        b.iter(|| black_box(value).to_string())
    });

    // Test ryu vs to_string for floats
    group.bench_function("ryu_format", |b| {
        let value = 1234.567890f64;
        b.iter(|| ryu::Buffer::new().format(black_box(value)).to_string())
    });

    group.bench_function("float_to_string", |b| {
        let value = 1234.567890f64;
        b.iter(|| black_box(value).to_string())
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_parsing,
    benchmark_collections,
    benchmark_formatting
);
criterion_main!(benches);
