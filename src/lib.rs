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

//! Pulsora - High-performance time series database
//!
//! A Rust-based time series database optimized for market data and similar
//! time-ordered datasets, built on RocksDB with a REST API interface.

pub mod config;
pub mod error;
pub mod server;
pub mod storage;

pub use config::Config;
pub use error::{PulsoraError, Result};
pub use server::start;
pub use storage::StorageEngine;
