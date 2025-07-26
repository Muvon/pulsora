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
