use super::*;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_default_config() {
    let config = Config::default();
    assert_eq!(config.server.host, "0.0.0.0");
    assert_eq!(config.server.port, 8080);
    assert_eq!(config.storage.write_buffer_size_mb, 64);
    assert_eq!(config.performance.compression, "lz4");
    assert_eq!(config.logging.level, "info");
}

#[test]
fn test_validate_valid_config() {
    let config = Config::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_validate_invalid_port() {
    let mut config = Config::default();
    config.server.port = 0;
    assert!(config.validate().is_err());
}

#[test]
fn test_validate_invalid_buffer_size() {
    let mut config = Config::default();
    config.storage.write_buffer_size_mb = 0;
    assert!(config.validate().is_err());
}

#[test]
fn test_validate_invalid_batch_size() {
    let mut config = Config::default();
    config.ingestion.batch_size = 0;
    assert!(config.validate().is_err());
}

#[test]
fn test_validate_invalid_compression() {
    let mut config = Config::default();
    config.performance.compression = "invalid".to_string();
    assert!(config.validate().is_err());
}

#[test]
fn test_validate_valid_compressions() {
    let valid = ["none", "snappy", "lz4", "zstd"];
    for comp in valid {
        let mut config = Config::default();
        config.performance.compression = comp.to_string();
        assert!(
            config.validate().is_ok(),
            "Compression {} should be valid",
            comp
        );
    }
}

#[test]
fn test_validate_invalid_log_level() {
    let mut config = Config::default();
    config.logging.level = "invalid".to_string();
    assert!(config.validate().is_err());
}

#[test]
fn test_from_file() {
    let mut file = NamedTempFile::new().unwrap();
    let config_str = r#"
[server]
host = "127.0.0.1"
port = 9090
max_body_size_mb = 10

[storage]
data_dir = "/tmp/pulsora"
write_buffer_size_mb = 128
max_open_files = 500
buffer_size = 2000
flush_interval_ms = 5000
wal_enabled = true

[ingestion]
max_csv_size_mb = 100
batch_size = 5000
ingestion_threads = 4

[performance]
compression = "zstd"
cache_size_mb = 512

[logging]
level = "debug"
format = "json"
enable_access_logs = false
enable_performance_logs = false
"#;
    write!(file, "{}", config_str).unwrap();

    let config = Config::from_file(file.path().to_path_buf()).unwrap();

    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(config.server.port, 9090);
    assert_eq!(config.storage.write_buffer_size_mb, 128);
    assert_eq!(config.performance.compression, "zstd");
    assert_eq!(config.logging.level, "debug");
    assert!(!config.logging.enable_access_logs);
}
