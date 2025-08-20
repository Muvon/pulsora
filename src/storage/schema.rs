//! Schema management for dynamic type inference and validation
//!
//! This module handles automatic schema detection from CSV data,
//! type inference, and data validation for time series storage.

use chrono::{DateTime, Utc};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{PulsoraError, Result};

/// Supported data types for time series data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    /// 64-bit unsigned integer ID (special type for row IDs)
    Id,
    /// 64-bit signed integer
    Integer,
    /// 64-bit floating point number
    Float,
    /// UTF-8 string
    String,
    /// Boolean true/false value
    Boolean,
    /// Timestamp in various formats
    Timestamp,
}

/// Column definition with name, type, and constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name from CSV header
    pub name: String,
    /// Inferred or specified data type
    pub data_type: DataType,
    /// Whether the column can contain null values (currently always false for MVP)
    pub nullable: bool,
}

/// Table schema with column definitions and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Name of the table this schema belongs to
    pub table_name: String,
    /// List of column definitions (includes ID column)
    pub columns: Vec<Column>,
    /// Ordered list of column names for consistent encoding/decoding
    pub column_order: Vec<String>,
    /// Name of the primary timestamp column (if detected)
    pub timestamp_column: Option<String>,
    /// Name of the ID column (always "id")
    pub id_column: String,
    /// When this schema was created
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct SchemaManager {
    schemas: HashMap<String, Schema>,
    db: Arc<DB>,
}

impl SchemaManager {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let mut manager = Self {
            schemas: HashMap::new(),
            db,
        };

        // Load existing schemas from RocksDB on startup
        manager.load_schemas_from_db()?;

        Ok(manager)
    }

    /// Load all schemas from RocksDB into memory
    fn load_schemas_from_db(&mut self) -> Result<()> {
        let schema_prefix = b"_schema_";
        let iter = self.db.prefix_iterator(schema_prefix);

        for item in iter {
            let (key, value) = item.map_err(PulsoraError::RocksDb)?;

            // Extract table name from key: "_schema_{table_name}"
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(table_name) = key_str.strip_prefix("_schema_") {
                    // Deserialize schema
                    match serde_json::from_slice::<Schema>(&value) {
                        Ok(schema) => {
                            tracing::info!("Loaded schema for table: {}", table_name);
                            self.schemas.insert(table_name.to_string(), schema);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to deserialize schema for table {}: {}",
                                table_name,
                                e
                            );
                        }
                    }
                }
            }
        }

        tracing::info!("Loaded {} schemas from database", self.schemas.len());
        Ok(())
    }

    /// Save schema to RocksDB
    fn save_schema_to_db(&self, table: &str, schema: &Schema) -> Result<()> {
        let key = format!("_schema_{}", table);
        let value = serde_json::to_vec(schema)
            .map_err(|e| PulsoraError::Schema(format!("Failed to serialize schema: {}", e)))?;

        self.db
            .put(key.as_bytes(), &value)
            .map_err(PulsoraError::RocksDb)?;

        tracing::info!("Saved schema for table: {}", table);
        Ok(())
    }

    pub fn get_schema(&self, table: &str) -> Option<&Schema> {
        self.schemas.get(table)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    pub fn get_or_create_schema(
        &mut self,
        table: &str,
        sample_rows: &[HashMap<String, String>],
    ) -> Result<Schema> {
        if let Some(schema) = self.schemas.get(table) {
            return Ok(schema.clone());
        }

        // Infer schema from ALL sample rows for better accuracy
        let schema = self.infer_schema_from_rows(table, sample_rows)?;

        // Save to both memory and RocksDB
        self.save_schema_to_db(table, &schema)?;
        self.schemas.insert(table.to_string(), schema.clone());

        Ok(schema)
    }

    fn infer_schema_from_rows(
        &self,
        table: &str,
        sample_rows: &[HashMap<String, String>],
    ) -> Result<Schema> {
        if sample_rows.is_empty() {
            return Err(PulsoraError::Schema(
                "Cannot infer schema from empty data".to_string(),
            ));
        }

        // Get all column names from first row
        let first_row = &sample_rows[0];
        let mut column_types: HashMap<String, DataType> = HashMap::new();
        let mut timestamp_column = None;
        let mut has_id_column = false;

        // Initialize with column names
        for name in first_row.keys() {
            if name == "id" {
                has_id_column = true;
                column_types.insert(name.clone(), DataType::Id); // ID is always Id type
            } else {
                column_types.insert(name.clone(), DataType::String); // Start with most general type
            }
        }

        // Always ensure ID column exists
        if !has_id_column {
            column_types.insert("id".to_string(), DataType::Id);
        }

        // Analyze ALL rows to determine the most specific type for each column
        for row in sample_rows {
            for (name, value) in row {
                if name == "id" {
                    // ID column is always Integer, but validate if provided
                    if !value.trim().is_empty()
                        && (value.parse::<u64>().is_err() || value.parse::<u64>().unwrap_or(0) == 0)
                    {
                        return Err(PulsoraError::Schema(format!(
                            "Invalid ID value '{}': ID must be a positive integer",
                            value
                        )));
                    }
                    // Keep as Integer type
                } else if let Some(current_type) = column_types.get_mut(name) {
                    let inferred_type = self.infer_data_type(value);
                    *current_type = self.merge_types(current_type.clone(), inferred_type);
                }
            }
        }

        // Build columns with final types
        let mut columns = Vec::new();
        for (name, data_type) in &column_types {
            // Check if this could be a timestamp column (but not ID)
            if name != "id"
                && (data_type == &DataType::Timestamp
                    || name.to_lowercase().contains("time")
                    || name.to_lowercase().contains("date"))
                && timestamp_column.is_none()
            {
                timestamp_column = Some(name.clone());
            }

            columns.push(Column {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable: name != "id", // ID is never nullable, others can be for future
            });
        }

        // Sort columns to ensure consistent ordering, but keep ID first for efficiency
        columns.sort_by(|a, b| {
            if a.name == "id" {
                std::cmp::Ordering::Less
            } else if b.name == "id" {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });

        // Create column order list (ID first, then alphabetical)
        let column_order = columns.iter().map(|c| c.name.clone()).collect();

        Ok(Schema {
            table_name: table.to_string(),
            columns,
            column_order,
            timestamp_column,
            id_column: "id".to_string(),
            created_at: Utc::now(),
        })
    }

    /// Merge two data types, returning the more general type that can hold both
    fn merge_types(&self, type1: DataType, type2: DataType) -> DataType {
        use DataType::*;

        match (type1, type2) {
            // Same type - no change
            (t1, t2) if t1 == t2 => t1,

            // String is the most general - always wins
            (String, _) | (_, String) => String,

            // Timestamp is specific - only if both are timestamps
            (Timestamp, Timestamp) => Timestamp,
            (Timestamp, _) | (_, Timestamp) => String,

            // Boolean is specific - only if both are booleans
            (Boolean, Boolean) => Boolean,
            (Boolean, _) | (_, Boolean) => String,

            // Integer can be promoted to Float
            (Integer, Float) | (Float, Integer) => Float,

            // Everything else becomes String
            _ => String,
        }
    }

    fn infer_data_type(&self, value: &str) -> DataType {
        // Empty or whitespace-only values are treated as String
        if value.trim().is_empty() {
            return DataType::String;
        }

        // Check for timestamp formats FIRST (most specific)
        if self.is_timestamp(value) {
            return DataType::Timestamp;
        }

        // Check for boolean (case insensitive)
        let lower = value.to_lowercase();
        if lower == "true" || lower == "false" {
            return DataType::Boolean;
        }

        // Check for integer (including negative)
        // Important: Check integer before float to avoid false positives
        if value.parse::<i64>().is_ok() {
            // Double-check it's not a float with .0
            if !value.contains('.') && !value.contains('e') && !value.contains('E') {
                return DataType::Integer;
            }
        }

        // Check for float
        if value.parse::<f64>().is_ok() {
            return DataType::Float;
        }

        // Default to string
        DataType::String
    }

    fn is_timestamp(&self, value: &str) -> bool {
        // Try RFC3339 formats first (with timezone)
        let rfc3339_formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%.3fZ"];

        for format in &rfc3339_formats {
            if DateTime::parse_from_str(value, format).is_ok() {
                return true;
            }
        }

        // Try parsing as RFC3339 directly
        if DateTime::parse_from_rfc3339(value).is_ok() {
            return true;
        }

        // Try naive datetime formats (without timezone)
        let naive_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"];

        for format in &naive_formats {
            if chrono::NaiveDateTime::parse_from_str(value, format).is_ok() {
                return true;
            }
        }

        // Try naive date format
        if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
            return true;
        }

        // Check for Unix timestamp (seconds) - must be reasonable range
        if let Ok(timestamp) = value.parse::<i64>() {
            if timestamp > 1_000_000_000 && timestamp < 4_000_000_000 {
                return true;
            }
        }

        false
    }
}

impl Schema {
    pub fn validate_row(&self, row: &HashMap<String, String>) -> Result<()> {
        // ID column is special - it can be missing (auto-assigned) or empty (auto-assigned)
        // but if present and non-empty, must be valid
        if let Some(id_value) = row.get(&self.id_column) {
            if !id_value.trim().is_empty() {
                // Validate ID if provided
                if id_value.parse::<u64>().is_err() || id_value.parse::<u64>().unwrap_or(0) == 0 {
                    return Err(PulsoraError::Schema(format!(
                        "Invalid ID value '{}': ID must be a positive integer",
                        id_value
                    )));
                }
            }
        }

        // Check that all other required columns are present
        for column in &self.columns {
            if !column.nullable && column.name != self.id_column && !row.contains_key(&column.name)
            {
                return Err(PulsoraError::Schema(format!(
                    "Missing required column: {}",
                    column.name
                )));
            }
        }

        // Validate data types for non-ID columns
        for (name, value) in row {
            if name != &self.id_column {
                if let Some(column) = self.columns.iter().find(|c| c.name == *name) {
                    self.validate_value(&column.data_type, value)?;
                }
            }
        }

        Ok(())
    }

    fn validate_value(&self, expected_type: &DataType, value: &str) -> Result<()> {
        match expected_type {
            DataType::Id => {
                // ID must be positive u64
                if value.trim().is_empty() {
                    return Ok(()); // Empty is OK - will be auto-assigned
                }
                let parsed = value.parse::<u64>().map_err(|_| {
                    PulsoraError::InvalidData(format!("Invalid ID value: {}", value))
                })?;
                if parsed == 0 {
                    return Err(PulsoraError::InvalidData(
                        "ID must be positive (> 0)".to_string(),
                    ));
                }
            }
            DataType::Integer => {
                value.parse::<i64>().map_err(|_| {
                    PulsoraError::InvalidData(format!("Invalid integer: {}", value))
                })?;
            }
            DataType::Float => {
                value
                    .parse::<f64>()
                    .map_err(|_| PulsoraError::InvalidData(format!("Invalid float: {}", value)))?;
            }
            DataType::Boolean => {
                let lower = value.to_lowercase();
                if lower != "true" && lower != "false" {
                    return Err(PulsoraError::InvalidData(format!(
                        "Invalid boolean: {}",
                        value
                    )));
                }
            }
            DataType::Timestamp => {
                // Validation handled during parsing
            }
            DataType::String => {
                // Strings are always valid
            }
        }

        Ok(())
    }

    pub fn get_timestamp_column(&self) -> Option<&str> {
        self.timestamp_column.as_deref()
    }
}
