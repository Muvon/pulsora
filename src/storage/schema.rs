//! Schema management for dynamic type inference and validation
//!
//! This module handles automatic schema detection from CSV data,
//! type inference, and data validation for time series storage.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{PulsoraError, Result};

/// Supported data types for time series data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
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
    /// List of column definitions
    pub columns: Vec<Column>,
    /// Name of the primary timestamp column (if detected)
    pub timestamp_column: Option<String>,
    /// When this schema was created
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct SchemaManager {
    schemas: HashMap<String, Schema>,
}

impl SchemaManager {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn get_schema(&self, table: &str) -> Option<&Schema> {
        self.schemas.get(table)
    }

    pub fn get_or_create_schema(
        &mut self,
        table: &str,
        sample_row: &HashMap<String, String>,
    ) -> Result<Schema> {
        if let Some(schema) = self.schemas.get(table) {
            return Ok(schema.clone());
        }

        // Infer schema from sample row
        let schema = self.infer_schema(table, sample_row)?;
        self.schemas.insert(table.to_string(), schema.clone());

        Ok(schema)
    }

    fn infer_schema(&self, table: &str, sample_row: &HashMap<String, String>) -> Result<Schema> {
        let mut columns = Vec::new();
        let mut timestamp_column = None;

        for (name, value) in sample_row {
            let data_type = self.infer_data_type(value);

            // Check if this could be a timestamp column
            if (data_type == DataType::Timestamp
                || name.to_lowercase().contains("time")
                || name.to_lowercase().contains("date"))
                && timestamp_column.is_none()
            {
                timestamp_column = Some(name.clone());
            }

            columns.push(Column {
                name: name.clone(),
                data_type,
                nullable: false, // For MVP, assume non-nullable
            });
        }

        // Sort columns to ensure consistent ordering
        columns.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(Schema {
            table_name: table.to_string(),
            columns,
            timestamp_column,
            created_at: Utc::now(),
        })
    }

    fn infer_data_type(&self, value: &str) -> DataType {
        // Try to parse as different types in order of specificity

        // Check for timestamp formats
        if self.is_timestamp(value) {
            return DataType::Timestamp;
        }

        // Check for boolean
        if value.to_lowercase() == "true" || value.to_lowercase() == "false" {
            return DataType::Boolean;
        }

        // Check for integer
        if value.parse::<i64>().is_ok() {
            return DataType::Integer;
        }

        // Check for float
        if value.parse::<f64>().is_ok() {
            return DataType::Float;
        }

        // Default to string
        DataType::String
    }

    fn is_timestamp(&self, value: &str) -> bool {
        // Try common timestamp formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%.3fZ",
            "%Y-%m-%d",
            "%s", // Unix timestamp
        ];

        for format in &formats {
            if DateTime::parse_from_str(value, format).is_ok() {
                return true;
            }
        }

        // Check for Unix timestamp (seconds)
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
        // Check that all required columns are present
        for column in &self.columns {
            if !column.nullable && !row.contains_key(&column.name) {
                return Err(PulsoraError::Schema(format!(
                    "Missing required column: {}",
                    column.name
                )));
            }
        }

        // Validate data types
        for (name, value) in row {
            if let Some(column) = self.columns.iter().find(|c| c.name == *name) {
                self.validate_value(&column.data_type, value)?;
            }
        }

        Ok(())
    }

    fn validate_value(&self, expected_type: &DataType, value: &str) -> Result<()> {
        match expected_type {
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
