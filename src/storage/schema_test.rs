use super::*;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

#[test]
fn test_infer_schema_from_arrow() {
    // Create a mock DB
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open_default(path.path()).unwrap());
    let manager = SchemaManager::new(db).unwrap();

    // Create an Arrow schema
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", ArrowDataType::Float64, false),
        Field::new("symbol", ArrowDataType::Utf8, false),
        Field::new("is_active", ArrowDataType::Boolean, false),
    ]);

    // Infer Pulsora schema
    let schema = manager
        .infer_schema_from_arrow("test_table", &arrow_schema)
        .unwrap();

    // Verify fields
    assert_eq!(schema.table_name, "test_table");
    assert_eq!(schema.id_column, "id");
    assert_eq!(schema.timestamp_column, Some("timestamp".to_string()));

    // Helper to find column type
    let get_type = |name: &str| -> Option<DataType> {
        schema
            .columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type.clone())
    };

    assert_eq!(get_type("id"), Some(DataType::Id));
    assert_eq!(get_type("timestamp"), Some(DataType::Timestamp));
    assert_eq!(get_type("price"), Some(DataType::Float));
    assert_eq!(get_type("symbol"), Some(DataType::String));
    assert_eq!(get_type("is_active"), Some(DataType::Boolean));
}

#[test]
fn test_infer_schema_from_arrow_auto_id() {
    // Create a mock DB
    let path = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open_default(path.path()).unwrap());
    let manager = SchemaManager::new(db).unwrap();

    // Create an Arrow schema WITHOUT id
    let arrow_schema = ArrowSchema::new(vec![Field::new("value", ArrowDataType::Int64, false)]);

    // Infer Pulsora schema
    let schema = manager
        .infer_schema_from_arrow("test_table_auto", &arrow_schema)
        .unwrap();

    // Verify ID was added
    assert_eq!(schema.id_column, "id");

    let get_type = |name: &str| -> Option<DataType> {
        schema
            .columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type.clone())
    };

    assert_eq!(get_type("id"), Some(DataType::Id));
    assert_eq!(get_type("value"), Some(DataType::Integer));
}
