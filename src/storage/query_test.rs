use super::*;

#[test]
fn test_parse_query_timestamp() {
    // Unix timestamp (seconds)
    assert!(parse_query_timestamp("1704110400").is_ok());

    // Unix timestamp (milliseconds)
    assert!(parse_query_timestamp("1704110400000").is_ok());

    // ISO format
    assert!(parse_query_timestamp("2024-01-01T10:00:00Z").is_ok());

    // Common format
    assert!(parse_query_timestamp("2024-01-01 10:00:00").is_ok());

    // Invalid
    assert!(parse_query_timestamp("invalid").is_err());
}
