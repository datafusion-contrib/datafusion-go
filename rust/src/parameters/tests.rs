use std::ptr;

use super::*;
use crate::error::*;

#[test]
fn rejects_parameter_index_beyond_parameters_length() {
    let param = dfgo_parameter {
        index: i64::MAX,
        name: ptr::null(),
        name_len: 0,
        type_code: PARAMETER_INT64,
        is_null: 0,
        int64_value: 7,
        uint64_value: 0,
        float64_value: 0.0,
        data: ptr::null(),
        data_len: 0,
        timezone: ptr::null(),
        timezone_len: 0,
        precision: 0,
        scale: 0,
    };
    let err = match bindings_from_params(&param, 1) {
        Err(err) => err,
        Ok(_) => panic!("expected an error for an oversized parameter index"),
    };
    assert_eq!(err.kind, ERROR_KIND_INVALID_ARGUMENT);
    assert!(err.message.contains("exceeds"), "{}", err.message);
}

#[test]
fn parses_decimal128_strings_to_scaled_values() {
    assert_eq!(parse_decimal128("123.45", 10, 2).unwrap(), 12345);
    assert_eq!(parse_decimal128("-.5", 10, 3).unwrap(), -500);
    assert_eq!(parse_decimal128("+0", 1, 0).unwrap(), 0);
    assert!(parse_decimal128("123.456", 10, 2).is_err());
    assert!(parse_decimal128("1000", 3, 0).is_err());
}
