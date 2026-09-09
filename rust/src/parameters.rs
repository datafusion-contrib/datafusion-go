//! Copy borrowed C parameters into owned DataFusion scalar values.

use std::ffi::c_char;
use std::slice;
use std::sync::Arc;

use datafusion::common::ScalarValue;

use crate::abi::{bytes_from_ptr, bytes_to_string, dfgo_parameter};
use crate::error::FfiError;
use crate::query::Binding;

pub(crate) const PARAMETER_NULL: i32 = 0;
pub(crate) const PARAMETER_BOOL: i32 = 1;
pub(crate) const PARAMETER_INT64: i32 = 2;
pub(crate) const PARAMETER_UINT64: i32 = 3;
pub(crate) const PARAMETER_FLOAT64: i32 = 4;
pub(crate) const PARAMETER_STRING: i32 = 5;
pub(crate) const PARAMETER_BINARY: i32 = 6;
pub(crate) const PARAMETER_DATE: i32 = 7;
pub(crate) const PARAMETER_TIME: i32 = 8;
pub(crate) const PARAMETER_TIMESTAMP: i32 = 9;
pub(crate) const PARAMETER_DURATION: i32 = 10;
pub(crate) const PARAMETER_DECIMAL: i32 = 11;

pub(crate) fn optional_timezone(
    ptr: *const c_char,
    len: i64,
) -> Result<Option<Arc<str>>, FfiError> {
    let timezone = bytes_to_string(ptr, len, "timezone")?;
    if timezone.is_empty() {
        // DataFusion/Arrow represent a timestamp without a zone as None. The Go
        // wrapper passes an empty string for that case instead of a nullable
        // pointer so pointer validation stays uniform.
        Ok(None)
    } else {
        Ok(Some(Arc::<str>::from(timezone.as_str())))
    }
}

pub(crate) fn validate_decimal_type(precision: u8, scale: i8) -> Result<(), FfiError> {
    // Arrow Decimal128 supports at most 38 base-10 digits. Validate before
    // constructing ScalarValue so bad user input is reported as invalid_argument
    // rather than a dependency-level native error.
    if precision == 0 || precision > 38 {
        return Err(FfiError::invalid_argument(format!(
            "decimal precision must be in [1,38], got {precision}"
        )));
    }
    if scale < 0 || scale as u8 > precision {
        return Err(FfiError::invalid_argument(format!(
            "decimal scale must be in [0,{precision}], got {scale}"
        )));
    }
    Ok(())
}

pub(crate) fn parse_decimal128(value: &str, precision: u8, scale: i8) -> Result<i128, FfiError> {
    validate_decimal_type(precision, scale)?;

    // Parse decimal strings ourselves instead of going through f64. This keeps
    // parameter binding exact and avoids accepting scientific notation or other
    // formats that Arrow Decimal128 would not round-trip predictably here.
    let value = value.trim();
    if value.is_empty() {
        return Err(FfiError::invalid_argument("decimal value is empty"));
    }

    let (negative, digits) = match value.as_bytes()[0] {
        b'-' => (true, &value[1..]),
        b'+' => (false, &value[1..]),
        _ => (false, value),
    };
    if digits.is_empty() {
        return Err(FfiError::invalid_argument(format!(
            "invalid decimal value {value:?}"
        )));
    }

    let parts: Vec<&str> = digits.split('.').collect();
    if parts.len() > 2 {
        return Err(FfiError::invalid_argument(format!(
            "invalid decimal value {value:?}"
        )));
    }

    let int_part = parts[0];
    let frac_part = if parts.len() == 2 { parts[1] } else { "" };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(FfiError::invalid_argument(format!(
            "invalid decimal value {value:?}"
        )));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(FfiError::invalid_argument(format!(
            "invalid decimal value {value:?}"
        )));
    }

    let scale = usize::try_from(scale).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
    if frac_part.len() > scale {
        return Err(FfiError::invalid_argument(format!(
            "decimal value {value:?} has more fractional digits than scale {scale}"
        )));
    }

    // Arrow stores Decimal128 as the integer value scaled by 10^scale, so
    // "12.3" with scale 2 becomes 1230.
    let mut scaled_digits = String::with_capacity(int_part.len() + scale);
    scaled_digits.push_str(int_part);
    scaled_digits.push_str(frac_part);
    for _ in frac_part.len()..scale {
        scaled_digits.push('0');
    }

    let significant = scaled_digits.trim_start_matches('0');
    // A zero value still consumes one digit of precision, matching Decimal128
    // semantics and avoiding a special "zero has precision 0" interpretation.
    let significant_len = if significant.is_empty() {
        1
    } else {
        significant.len()
    };
    if significant_len > usize::from(precision) {
        return Err(FfiError::invalid_argument(format!(
            "decimal value {value:?} exceeds precision {precision}"
        )));
    }

    let mut parsed = if scaled_digits.is_empty() {
        0
    } else {
        scaled_digits.parse::<i128>().map_err(|e| {
            FfiError::invalid_argument(format!("invalid decimal value {value:?}: {e}"))
        })?
    };
    if negative {
        parsed = -parsed;
    }
    Ok(parsed)
}

pub(crate) fn typed_null(
    type_code: i32,
    precision: u8,
    scale: i8,
    timezone: Option<Arc<str>>,
) -> Result<ScalarValue, FfiError> {
    // database/sql sends nil without a type. These explicit typed nulls let Go
    // callers control DataFusion inference when plain ScalarValue::Null is too
    // ambiguous for the query.
    match type_code {
        PARAMETER_BOOL => Ok(ScalarValue::Boolean(None)),
        PARAMETER_INT64 => Ok(ScalarValue::Int64(None)),
        PARAMETER_UINT64 => Ok(ScalarValue::UInt64(None)),
        PARAMETER_FLOAT64 => Ok(ScalarValue::Float64(None)),
        PARAMETER_STRING => Ok(ScalarValue::Utf8(None)),
        PARAMETER_BINARY => Ok(ScalarValue::Binary(None)),
        PARAMETER_DATE => Ok(ScalarValue::Date32(None)),
        PARAMETER_TIME => Ok(ScalarValue::Time64Nanosecond(None)),
        PARAMETER_TIMESTAMP => Ok(ScalarValue::TimestampNanosecond(None, timezone)),
        PARAMETER_DURATION => Ok(ScalarValue::DurationNanosecond(None)),
        PARAMETER_DECIMAL => {
            validate_decimal_type(precision, scale)?;
            Ok(ScalarValue::Decimal128(None, precision, scale))
        }
        other => Err(FfiError::invalid_argument(format!(
            "unsupported typed null parameter type {other}"
        ))),
    }
}

pub(crate) fn bindings_from_params(
    params: *const dfgo_parameter,
    params_len: i64,
) -> Result<Vec<Binding>, FfiError> {
    if params.is_null() && params_len != 0 {
        return Err(FfiError::invalid_argument("parameters pointer is null"));
    }
    if params_len < 0 {
        return Err(FfiError::invalid_argument(format!(
            "parameters length must be non-negative, got {params_len}"
        )));
    }

    let params_len =
        usize::try_from(params_len).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
    let params = if params_len == 0 {
        &[]
    } else {
        // SAFETY: non-empty parameter arrays require a non-null pointer,
        // negative lengths were rejected, and the borrow lasts only for this
        // FFI call. Every nested pointer is copied into owned Rust data below.
        unsafe { slice::from_raw_parts(params, params_len) }
    };

    let mut bindings = Vec::new();
    for param in params {
        if param.index <= 0 {
            return Err(FfiError::invalid_argument(format!(
                "parameter index must be positive, got {}",
                param.index
            )));
        }

        let index = usize::try_from(param.index - 1)
            .map_err(|e| FfiError::invalid_argument(e.to_string()))?;
        // Dense bindings need at most params_len slots. Bound the allocation
        // before resize_with uses the caller's index.
        if index >= params_len {
            return Err(FfiError::invalid_argument(format!(
                "parameter index {} exceeds parameters length {params_len}",
                param.index
            )));
        }
        if bindings.len() <= index {
            bindings.resize_with(index + 1, || Binding {
                name: None,
                value: None,
            });
        }

        bindings[index] = Binding {
            name: parameter_name(param)?,
            value: Some(parameter_value(param)?),
        };
    }

    Ok(bindings)
}

pub(crate) fn parameter_name(param: &dfgo_parameter) -> Result<Option<String>, FfiError> {
    if param.name.is_null() && param.name_len == 0 {
        return Ok(None);
    }

    let name = bytes_to_string(param.name, param.name_len, "parameter name")?;
    if name.is_empty() {
        return Err(FfiError::invalid_argument("parameter name is empty"));
    }
    Ok(Some(name))
}

pub(crate) fn parameter_value(param: &dfgo_parameter) -> Result<ScalarValue, FfiError> {
    if param.is_null != 0 {
        if param.type_code == PARAMETER_NULL {
            return Ok(ScalarValue::Null);
        }
        return typed_null(
            param.type_code,
            param.precision,
            param.scale,
            optional_timezone(param.timezone, param.timezone_len)?,
        );
    }

    match param.type_code {
        PARAMETER_BOOL => Ok(ScalarValue::Boolean(Some(param.int64_value != 0))),
        PARAMETER_INT64 => Ok(ScalarValue::Int64(Some(param.int64_value))),
        PARAMETER_UINT64 => Ok(ScalarValue::UInt64(Some(param.uint64_value))),
        PARAMETER_FLOAT64 => Ok(ScalarValue::Float64(Some(param.float64_value))),
        PARAMETER_STRING => {
            let value = bytes_to_string(
                param.data.cast::<c_char>(),
                param.data_len,
                "string parameter",
            )?;
            Ok(ScalarValue::Utf8(Some(value)))
        }
        PARAMETER_BINARY => {
            let bytes = bytes_from_ptr(param.data, param.data_len, "binary parameter")?;
            Ok(ScalarValue::Binary(Some(bytes.to_vec())))
        }
        PARAMETER_DATE => {
            let days = i32::try_from(param.int64_value)
                .map_err(|e| FfiError::invalid_argument(e.to_string()))?;
            Ok(ScalarValue::Date32(Some(days)))
        }
        PARAMETER_TIME => Ok(ScalarValue::Time64Nanosecond(Some(param.int64_value))),
        PARAMETER_TIMESTAMP => Ok(ScalarValue::TimestampNanosecond(
            Some(param.int64_value),
            optional_timezone(param.timezone, param.timezone_len)?,
        )),
        PARAMETER_DURATION => Ok(ScalarValue::DurationNanosecond(Some(param.int64_value))),
        PARAMETER_DECIMAL => {
            let value =
                bytes_to_string(param.data.cast::<c_char>(), param.data_len, "decimal value")?;
            let scaled = parse_decimal128(&value, param.precision, param.scale)?;
            Ok(ScalarValue::Decimal128(
                Some(scaled),
                param.precision,
                param.scale,
            ))
        }
        other => Err(FfiError::invalid_argument(format!(
            "unsupported parameter type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests;
