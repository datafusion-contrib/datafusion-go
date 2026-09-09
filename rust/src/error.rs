//! Stable native errors and containment for fallible C entry points.

use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use arrow::error::ArrowError;
use datafusion::arrow;
use datafusion::common::DataFusionError;

use crate::abi::dfgo_error;

pub(crate) const DFG_OK: i32 = 0;
pub(crate) const DFG_ERR: i32 = 1;
pub(crate) const CANCELLED_MESSAGE: &str = "query canceled";
pub(crate) const ERROR_KIND_CANCELLED: &str = "cancelled";
pub(crate) const ERROR_KIND_INVALID_ARGUMENT: &str = "invalid_argument";
pub(crate) const ERROR_KIND_NATIVE: &str = "native";
pub(crate) const ERROR_KIND_PANIC: &str = "panic";
#[derive(Debug)]
pub(crate) struct FfiError {
    // `kind` is static because callers branch on stable symbolic categories.
    pub(crate) kind: &'static str,
    // `message` is owned because most errors are formatted at the failing site.
    pub(crate) message: String,
}

impl FfiError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(ERROR_KIND_CANCELLED, CANCELLED_MESSAGE)
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ERROR_KIND_INVALID_ARGUMENT, message)
    }

    pub(crate) fn native(message: impl Into<String>) -> Self {
        Self::new(ERROR_KIND_NATIVE, message)
    }

    pub(crate) fn panic() -> Self {
        Self::new(
            ERROR_KIND_PANIC,
            "panic across datafusion-go native boundary",
        )
    }
}

impl From<DataFusionError> for FfiError {
    fn from(value: DataFusionError) -> Self {
        Self::native(value.to_string())
    }
}

impl From<ArrowError> for FfiError {
    fn from(value: ArrowError) -> Self {
        Self::native(value.to_string())
    }
}

pub(crate) fn set_error(err: *mut *mut dfgo_error, ffi_err: FfiError) {
    if err.is_null() {
        return;
    }

    // CString cannot contain interior NUL bytes. Error messages can originate
    // from dependencies, so sanitize them instead of panicking across the ABI.
    let message = ffi_err.message.replace('\0', "\\0");
    let error = dfgo_error {
        kind: CString::new(ffi_err.kind).expect("static error kind has no nul bytes"),
        message: CString::new(message).expect("nul bytes were replaced"),
    };

    // SAFETY: `err` was checked for null above. The written pointer is produced
    // by Box::into_raw and remains valid until dfgo_error_free receives it.
    unsafe {
        *err = Box::into_raw(Box::new(error));
    }
}

// Clear the caller's error slot before each fallible call. This prevents stale
// error handles from being interpreted as the result of a successful call.
pub(crate) fn clear_error(err: *mut *mut dfgo_error) {
    if !err.is_null() {
        // SAFETY: `err` is non-null and points to caller-provided storage for an
        // optional error handle. Writing null does not take ownership of any
        // previous value; callers are expected to free errors they read.
        unsafe {
            *err = ptr::null_mut();
        }
    }
}

// Convert a required NUL-terminated C string into owned UTF-8. This is used for
// short-lived inputs such as DSNs, table names, SQL text, and parameter names.
pub(crate) fn run_ffi(err: *mut *mut dfgo_error, f: impl FnOnce() -> Result<(), FfiError>) -> i32 {
    clear_error(err);

    // No panic may unwind into C. Every exported fallible function goes through
    // this wrapper so panics become a stable native error kind instead of
    // undefined behavior at the language boundary.
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => DFG_OK,
        Ok(Err(ffi_err)) => {
            set_error(err, ffi_err);
            DFG_ERR
        }
        Err(_) => {
            set_error(err, FfiError::panic());
            DFG_ERR
        }
    }
}
