#![no_main]

use std::ffi::CString;
use std::ptr;

use datafusion_go::*;
use libfuzzer_sys::fuzz_target;

// Exercise parsing, parameter metadata, diagnostics, and FFI cleanup using
// allocated, NUL-terminated input. No arbitrary input becomes a raw pointer.
// Preparing a statement does not execute SQL or perform external I/O.
fuzz_target!(|input: &str| {
    if input.len() > 8192 {
        return;
    }
    let Ok(query) = CString::new(input) else {
        return;
    };
    unsafe {
        let mut db = ptr::null_mut();
        let mut conn = ptr::null_mut();
        let mut stmt = ptr::null_mut();
        let mut error = ptr::null_mut();
        assert_eq!(dfgo_database_open(ptr::null(), &mut db, &mut error), 0);
        assert_eq!(dfgo_connection_open_isolated(db, &mut conn, &mut error), 0);
        let status = dfgo_prepare(conn, query.as_ptr(), &mut stmt, &mut error);
        if status == 0 {
            assert!(!stmt.is_null() && error.is_null());
            assert!(dfgo_statement_num_params(stmt) >= 0);
            assert!([0, 1].contains(&dfgo_statement_serializes(stmt)));
            dfgo_statement_close(stmt);
        } else {
            assert!(stmt.is_null() && !error.is_null());
            assert!(!dfgo_error_kind(error).is_null());
            assert!(!dfgo_error_message(error).is_null());
            dfgo_error_free(error);
        }
        dfgo_connection_close(conn);
        dfgo_database_close(db);
    }
});
