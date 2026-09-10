use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Arc;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use super::*;
use crate::error::*;

#[test]
fn c_and_rust_abi_layouts_match() {
    use std::path::Path;
    use std::process::Command;

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = std::env::temp_dir().join(format!("dfgo-abi-layout-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let exe = dir.join(if cfg!(windows) {
        "layout.exe"
    } else {
        "layout"
    });
    let compiler = std::env::var("CC").unwrap_or_else(|_| "cc".to_owned());
    let output = Command::new(&compiler)
        .arg("-std=c11")
        .arg("-Wall")
        .arg("-Wextra")
        .arg("-Werror")
        .arg("-I")
        .arg(root.join("include"))
        .arg(root.join("tests/abi_layout.c"))
        .arg("-o")
        .arg(&exe)
        .output()
        .expect("C compiler is required to verify the ABI layout");
    assert!(
        output.status.success(),
        "C ABI compilation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new(&exe).output().unwrap();
    assert!(output.status.success());
    let c_layout: Vec<usize> = String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(
        c_layout,
        super::contract_generated::rust_layout(),
        "C and Rust ABI layouts differ"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn native_handles_release_sessions_and_runtimes() {
    for export in [false, true] {
        for _ in 0..8 {
            // Exercise ownership transfers through the same C ABI used by
            // Go. Weak references observe real native object destruction.
            unsafe {
                let mut db = ptr::null_mut();
                let mut conn = ptr::null_mut();
                let mut stmt = ptr::null_mut();
                let mut token = ptr::null_mut();
                let mut result = ptr::null_mut();
                let mut err = ptr::null_mut();
                assert_eq!(dfgo_database_open(ptr::null(), &mut db, &mut err), DFG_OK);
                let runtime = Arc::downgrade(&(*db).runtime);
                assert_eq!(dfgo_connection_open_shared(db, &mut conn, &mut err), DFG_OK);
                let session = Arc::downgrade(&(*conn).inner);
                assert_eq!(
                    dfgo_prepare(conn, c"select 1".as_ptr(), &mut stmt, &mut err),
                    DFG_OK
                );
                assert_eq!(dfgo_cancel_token_create(&mut token, &mut err), DFG_OK);
                assert_eq!(
                    dfgo_statement_execute_with_params(
                        stmt,
                        ptr::null(),
                        0,
                        token,
                        &mut result,
                        &mut err
                    ),
                    DFG_OK
                );
                let mut stream = FFI_ArrowArrayStream::empty();
                if export {
                    assert_eq!(
                        dfgo_result_export_arrow_stream(result, &mut stream, &mut err),
                        DFG_OK
                    );
                }
                dfgo_statement_close(stmt);
                dfgo_connection_close(conn);
                dfgo_database_close(db);
                assert!(
                    session.upgrade().is_some(),
                    "result must keep its session alive"
                );
                dfgo_result_close(result);
                dfgo_cancel_token_close(token);
                if export {
                    assert!(
                        session.upgrade().is_some(),
                        "exported stream owns the session"
                    );
                }
                drop(stream);
                assert!(
                    session.upgrade().is_none(),
                    "session leaked after result release"
                );
                assert!(
                    runtime.upgrade().is_none(),
                    "runtime leaked after all handles closed"
                );
            }
        }
    }
}

#[test]
fn canceled_execution_releases_native_resources() {
    unsafe {
        let mut db = ptr::null_mut();
        let mut conn = ptr::null_mut();
        let mut stmt = ptr::null_mut();
        let mut token = ptr::null_mut();
        let mut result = ptr::null_mut();
        let mut err = ptr::null_mut();
        assert_eq!(dfgo_database_open(ptr::null(), &mut db, &mut err), DFG_OK);
        let runtime = Arc::downgrade(&(*db).runtime);
        assert_eq!(
            dfgo_connection_open_isolated(db, &mut conn, &mut err),
            DFG_OK
        );
        let session = Arc::downgrade(&(*conn).inner);
        assert_eq!(
            dfgo_prepare(
                conn,
                c"create view canceled as select 1".as_ptr(),
                &mut stmt,
                &mut err
            ),
            DFG_OK
        );
        assert_eq!(dfgo_cancel_token_create(&mut token, &mut err), DFG_OK);
        dfgo_cancel_token_cancel(token);
        assert_eq!(
            dfgo_statement_execute_with_params(stmt, ptr::null(), 0, token, &mut result, &mut err),
            DFG_ERR
        );
        assert!(result.is_null());
        assert_eq!(CStr::from_ptr(dfgo_error_kind(err)), c"cancelled");
        let connection = &*conn;
        assert!(!connection.inner.ctx.table_exist("canceled").unwrap());
        dfgo_error_free(err);
        dfgo_cancel_token_close(token);
        dfgo_statement_close(stmt);
        dfgo_connection_close(conn);
        dfgo_database_close(db);
        assert!(session.upgrade().is_none());
        assert!(runtime.upgrade().is_none());
    }
}

#[test]
fn registers_and_queries_ffi_table_provider() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::execution::TaskContextProvider;
    use datafusion_ffi::execution::FFI_TaskContextProvider;
    use datafusion_ffi::table_provider::FFI_TableProvider;

    // A one-column, three-row MemTable exported as an FFI_TableProvider.
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "a",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("record batch");
    let mem: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable"));
    let provider_lifetime = Arc::downgrade(&mem);

    // FFI_TaskContextProvider holds a Weak, so this SessionContext must
    // outlive the exported provider for the duration of the test.
    let tcp_ctx = Arc::new(SessionContext::new());
    let tcp = Arc::clone(&tcp_ctx) as Arc<dyn TaskContextProvider>;
    let ffi = FFI_TableProvider::new(mem, true, None, FFI_TaskContextProvider::from(&tcp), None);

    // A minimal connection handle to register into.
    let runtime = Arc::new(Runtime::new().expect("runtime"));
    let mut conn = dfgo_connection {
        inner: Arc::new(Inner {
            runtime: Arc::clone(&runtime),
            ctx: SessionContext::new(),
        }),
    };

    // Exercise the exported C entry point exactly as Go calls it.
    let name = CString::new("t").unwrap();
    let version = CString::new(datafusion_version_str()).unwrap();
    let mut err: *mut dfgo_error = ptr::null_mut();
    let rc = unsafe {
        dfgo_connection_register_ffi_table_provider(
            &mut conn,
            name.as_ptr(),
            &ffi as *const FFI_TableProvider as *const std::ffi::c_void,
            version.as_ptr(),
            &mut err,
        )
    };
    assert_eq!(rc, DFG_OK, "register returned an error");
    assert!(err.is_null(), "register set an error");
    drop(ffi);
    assert!(
        provider_lifetime.upgrade().is_some(),
        "registration owns a provider clone"
    );

    // The registered provider is queryable, and a predicate filters rows.
    let rows: usize = runtime
        .block_on(async {
            conn.inner
                .ctx
                .sql("SELECT a FROM t WHERE a > 1 ORDER BY a")
                .await?
                .collect()
                .await
        })
        .expect("query")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 2, "expected rows a=2,3");

    let pending = runtime
        .block_on(conn.inner.ctx.sql("SELECT a FROM t"))
        .unwrap();

    // Deregistering removes the table, so planning against it now fails.
    let mut derr: *mut dfgo_error = ptr::null_mut();
    let drc = unsafe { dfgo_connection_deregister_table(&mut conn, name.as_ptr(), &mut derr) };
    assert_eq!(drc, DFG_OK, "deregister returned an error");
    assert!(derr.is_null(), "deregister set an error");
    let after = runtime.block_on(async { conn.inner.ctx.sql("SELECT a FROM t").await });
    assert!(after.is_err(), "table should be gone after deregister");
    assert!(
        provider_lifetime.upgrade().is_some(),
        "a prepared plan outlives deregistration"
    );
    let batches = runtime.block_on(pending.collect()).unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    drop(batches);
    assert!(
        provider_lifetime.upgrade().is_none(),
        "last plan releases the foreign provider"
    );
}

#[test]
fn rejects_version_mismatch_before_dereference() {
    let mut conn = dfgo_connection {
        inner: Arc::new(Inner {
            runtime: Arc::new(Runtime::new().expect("runtime")),
            ctx: SessionContext::new(),
        }),
    };
    let name = CString::new("t").unwrap();
    let version = CString::new("0.0.0-not-a-real-version").unwrap();
    // Not a real FFI_TableProvider; must never be dereferenced.
    let bogus: [u8; 64] = [0; 64];
    let mut err: *mut dfgo_error = ptr::null_mut();
    let rc = unsafe {
        dfgo_connection_register_ffi_table_provider(
            &mut conn,
            name.as_ptr(),
            bogus.as_ptr() as *const std::ffi::c_void,
            version.as_ptr(),
            &mut err,
        )
    };
    assert_eq!(rc, DFG_ERR);
    assert!(!err.is_null());
    unsafe { dfgo_error_free(err) };
}

#[test]
fn rejects_null_ffi_table_provider() {
    let mut conn = dfgo_connection {
        inner: Arc::new(Inner {
            runtime: Arc::new(Runtime::new().expect("runtime")),
            ctx: SessionContext::new(),
        }),
    };
    let name = CString::new("t").unwrap();
    let version = CString::new(datafusion_version_str()).unwrap();
    let mut err: *mut dfgo_error = ptr::null_mut();
    let rc = unsafe {
        dfgo_connection_register_ffi_table_provider(
            &mut conn,
            name.as_ptr(),
            ptr::null(),
            version.as_ptr(),
            &mut err,
        )
    };
    assert_eq!(rc, DFG_ERR);
    assert!(!err.is_null());
    unsafe { dfgo_error_free(err) };
}
