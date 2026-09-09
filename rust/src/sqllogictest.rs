//! Test-only adapter: upstream fixtures and assertions, SQL execution in Go.
//!
//! The callback returns an Arrow C stream of batches consumed by Go. This
//! keeps upstream's type/value normalization independent of the driver and
//! avoids a second implementation of the SQLLogicTest language.

use std::ffi::{CString, c_char, c_void};
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion_sqllogictest::{
    DFColumnType, TestContext, convert_batches, convert_schema_to_types, df_value_validator,
    setup_scratch_dir, value_normalizer,
};
use serde_json::json;
use sqllogictest::{
    Condition, Connection, DB, DBOutput, QueryExpect, Record, Runner, StatementExpect,
    strict_column_validator,
};

mod coverage;

use crate::abi::{cstr_to_string, dfgo_connection, dfgo_error};
use crate::error::{FfiError, run_ffi};
use crate::session::Inner;

type QueryCallback =
    unsafe extern "C" fn(usize, *const c_char, usize, *mut *mut u8, *mut usize) -> i32;
type FreeCallback = unsafe extern "C" fn(*mut c_void);

struct GoDatabase {
    handle: usize,
    query: QueryCallback,
    free: FreeCallback,
    spark: bool,
    workspace: String,
    failures: Arc<Mutex<Vec<String>>>,
}

impl GoDatabase {
    fn adapter_error(&self, error: impl std::fmt::Display) -> std::io::Error {
        let message = format!("SQLLogicTest adapter failed: {error}");
        self.failures.lock().unwrap().push(message.clone());
        std::io::Error::other(message)
    }
}

impl DB for GoDatabase {
    type Error = std::io::Error;
    type ColumnType = DFColumnType;

    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    fn run(&mut self, sql: &str) -> Result<DBOutput<DFColumnType>, Self::Error> {
        let mut data = ptr::null_mut();
        let mut len = 0;
        // SAFETY: Go retains the callback handle throughout this synchronous
        // run. The callback copies SQL and returns its own allocation, which
        // is released with the matching callback immediately after copying.
        let status = unsafe {
            (self.query)(
                self.handle,
                sql.as_ptr().cast(),
                sql.len(),
                &mut data,
                &mut len,
            )
        };
        if data.is_null() {
            return Err(self.adapter_error("Go SQL callback returned no data"));
        }
        if status != 0 {
            let bytes = unsafe { std::slice::from_raw_parts(data, len).to_vec() };
            unsafe { (self.free)(data.cast()) };
            if status != 1 {
                return Err(self.adapter_error(String::from_utf8_lossy(&bytes)));
            }
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }
        if len != std::mem::size_of::<FFI_ArrowArrayStream>() {
            unsafe { (self.free)(data.cast()) };
            return Err(self.adapter_error("invalid Arrow C stream size"));
        }
        // Move the exported stream out of C-allocated storage. The reader owns
        // its callbacks and releases Go's retained batches on every exit path.
        let stream = unsafe { data.cast::<FFI_ArrowArrayStream>().read() };
        unsafe { (self.free)(data.cast()) };
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| self.adapter_error(e))?;
        let schema = reader.schema();
        let types = convert_schema_to_types(schema.fields());
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| self.adapter_error(e))?;
        let mut rows =
            convert_batches(&schema, batches, self.spark).map_err(|e| self.adapter_error(e))?;
        // The published upstream crate embeds its build directory. Our data
        // lives in a separately pinned checkout, so normalize that root too.
        for row in &mut rows {
            for cell in row {
                *cell = cell.replace(&self.workspace, "WORKSPACE_ROOT");
            }
        }
        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }
}

/// Install the upstream fixture context before preparing any statements.
///
/// # Safety
/// All pointers must be live. The connection must be exclusively borrowed and
/// have no statements or readers. `out` receives a context owned by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_test_slt_setup(
    connection: *mut dfgo_connection,
    path: *const c_char,
    out: *mut *mut c_void,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if connection.is_null() || out.is_null() {
            return Err(FfiError::invalid_argument(
                "missing SQLLogicTest setup pointer",
            ));
        }
        let path = cstr_to_string(path, "test path")?;
        let path = Path::new(&path);
        setup_scratch_dir(path).map_err(|e| FfiError::native(e.to_string()))?;
        let connection = unsafe { &mut *connection };
        let context = connection
            .inner
            .runtime
            .block_on(TestContext::try_new_for_test_file(path))
            .ok_or_else(|| {
                FfiError::native("upstream fixture unavailable; skipping is forbidden")
            })?;
        connection.inner = Arc::new(Inner {
            runtime: Arc::clone(&connection.inner.runtime),
            ctx: context.session_ctx().clone(),
        });
        unsafe { *out = Box::into_raw(Box::new(context)).cast() };
        Ok(())
    })
}

/// Release fixture resources after closing its Go connection.
///
/// # Safety
/// `context` must be null or a live, uniquely owned setup result.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_test_slt_context_free(context: *mut c_void) {
    if !context.is_null() {
        drop(unsafe { Box::from_raw(context.cast::<TestContext>()) });
    }
}

/// Execute every record with upstream parsing and comparison rules.
///
/// # Safety
/// Strings, callbacks and handle must remain valid throughout the call. `out`
/// receives a JSON report allocated by Rust, released with dfgo_test_slt_free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_test_slt_run(
    path: *const c_char,
    workspace: *const c_char,
    handle: usize,
    query: QueryCallback,
    free: FreeCallback,
    out: *mut *mut c_char,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "missing SQLLogicTest report pointer",
            ));
        }
        let path = cstr_to_string(path, "test path")?;
        let workspace = cstr_to_string(workspace, "workspace")?;
        let workspace = Path::new(&workspace)
            .canonicalize()
            .map_err(|e| FfiError::native(e.to_string()))?
            .to_string_lossy()
            .trim_start_matches(r"\\?\")
            .replace('\\', "/")
            .trim_start_matches('/')
            .to_owned();
        let records = sqllogictest::parse_file::<DFColumnType>(&path)
            .map_err(|e| FfiError::native(e.to_string()))?;
        let spark = path.replace('\\', "/").contains("/spark/");
        let failures = Arc::new(Mutex::new(Vec::new()));
        let mut runner = Runner::new(|| async {
            Ok(GoDatabase {
                handle,
                query,
                free,
                spark,
                workspace: workspace.clone(),
                failures: Arc::clone(&failures),
            })
        });
        runner.with_column_validator(strict_column_validator);
        runner.with_normalizer(value_normalizer);
        runner.with_validator(df_value_validator);
        let mut statements = 0;
        let mut queries = 0;
        let mut errors = Vec::new();
        let mut eligible = 0;
        let mut passed = 0;
        let mut skipped = Vec::new();
        let mut evidence = Vec::new();
        // Do not enter a Tokio runtime here: callbacks synchronously re-enter
        // the native bridge, which owns the runtime used for SQL execution.
        futures::executor::block_on(async {
            for record in records {
                let mut is_sql = false;
                let mut witness = None;
                match &record {
                    Record::Statement { .. } => statements += 1,
                    Record::Query { .. } => queries += 1,
                    Record::Halt { .. } => {
                        errors.push("halt would silently omit tests".to_owned());
                        continue;
                    }
                    Record::Comment(_)
                    | Record::Newline
                    | Record::HashThreshold { .. }
                    | Record::Include { .. }
                    | Record::Condition(_)
                    | Record::Injected(_) => {}
                    other => {
                        errors.push(format!("unsupported SQLLogicTest directive: {other:?}"));
                        continue;
                    }
                }
                if let Record::Statement {
                    loc,
                    conditions,
                    connection,
                    retry,
                    sql,
                    ..
                }
                | Record::Query {
                    loc,
                    conditions,
                    connection,
                    retry,
                    sql,
                    ..
                } = &record
                {
                    if connection != &Connection::Default || retry.is_some() {
                        errors.push(format!(
                            "unsupported connection or retry directive at {loc}"
                        ));
                        continue;
                    }
                    if conditions.iter().any(|condition| match condition {
                        Condition::OnlyIf { label } | Condition::SkipIf { label } => {
                            label != "DataFusion" && label != "postgres"
                        }
                    }) {
                        errors.push(format!(
                            "unrecognized engine condition at {loc}: {conditions:?}"
                        ));
                        continue;
                    }
                    let skip = conditions.iter().any(|condition| match condition {
                        Condition::OnlyIf { label } => label != "DataFusion",
                        Condition::SkipIf { label } => label == "DataFusion",
                    });
                    if skip {
                        skipped.push(json!({"location": loc.to_string(), "conditions": format!("{conditions:?}")}));
                    } else {
                        is_sql = true;
                        eligible += 1;
                    }
                    let expects_error = matches!(
                        &record,
                        Record::Statement {
                            expected: StatementExpect::Error(_),
                            ..
                        } | Record::Query {
                            expected: QueryExpect::Error(_),
                            ..
                        }
                    );
                    let (functions, parse_error) = coverage::functions(sql);
                    witness = Some(json!({"location": loc.to_string(), "sql": sql,
                        "kind": if matches!(&record, Record::Query { .. }) { "query" } else { "statement" },
                        "expects_error": expects_error, "skipped": skip, "passed": false,
                        "functions": functions, "parse_error": parse_error}));
                }
                let failures_before = failures.lock().unwrap().len();
                if let Err(error) = runner.run_async(record).await {
                    errors.push(error.to_string());
                } else if is_sql && failures.lock().unwrap().len() == failures_before {
                    passed += 1;
                    witness.as_mut().unwrap()["passed"] = json!(true);
                }
                if let Some(witness) = witness {
                    evidence.push(witness);
                }
            }
            runner.shutdown_async().await;
        });
        errors.extend(failures.lock().unwrap().iter().cloned());
        let report = json!({"statements": statements, "queries": queries, "eligible": eligible,
            "passed": passed, "skipped": skipped, "errors": errors, "records": evidence});
        let report =
            CString::new(report.to_string()).map_err(|e| FfiError::native(e.to_string()))?;
        unsafe { *out = report.into_raw() };
        Ok(())
    })
}

/// Release a report returned by dfgo_test_slt_run.
///
/// # Safety
/// `report` must be null or an unfreed report returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_test_slt_free(report: *mut c_char) {
    if !report.is_null() {
        drop(unsafe { CString::from_raw(report) });
    }
}
