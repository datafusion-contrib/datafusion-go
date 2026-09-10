//! C entry points and opaque handles. Own all pointer transfers at this seam.

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;
use std::slice;
use std::sync::Arc;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_sql::parser::DFParserBuilder;
use datafusion_sql::sqlparser::dialect::dialect_from_str;
use tokio::runtime::Runtime;

use crate::error::{FfiError, run_ffi};
use crate::generated::{DATAFUSION_VERSION, DFGO_ABI_VERSION};
use crate::parameters::bindings_from_params;
use crate::query::{
    Binding, ParameterMetadata, PreparedQuery, prepare_query, statement_defines_parameters,
    statement_serializes,
};
use crate::registration::{arrow_stream_batches, ipc_batches, register_record_batches};
use crate::session::{Inner, session_config_from_dsn};
use crate::stream::{CancelToken, execute_to_stream};

// These names intentionally match the C header typedefs. They are opaque to Go
// and C callers; only Rust ever inspects their fields.
#[allow(non_camel_case_types)]
mod ffi_types {
    use super::{
        Arc, CString, CancelToken, FFI_ArrowArrayStream, Inner, ParameterMetadata, Runtime,
        SessionConfig, SessionContext, c_char,
    };

    // Database lifetime owns the Tokio runtime and the shared-session context.
    // Connections clone these values, so closing the database only releases the
    // top-level handle after existing connections have taken their own Arcs.
    pub struct dfgo_database {
        pub(crate) runtime: Arc<Runtime>,
        pub(crate) config: SessionConfig,
        pub(crate) shared_ctx: SessionContext,
    }

    // A connection owns an execution context. It may be isolated or a clone of
    // the database-level shared context depending on the DSN/session setting.
    pub struct dfgo_connection {
        pub(crate) inner: Arc<Inner>,
    }

    // Prepared statements keep normalized SQL plus parser-derived parameter
    // metadata. Parameter values are supplied per execution call.
    pub struct dfgo_statement {
        pub(crate) inner: Arc<Inner>,
        pub(crate) query: String,
        pub(crate) params: ParameterMetadata,
        pub(crate) serializes: bool,
    }

    // A result can be exported exactly once as an Arrow C stream. The Option is
    // the single-export guard; the cancellation token remains available for
    // explicit cancellation and close.
    pub struct dfgo_result_stream {
        pub(crate) stream: Option<FFI_ArrowArrayStream>,
        pub(crate) cancel: Arc<CancelToken>,
    }

    // Separate cancel tokens let Go cancel a query before the Arrow stream has
    // been constructed, while the result stream can keep sharing the same token.
    pub struct dfgo_cancel_token {
        pub(crate) cancel: Arc<CancelToken>,
    }

    // Errors are heap-allocated so C/Go can read stable pointers until
    // dfgo_error_free transfers ownership back to Rust and drops the value.
    pub struct dfgo_error {
        pub(crate) kind: CString,
        pub(crate) message: CString,
    }

    // Parameter array element for dfgo_statement_execute_with_params. All
    // pointer fields are borrowed only during that one FFI call.
    #[repr(C)]
    pub struct dfgo_parameter {
        pub(crate) index: i64,
        pub(crate) name: *const c_char,
        pub(crate) name_len: i64,
        pub(crate) type_code: i32,
        pub(crate) is_null: i32,
        pub(crate) int64_value: i64,
        pub(crate) uint64_value: u64,
        pub(crate) float64_value: f64,
        pub(crate) data: *const u8,
        pub(crate) data_len: i64,
        pub(crate) timezone: *const c_char,
        pub(crate) timezone_len: i64,
        pub(crate) precision: u8,
        pub(crate) scale: i8,
    }
}

pub use ffi_types::*;

pub(crate) fn cstr_to_string(ptr: *const c_char, name: &str) -> Result<String, FfiError> {
    if ptr.is_null() {
        return Err(FfiError::invalid_argument(format!("{name} is null")));
    }

    // SAFETY: `ptr` is non-null and the ABI requires it to point at a valid
    // NUL-terminated C string for the duration of this call.
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map(|s| s.to_owned())
        .map_err(|e| FfiError::invalid_argument(format!("{name} is not valid UTF-8: {e}")))
}

// Borrow a caller-owned byte region for the duration of one FFI call. The slice
// must never be stored past the call boundary; helpers that need longer
// lifetimes copy or import the data immediately.
pub(crate) fn bytes_from_ptr<'a>(
    ptr: *const u8,
    len: i64,
    name: &str,
) -> Result<&'a [u8], FfiError> {
    if ptr.is_null() && len != 0 {
        return Err(FfiError::invalid_argument(format!(
            "{name} pointer is null"
        )));
    }
    if len < 0 {
        return Err(FfiError::invalid_argument(format!(
            "{name} length must be non-negative, got {len}"
        )));
    }

    if len == 0 {
        Ok(&[])
    } else {
        // SAFETY: non-zero lengths require a non-null pointer, negative lengths
        // were rejected, and the converted usize length came from the checked
        // i64 value. The caller owns the allocation and must keep it alive for
        // this FFI call.
        unsafe {
            Ok(slice::from_raw_parts(
                ptr,
                usize::try_from(len).map_err(|e| FfiError::invalid_argument(e.to_string()))?,
            ))
        }
    }
}

// Convert a pointer/length UTF-8 region into an owned Rust String. Unlike
// cstr_to_string, this accepts embedded NUL bytes because the length is explicit.
pub(crate) fn bytes_to_string(
    ptr: *const c_char,
    len: i64,
    name: &str,
) -> Result<String, FfiError> {
    if ptr.is_null() && len != 0 {
        return Err(FfiError::invalid_argument(format!(
            "{name} pointer is null"
        )));
    }
    if len < 0 {
        return Err(FfiError::invalid_argument(format!(
            "{name} length must be non-negative, got {len}"
        )));
    }

    let bytes = if len == 0 {
        &[]
    } else {
        // SAFETY: same invariant as bytes_from_ptr: non-null for non-empty
        // input, non-negative checked length, and borrow scoped to this call.
        unsafe {
            slice::from_raw_parts(
                ptr.cast::<u8>(),
                usize::try_from(len).map_err(|e| FfiError::invalid_argument(e.to_string()))?,
            )
        }
    };
    std::str::from_utf8(bytes)
        .map(|s| s.to_owned())
        .map_err(|e| FfiError::invalid_argument(format!("{name} is not valid UTF-8: {e}")))
}

// --- Public C ABI ---------------------------------------------------------
//
// Every fallible exported function writes DFG_OK/DFG_ERR and uses `err` for
// details. Every handle returned through an `out` parameter is Rust-owned and
// must come back through the matching close/free function exactly once.

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_abi_version() -> i32 {
    DFGO_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_datafusion_version() -> *const c_char {
    // Static NUL-terminated bytes are safe to expose for the process lifetime.
    DATAFUSION_VERSION.as_ptr().cast()
}

/// The datafusion version this crate links, as a `&str`, for the FFI provider
/// version handshake. Derived from the generated `DATAFUSION_VERSION` bytes.
pub(crate) fn datafusion_version_str() -> &'static str {
    CStr::from_bytes_with_nul(DATAFUSION_VERSION)
        .expect("generated DATAFUSION_VERSION is NUL-terminated")
        .to_str()
        .expect("generated DATAFUSION_VERSION is valid UTF-8")
}

/// # Safety
///
/// `dsn`, when non-null, must point to a valid NUL-terminated C string for the
/// duration of this call. `out` must point to writable storage for one database
/// handle. `err`, when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_database_open(
    dsn: *const c_char,
    out: *mut *mut dfgo_database,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "database output pointer is null",
            ));
        }

        let dsn = if dsn.is_null() {
            // Treat a null DSN the same as an empty DSN so C callers do not need
            // to allocate an empty string for the common default configuration.
            String::new()
        } else {
            cstr_to_string(dsn, "dsn")?
        };

        // One runtime per database keeps async execution isolated between
        // database handles while allowing all connections from one database to
        // share worker threads.
        let runtime = Runtime::new().map_err(|e| FfiError::native(e.to_string()))?;
        let config = session_config_from_dsn(&dsn)?;
        let shared_ctx = SessionContext::new_with_config(config.clone());
        let db = dfgo_database {
            runtime: Arc::new(runtime),
            config,
            shared_ctx,
        };

        // SAFETY: `out` was checked for null. The Box allocation is transferred
        // to the caller and must be returned via dfgo_database_close.
        unsafe {
            *out = Box::into_raw(Box::new(db));
        }
        Ok(())
    })
}

/// # Safety
///
/// `db` must be null or a live database handle returned by
/// `dfgo_database_open` that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_database_close(db: *mut dfgo_database) {
    if !db.is_null() {
        // SAFETY: the ABI requires `db` to be a handle previously returned from
        // dfgo_database_open and not already closed. Box::from_raw retakes Rust
        // ownership so the value is dropped exactly once.
        unsafe {
            drop(Box::from_raw(db));
        }
    }
}

/// # Safety
///
/// `db` must be a live database handle. `out` must point to writable storage for
/// one connection handle. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_open_isolated(
    db: *mut dfgo_database,
    out: *mut *mut dfgo_connection,
    err: *mut *mut dfgo_error,
) -> i32 {
    open_connection(db, out, err, false)
}

/// # Safety
///
/// `db` must be a live database handle. `out` must point to writable storage for
/// one connection handle. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_open_shared(
    db: *mut dfgo_database,
    out: *mut *mut dfgo_connection,
    err: *mut *mut dfgo_error,
) -> i32 {
    open_connection(db, out, err, true)
}

pub(crate) fn open_connection(
    db: *mut dfgo_database,
    out: *mut *mut dfgo_connection,
    err: *mut *mut dfgo_error,
    shared: bool,
) -> i32 {
    run_ffi(err, || {
        if db.is_null() {
            return Err(FfiError::invalid_argument("database handle is null"));
        }
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "connection output pointer is null",
            ));
        }

        // SAFETY: `db` is non-null and must be a live database handle owned by
        // the caller. This function only borrows it long enough to clone Arcs.
        let db = unsafe { &*db };
        let ctx = if shared {
            // Shared connections see tables registered by other shared
            // connections from the same database handle.
            db.shared_ctx.clone()
        } else {
            // Isolated connections inherit the same configuration but get their
            // own catalog/session state.
            SessionContext::new_with_config(db.config.clone())
        };
        let conn = dfgo_connection {
            inner: Arc::new(Inner {
                runtime: db.runtime.clone(),
                ctx,
            }),
        };

        // SAFETY: `out` is non-null. Ownership of this Box moves to the caller
        // until dfgo_connection_close is called.
        unsafe {
            *out = Box::into_raw(Box::new(conn));
        }
        Ok(())
    })
}

/// # Safety
///
/// `conn` must be null or a live connection handle returned by
/// `dfgo_connection_open_isolated` or `dfgo_connection_open_shared` that has not
/// already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_close(conn: *mut dfgo_connection) {
    if !conn.is_null() {
        // SAFETY: `conn` must be a live handle returned from open_connection and
        // not previously closed. Dropping it releases its Arc references.
        unsafe {
            drop(Box::from_raw(conn));
        }
    }
}

/// # Safety
///
/// `conn` must be a live connection handle. `name` must point to a valid
/// NUL-terminated C string. `data` must be null when `len` is zero, or point to
/// `len` readable bytes for the duration of this call. `err`, when non-null,
/// must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_register_arrow_ipc(
    conn: *mut dfgo_connection,
    name: *const c_char,
    data: *const u8,
    len: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if conn.is_null() {
            return Err(FfiError::invalid_argument("connection handle is null"));
        }

        let name = cstr_to_string(name, "table name")?;
        let data = bytes_from_ptr(data, len, "arrow ipc stream")?;
        let (schema, batches) = ipc_batches(data)?;
        // SAFETY: `conn` was checked for null and is only borrowed for
        // registration; Rust does not take ownership of the handle here.
        let conn = unsafe { &*conn };
        register_record_batches(&conn.inner, &name, schema, batches)
    })
}

/// # Safety
///
/// `stream` must point to a valid Arrow C stream. A non-null stream is consumed
/// by this function even when registration fails. `conn` must be a live
/// connection handle, `name` must point to a valid NUL-terminated C string, and
/// `err`, when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_register_arrow_stream(
    conn: *mut dfgo_connection,
    name: *const c_char,
    stream: *mut FFI_ArrowArrayStream,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        // Import the stream before validating other arguments so this function
        // has simple ownership semantics: a non-null stream is consumed even
        // when registration fails later.
        let (schema, batches) = arrow_stream_batches(stream)?;

        if conn.is_null() {
            return Err(FfiError::invalid_argument("connection handle is null"));
        }

        let name = cstr_to_string(name, "table name")?;
        // SAFETY: `conn` is non-null and is borrowed for the duration of table
        // registration only.
        let conn = unsafe { &*conn };
        register_record_batches(&conn.inner, &name, schema, batches)
    })
}

/// Register a foreign `datafusion-ffi` `FFI_TableProvider` produced by another
/// library. `provider` points to an `FFI_TableProvider` (passed as an opaque
/// `const void*` so the C header need not know datafusion-ffi's layout). The
/// provider is imported via datafusion-ffi's `From<&FFI_TableProvider>`, which
/// clones it — the caller retains ownership of `provider` and must free it.
///
/// The clone only bumps the foreign provider's refcount; the registered table
/// invokes the provider's function pointers on every scan. The producing
/// library therefore must stay loaded and un-freed for as long as the table
/// remains registered on `conn` and until all dependent views, query plans,
/// streams, and returned batches are released. Deregistration does not release
/// those outstanding references. Its `scan`/`clone`/`release` function
/// pointers dangle if it is unloaded, which is undefined behavior. The session
/// backing the provider's (weakly held) task context should also outlive the
/// registration; if it does not, queries against the table fail with a clean
/// "TaskContextProvider went out of scope" error rather than crashing.
///
/// `provider_datafusion_version` is the datafusion version the producing
/// library reports building against. It is compared against this crate's
/// version *before* `provider` is dereferenced: `FFI_TableProvider` is a plain
/// `repr(C)` vtable with no stable prefix, so its layout (and its own `version`
/// function pointer) can only be trusted once the versions are known to match.
/// A mismatch is rejected as an error instead of risking undefined behavior.
/// This is a cooperative check and cannot detect a mislabeled provider.
///
/// # Safety
///
/// `conn` must be a live connection handle. `name` and
/// `provider_datafusion_version` must be valid NUL-terminated C strings.
/// `provider` must be a non-null pointer to a valid `FFI_TableProvider` built
/// against the reported datafusion version. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_register_ffi_table_provider(
    conn: *mut dfgo_connection,
    name: *const c_char,
    provider: *const c_void,
    provider_datafusion_version: *const c_char,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if conn.is_null() {
            return Err(FfiError::invalid_argument("connection handle is null"));
        }
        if provider.is_null() {
            return Err(FfiError::invalid_argument("provider handle is null"));
        }
        let name = cstr_to_string(name, "table name")?;
        if name.trim().is_empty() {
            return Err(FfiError::invalid_argument("table name is empty"));
        }
        let provider_version =
            cstr_to_string(provider_datafusion_version, "provider datafusion version")?;

        // Out-of-band version handshake, checked BEFORE the provider is
        // dereferenced. Because `FFI_TableProvider` has no stable prefix, reading
        // anything out of it — including its own `version` fn-pointer — is only
        // sound once we know the layouts match. A mismatch becomes a clean error
        // instead of undefined behavior.
        //
        // This requires an exact datafusion version match, which is deliberately
        // stricter than datafusion-ffi's own contract (it promises ABI stability
        // at the major version only; see `datafusion_ffi::version`). datafusion
        // is pre-1.0 and has broken layouts across minor/patch releases, so we
        // refuse anything but an exact match rather than trust a looser bound.
        let expected = datafusion_version_str();
        if provider_version != expected {
            return Err(FfiError::invalid_argument(format!(
                "incompatible FFI table provider: datafusion-go links datafusion {expected}, but the provider reports {provider_version}; both sides must link the same datafusion version"
            )));
        }

        // SAFETY: `conn` is non-null and borrowed only for the duration of
        // registration.
        let conn = unsafe { &*conn };
        // SAFETY: `provider` is non-null, the version handshake above confirmed a
        // matching datafusion build, and per the contract it points to a valid
        // `FFI_TableProvider`. We only borrow it; `From` clones it.
        let ffi_provider = unsafe { &*(provider as *const FFI_TableProvider) };
        let table: Arc<dyn TableProvider> = ffi_provider.into();
        conn.inner.ctx.register_table(&name, table)?;
        Ok(())
    })
}

/// Deregister a table previously registered on `conn` (for example one added by
/// [`dfgo_connection_register_ffi_table_provider`]). Deregistering a name that
/// is not currently registered is not an error.
///
/// # Safety
///
/// `conn` must be a live connection handle. `name` must be a valid
/// NUL-terminated C string. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_connection_deregister_table(
    conn: *mut dfgo_connection,
    name: *const c_char,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if conn.is_null() {
            return Err(FfiError::invalid_argument("connection handle is null"));
        }
        let name = cstr_to_string(name, "table name")?;
        // SAFETY: `conn` is non-null and borrowed only for the duration of the
        // deregistration.
        let conn = unsafe { &*conn };
        conn.inner.ctx.deregister_table(&name)?;
        Ok(())
    })
}

/// # Safety
///
/// `conn` must be a live connection handle. `query` must point to a valid
/// NUL-terminated C string. `out` must point to writable storage for one
/// statement handle. `err`, when non-null, must point to writable error-handle
/// storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_prepare(
    conn: *mut dfgo_connection,
    query: *const c_char,
    out: *mut *mut dfgo_statement,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if conn.is_null() {
            return Err(FfiError::invalid_argument("connection handle is null"));
        }
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "statement output pointer is null",
            ));
        }

        // Prepare does all driver-level validation that can be decided from SQL
        // text alone: placeholder style, single-statement enforcement, and
        // whether database/sql must serialize statement execution.
        // SAFETY: the connection is live for this call. Snapshot its parser
        // settings so prepare honors the same dialect and recursion limit as
        // execution, including settings changed by a preceding SET statement.
        let conn = unsafe { &*conn };
        let state = conn.inner.ctx.state();
        let options = &state.config_options().sql_parser;
        let dialect = dialect_from_str(options.dialect).ok_or_else(|| {
            FfiError::invalid_argument(format!("unsupported SQL dialect: {}", options.dialect))
        })?;
        let query = cstr_to_string(query, "query")?;
        let statements = DFParserBuilder::new(query.as_str())
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(options.recursion_limit.get())
            .build()
            .and_then(|mut parser| parser.parse_statements())
            .map_err(|e| FfiError::invalid_argument(e.to_string()))?;
        let serializes = match statements.len() {
            0 => {
                return Err(FfiError::invalid_argument(
                    "query does not contain a SQL statement",
                ));
            }
            1 => statement_serializes(&statements[0]),
            count => {
                return Err(FfiError::invalid_argument(format!(
                    "query contains {count} SQL statements; exactly one statement is supported"
                )));
            }
        };
        let prepared = if statement_defines_parameters(&statements[0]) {
            PreparedQuery {
                query,
                params: ParameterMetadata::None,
            }
        } else {
            prepare_query(query, dialect.as_ref())?
        };
        // SAFETY: `conn` is non-null and live; the prepared statement clones the
        // Arc it needs, so it can outlive the borrowed connection reference.
        let stmt = dfgo_statement {
            inner: conn.inner.clone(),
            query: prepared.query,
            params: prepared.params,
            serializes,
        };

        // SAFETY: `out` is non-null. Ownership of the statement handle moves to
        // the caller until dfgo_statement_close.
        unsafe {
            *out = Box::into_raw(Box::new(stmt));
        }
        Ok(())
    })
}

/// # Safety
///
/// `stmt` must be null or a live statement handle returned by `dfgo_prepare`
/// that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_close(stmt: *mut dfgo_statement) {
    if !stmt.is_null() {
        // SAFETY: `stmt` must be a live handle returned from dfgo_prepare and
        // not already closed.
        unsafe {
            drop(Box::from_raw(stmt));
        }
    }
}

/// # Safety
///
/// `stmt` must be null or a live statement handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_num_params(stmt: *mut dfgo_statement) -> i64 {
    if stmt.is_null() {
        return -1;
    }
    // SAFETY: `stmt` is non-null and borrowed read-only for this query.
    unsafe { (*stmt).params.count() }
}

/// # Safety
///
/// `stmt` must be null or a live statement handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_serializes(stmt: *mut dfgo_statement) -> i32 {
    if stmt.is_null() {
        return 0;
    }
    // SAFETY: `stmt` is non-null and borrowed read-only for this query.
    if unsafe { (*stmt).serializes } { 1 } else { 0 }
}

/// # Safety
///
/// `out` must point to writable storage for one cancel-token handle. `err`, when
/// non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_cancel_token_create(
    out: *mut *mut dfgo_cancel_token,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "cancel token output pointer is null",
            ));
        }

        let token = dfgo_cancel_token {
            cancel: Arc::new(CancelToken::new()),
        };

        // SAFETY: `out` is non-null. The token is Rust-owned until
        // dfgo_cancel_token_close is called.
        unsafe {
            *out = Box::into_raw(Box::new(token));
        }
        Ok(())
    })
}

/// # Safety
///
/// `token` must be null or a live cancel-token handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_cancel_token_cancel(token: *mut dfgo_cancel_token) {
    if !token.is_null() {
        // SAFETY: `token` is non-null and borrowed long enough to flip the
        // atomic cancellation flag. Ownership stays with the caller.
        let token = unsafe { &*token };
        token.cancel.cancel();
    }
}

/// # Safety
///
/// `token` must be null or a live cancel-token handle returned by
/// `dfgo_cancel_token_create` that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_cancel_token_close(token: *mut dfgo_cancel_token) {
    if !token.is_null() {
        // SAFETY: `token` must be a live handle returned from
        // dfgo_cancel_token_create and not already closed.
        unsafe {
            drop(Box::from_raw(token));
        }
    }
}

pub(crate) fn execute_with_bindings(
    stmt: &dfgo_statement,
    cancel: Arc<CancelToken>,
    bindings: Vec<Binding>,
    out: *mut *mut dfgo_result_stream,
) -> Result<(), FfiError> {
    let stream = execute_to_stream(
        stmt.inner.clone(),
        &stmt.query,
        &stmt.params,
        bindings,
        cancel.clone(),
    )?;
    let result = dfgo_result_stream {
        stream: Some(stream),
        cancel,
    };

    // SAFETY: callers validate `out` before reaching this helper. Ownership of
    // the result handle moves to the caller until dfgo_result_close.
    unsafe {
        *out = Box::into_raw(Box::new(result));
    }
    Ok(())
}

/// # Safety
///
/// `stmt` and `cancel` must be live handles. `params` must be null when
/// `params_len` is zero, or point to `params_len` readable dfgo_parameter values.
/// Every nested pointer in `params` must remain readable for the duration of this
/// call. `out` must point to writable storage for one result-stream handle.
/// `err`, when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_execute_with_params(
    stmt: *mut dfgo_statement,
    params: *const dfgo_parameter,
    params_len: i64,
    cancel: *mut dfgo_cancel_token,
    out: *mut *mut dfgo_result_stream,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if stmt.is_null() {
            return Err(FfiError::invalid_argument("statement handle is null"));
        }
        if out.is_null() {
            return Err(FfiError::invalid_argument("result output pointer is null"));
        }
        if cancel.is_null() {
            return Err(FfiError::invalid_argument("cancel token handle is null"));
        }

        // Copy params into local Bindings before execution. The statement keeps
        // only immutable prepared-query metadata, so concurrent callers with
        // separate params arrays cannot interleave parameter state.
        let bindings = bindings_from_params(params, params_len)?;
        // SAFETY: `stmt` is non-null and borrowed only for immutable prepared
        // statement state. Execution clones the Arcs it needs.
        let stmt = unsafe { &*stmt };
        // SAFETY: `cancel` is non-null and borrowed just long enough to clone
        // its Arc-backed token.
        let cancel = unsafe { &*cancel }.cancel.clone();
        execute_with_bindings(stmt, cancel, bindings, out)
    })
}

/// # Safety
///
/// `result` must be a live result-stream handle that is not concurrently used.
/// `out` must point to writable storage for one Arrow C stream. `err`, when
/// non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_result_export_arrow_stream(
    result: *mut dfgo_result_stream,
    out: *mut FFI_ArrowArrayStream,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if result.is_null() {
            return Err(FfiError::invalid_argument("result handle is null"));
        }
        if out.is_null() {
            return Err(FfiError::invalid_argument(
                "arrow stream output pointer is null",
            ));
        }

        // SAFETY: `result` is non-null and uniquely borrowed for mutation during
        // export. The ABI requires callers not to concurrently export/close the
        // same result handle.
        let result = unsafe { &mut *result };
        let stream = result
            .stream
            .take()
            .ok_or_else(|| FfiError::invalid_argument("result stream has already been exported"))?;

        // SAFETY: `out` is non-null and points to caller-owned storage for an
        // ArrowArrayStream struct. ptr::write avoids reading/dropping any
        // uninitialized bytes currently in that storage.
        unsafe {
            ptr::write(out, stream);
        }
        Ok(())
    })
}

/// # Safety
///
/// `result` must be null or a live result-stream handle returned by
/// `dfgo_statement_execute_with_params` that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_result_close(result: *mut dfgo_result_stream) {
    if !result.is_null() {
        // SAFETY: `result` is non-null and borrowed briefly to signal
        // cancellation before ownership is retaken and dropped below.
        let result_ref = unsafe { &*result };
        result_ref.cancel.cancel();
        // SAFETY: `result` must be a live handle returned from
        // dfgo_statement_execute_with_params and not already closed.
        unsafe {
            drop(Box::from_raw(result));
        }
    }
}

/// # Safety
///
/// `result` must be null or a live result-stream handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_result_cancel(result: *mut dfgo_result_stream) {
    if !result.is_null() {
        // SAFETY: `result` is non-null and only borrowed to trigger
        // cancellation; ownership remains with the caller.
        let result = unsafe { &*result };
        result.cancel.cancel();
    }
}

/// # Safety
///
/// `err` must be null or a live error handle. The returned pointer is valid only
/// until `dfgo_error_free` is called for the same handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_error_message(err: *const dfgo_error) -> *const c_char {
    if err.is_null() {
        return ptr::null();
    }

    // SAFETY: `err` is non-null and points to a live dfgo_error. The returned
    // CString pointer remains valid until dfgo_error_free.
    unsafe { (*err).message.as_ptr() }
}

/// # Safety
///
/// `err` must be null or a live error handle. The returned pointer is valid only
/// until `dfgo_error_free` is called for the same handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_error_kind(err: *const dfgo_error) -> *const c_char {
    if err.is_null() {
        return ptr::null();
    }

    // SAFETY: `err` is non-null and points to a live dfgo_error. The returned
    // CString pointer remains valid until dfgo_error_free.
    unsafe { (*err).kind.as_ptr() }
}

/// # Safety
///
/// `err` must be null or a live error handle returned through an error out
/// parameter that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_error_free(err: *mut dfgo_error) {
    if !err.is_null() {
        // SAFETY: `err` must be a live handle returned through an error out
        // parameter and not previously freed.
        unsafe {
            drop(Box::from_raw(err));
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod contract_generated;
