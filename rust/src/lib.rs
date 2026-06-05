//! Native DataFusion bridge for the Go `database/sql` driver.
//!
//! This crate is compiled into a C static library. The `dfgo_*` functions below
//! are therefore part of a manually-maintained ABI shared with
//! `rust/include/datafusion_go.h` and the cgo wrapper in `internal/native`.
//!
//! The most important maintenance rule in this file is that Rust must own every
//! allocation it later frees, and Go/C must free every handle exported through
//! the matching `dfgo_*_close` or `dfgo_error_free` function. Keep that contract
//! explicit when adding new functions; the compiler cannot check it across FFI.

#![deny(clippy::missing_safety_doc)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::{BTreeSet, HashMap};
use std::ffi::{CStr, CString, c_char};
use std::fmt;
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ParamValues, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::{DFParser, Statement as DFStatement};
use datafusion_sql::sqlparser::ast::Statement as SQLStatement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::tokenizer::{Location, Token, Tokenizer};
use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::Notify;

mod generated;

use generated::{DATAFUSION_VERSION, DFGO_ABI_VERSION};

// These names intentionally match the C header typedefs. They are opaque to Go
// and C callers; only Rust ever inspects their fields.
#[allow(non_camel_case_types)]
mod ffi_types {
    use super::*;

    // Database lifetime owns the Tokio runtime and the shared-session context.
    // Connections clone these values, so closing the database only releases the
    // top-level handle after existing connections have taken their own Arcs.
    pub struct dfgo_database {
        pub(super) runtime: Arc<Runtime>,
        pub(super) config: SessionConfig,
        pub(super) shared_ctx: SessionContext,
    }

    // A connection owns an execution context. It may be isolated or a clone of
    // the database-level shared context depending on the DSN/session setting.
    pub struct dfgo_connection {
        pub(super) inner: Arc<Inner>,
    }

    // Prepared statements keep normalized SQL plus parser-derived parameter
    // metadata. Bindings are mutable because database/sql reuses statements.
    pub struct dfgo_statement {
        pub(super) inner: Arc<Inner>,
        pub(super) query: String,
        pub(super) params: ParameterMetadata,
        pub(super) serializes: bool,
        pub(super) bindings: Mutex<Vec<Binding>>,
    }

    // A result can be exported exactly once as an Arrow C stream. The Option is
    // the single-export guard; the cancellation token remains available for
    // explicit cancellation and close.
    pub struct dfgo_result_stream {
        pub(super) stream: Option<FFI_ArrowArrayStream>,
        pub(super) cancel: Arc<CancelToken>,
    }

    // Separate cancel tokens let Go cancel a query before the Arrow stream has
    // been constructed, while the result stream can keep sharing the same token.
    pub struct dfgo_cancel_token {
        pub(super) cancel: Arc<CancelToken>,
    }

    // Errors are heap-allocated so C/Go can read stable pointers until
    // dfgo_error_free transfers ownership back to Rust and drops the value.
    pub struct dfgo_error {
        pub(super) kind: CString,
        pub(super) message: CString,
    }
}

use ffi_types::*;

const DFG_OK: i32 = 0;
const DFG_ERR: i32 = 1;
const CANCELLED_MESSAGE: &str = "query canceled";
const ERROR_KIND_CANCELLED: &str = "cancelled";
const ERROR_KIND_INVALID_ARGUMENT: &str = "invalid_argument";
const ERROR_KIND_NATIVE: &str = "native";
const ERROR_KIND_PANIC: &str = "panic";
const PARAMETER_BOOL: i32 = 1;
const PARAMETER_INT64: i32 = 2;
const PARAMETER_UINT64: i32 = 3;
const PARAMETER_FLOAT64: i32 = 4;
const PARAMETER_STRING: i32 = 5;
const PARAMETER_BINARY: i32 = 6;
const PARAMETER_DATE: i32 = 7;
const PARAMETER_TIME: i32 = 8;
const PARAMETER_TIMESTAMP: i32 = 9;
const PARAMETER_DURATION: i32 = 10;
const PARAMETER_DECIMAL: i32 = 11;

#[derive(Debug)]
struct FfiError {
    // `kind` is static because callers branch on stable symbolic categories.
    kind: &'static str,
    // `message` is owned because most errors are formatted at the failing site.
    message: String,
}

impl FfiError {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn cancelled() -> Self {
        Self::new(ERROR_KIND_CANCELLED, CANCELLED_MESSAGE)
    }

    fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ERROR_KIND_INVALID_ARGUMENT, message)
    }

    fn native(message: impl Into<String>) -> Self {
        Self::new(ERROR_KIND_NATIVE, message)
    }

    fn panic() -> Self {
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

struct Inner {
    // DataFusion APIs are async, but the Arrow C stream callbacks are
    // synchronous. Keeping the runtime here lets callbacks block_on future work
    // while ensuring the runtime outlives any exported stream.
    runtime: Arc<Runtime>,
    ctx: SessionContext,
}

#[derive(Clone)]
struct Binding {
    // Name is present only for database/sql named arguments.
    name: Option<String>,
    // None means a binding slot was referenced but no value has been supplied.
    // That lets validation produce a targeted "missing value" error.
    value: Option<ScalarValue>,
}

#[derive(Clone, Debug)]
enum ParameterMetadata {
    // No placeholders were found during prepare.
    None,
    // Positional metadata stores the highest required slot. `$3` therefore
    // requires three arguments even if `$1` or `$2` are absent from the SQL.
    Positional { count: i64 },
    // Named metadata stores distinct names. Repeated `$name` placeholders bind
    // from a single sql.Named argument.
    Named { names: BTreeSet<String> },
}

struct PreparedQuery {
    query: String,
    params: ParameterMetadata,
}

impl ParameterMetadata {
    fn count(&self) -> i64 {
        match self {
            Self::None => 0,
            Self::Positional { count } => *count,
            Self::Named { names } => i64::try_from(names.len()).unwrap_or(i64::MAX),
        }
    }
}

struct CancelToken {
    // Atomic state gives a cheap fast path for synchronous callbacks.
    cancelled: AtomicBool,
    // Notify wakes async DataFusion work that is waiting inside tokio::select!.
    notify: Notify,
}

impl CancelToken {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    async fn cancelled(&self) {
        loop {
            // Register interest before reading the atomic flag. If cancellation
            // happens between these two operations, notified.await completes
            // immediately instead of sleeping forever.
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
struct CancelledError;

impl fmt::Display for CancelledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CANCELLED_MESSAGE)
    }
}

impl std::error::Error for CancelledError {}

struct StreamingReader {
    // Hold Inner so the runtime and SessionContext stay alive for every C stream
    // callback. The Go side may close statements/connections while rows remain.
    inner: Arc<Inner>,
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    cancel: Arc<CancelToken>,
    // Once Arrow sees EOF or an error, the C stream must remain terminal.
    done: bool,
}

impl Iterator for StreamingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.cancel.is_cancelled() {
            self.done = true;
            return Some(Err(cancelled_arrow_error()));
        }

        let cancel = self.cancel.clone();
        let stream = &mut self.stream;
        // Arrow's C stream API is pull-based and synchronous. DataFusion's
        // stream is async, so each pull blocks this crate's runtime until either
        // the next batch arrives or cancellation wins the select.
        let next = self.inner.runtime.block_on(async {
            tokio::select! {
                _ = cancel.cancelled() => Some(Err(cancelled_datafusion_error())),
                item = stream.next() => item,
            }
        });

        match next {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => {
                self.done = true;
                Some(Err(ArrowError::ExternalError(Box::new(err))))
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}

impl RecordBatchReader for StreamingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn cancelled_datafusion_error() -> DataFusionError {
    DataFusionError::Execution(CANCELLED_MESSAGE.to_owned())
}

fn cancelled_arrow_error() -> ArrowError {
    ArrowError::ExternalError(Box::new(CancelledError))
}

// Store a Rust-owned error handle for C/Go. Callers receive stable C string
// pointers through dfgo_error_kind/message and must later call dfgo_error_free.
fn set_error(err: *mut *mut dfgo_error, ffi_err: FfiError) {
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
fn clear_error(err: *mut *mut dfgo_error) {
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
fn cstr_to_string(ptr: *const c_char, name: &str) -> Result<String, FfiError> {
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
fn bytes_from_ptr<'a>(ptr: *const u8, len: i64, name: &str) -> Result<&'a [u8], FfiError> {
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
fn bytes_to_string(ptr: *const c_char, len: i64, name: &str) -> Result<String, FfiError> {
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

fn register_record_batches(
    inner: &Inner,
    table_name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(), FfiError> {
    if table_name.trim().is_empty() {
        return Err(FfiError::invalid_argument("table name is empty"));
    }

    // A MemTable materializes the registered data at registration time. The
    // zero-copy Arrow path below preserves buffers, but it still eagerly imports
    // all batches into this table rather than registering a streaming source.
    let table = MemTable::try_new(schema, vec![batches])?;
    inner.ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

fn ipc_batches(data: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), FfiError> {
    // Own the IPC bytes on the Rust side before decoding. This keeps the safe
    // registration path independent of the Go byte slice passed through cgo.
    let mut reader = StreamReader::try_new(Cursor::new(data.to_vec()), None)?;
    let schema = reader.schema();
    let batches = reader.by_ref().collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((schema, batches))
}

fn arrow_stream_batches(
    stream: *mut FFI_ArrowArrayStream,
) -> Result<(SchemaRef, Vec<RecordBatch>), FfiError> {
    if stream.is_null() {
        return Err(FfiError::invalid_argument("arrow stream pointer is null"));
    }

    // SAFETY: `stream` is non-null and points to an ArrowArrayStream exported by
    // the Go Arrow library. from_raw moves the callback pointers out of the
    // caller's stream struct, so the caller must not release those callbacks
    // again after this call.
    //
    // The imported RecordBatches keep Arrow release callbacks, so this is the
    // zero-copy path: table lifetime must be tied to valid exported buffers.
    let mut reader = unsafe { ArrowArrayStreamReader::from_raw(stream) }?;
    let schema = reader.schema();
    let batches = reader.by_ref().collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((schema, batches))
}

fn optional_timezone(ptr: *const c_char, len: i64) -> Result<Option<Arc<str>>, FfiError> {
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

fn validate_decimal_type(precision: u8, scale: i8) -> Result<(), FfiError> {
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

fn parse_decimal128(value: &str, precision: u8, scale: i8) -> Result<i128, FfiError> {
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

fn typed_null(
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

fn run_ffi(err: *mut *mut dfgo_error, f: impl FnOnce() -> Result<(), FfiError>) -> i32 {
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

fn execute_to_stream(
    inner: Arc<Inner>,
    query: &str,
    params: &ParameterMetadata,
    bindings: Vec<Binding>,
    cancel: Arc<CancelToken>,
) -> Result<FFI_ArrowArrayStream, FfiError> {
    let stream = inner.runtime.block_on(async {
        // Check cancellation around both planning and execution. DataFusion may
        // still do CPU work between await points, but these gates keep canceled
        // contexts from starting avoidable work and make streaming reads stop.
        let df = tokio::select! {
            _ = cancel.cancelled() => return Err(FfiError::cancelled()),
            df = inner.ctx.sql(query) => df.map_err(FfiError::from)?,
        };

        let df = if let Some(values) = param_values(params, bindings)? {
            df.with_param_values(values).map_err(FfiError::from)?
        } else {
            df
        };

        tokio::select! {
            _ = cancel.cancelled() => Err(FfiError::cancelled()),
            stream = df.execute_stream() => stream.map_err(FfiError::from),
        }
    })?;

    let schema = stream.schema();
    let reader = StreamingReader {
        inner,
        schema,
        stream,
        cancel,
        done: false,
    };

    // FFI_ArrowArrayStream takes ownership of the boxed reader. Go later calls
    // the Arrow release callback through cdata, which drops the reader and
    // releases the DataFusion stream.
    Ok(FFI_ArrowArrayStream::new(Box::new(reader)))
}

fn param_values(
    metadata: &ParameterMetadata,
    bindings: Vec<Binding>,
) -> Result<Option<ParamValues>, FfiError> {
    // Validate the argument shape before handing values to DataFusion. The goal
    // is to report database/sql-friendly errors for mixed named/positional
    // inputs, duplicates, sparse positional bindings, and missing values.
    match metadata {
        ParameterMetadata::None => {
            if bindings.is_empty() {
                Ok(None)
            } else {
                Err(FfiError::invalid_argument(format!(
                    "SQL statement has no placeholders but got {} argument(s); remove the arguments or add ?, $1, or $name placeholders",
                    bindings.len()
                )))
            }
        }
        ParameterMetadata::Positional { count } => {
            // Positional placeholders are dense by contract: `$3` means callers
            // must pass arguments 1, 2, and 3. This matches database/sql's
            // NumInput behavior and avoids surprising sparse binding semantics.
            let count =
                usize::try_from(*count).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
            if bindings.len() != count {
                return Err(FfiError::invalid_argument(format!(
                    "SQL statement expects {count} positional argument(s), got {}; pass exactly {count} plain argument(s) for the ?, $1, $2, ... placeholders",
                    bindings.len()
                )));
            }

            let mut params = Vec::with_capacity(count);
            for (idx, binding) in bindings.into_iter().enumerate() {
                if let Some(name) = binding.name {
                    return Err(FfiError::invalid_argument(format!(
                        "SQL statement uses positional placeholders but got named argument {name}; pass a plain argument instead of sql.Named"
                    )));
                }
                let value = binding.value.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "SQL argument {} has no value; pass a non-missing value or a typed null such as datafusion.NullOf(...)",
                        idx + 1
                    ))
                })?;
                params.push(value);
            }
            Ok(Some(ParamValues::from(params)))
        }
        ParameterMetadata::Named { names } => {
            // For named placeholders, repeated `$name` occurrences share one
            // supplied value. Requiring exact name coverage catches typos before
            // DataFusion sees the query.
            if bindings.len() != names.len() {
                return Err(FfiError::invalid_argument(format!(
                    "SQL statement expects {} named argument(s) {}, got {}; pass matching sql.Named values",
                    names.len(),
                    expected_parameter_list(names),
                    bindings.len()
                )));
            }

            let mut params = HashMap::new();
            let mut seen = BTreeSet::new();
            for (idx, binding) in bindings.into_iter().enumerate() {
                let name = binding.name.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "SQL statement uses named placeholders {}; argument {} is positional, so pass sql.Named(\"name\", value)",
                        expected_parameter_list(names),
                        idx + 1
                    ))
                })?;
                if !names.contains(&name) {
                    return Err(FfiError::invalid_argument(format!(
                        "unexpected named argument {name}; expected one of {}",
                        expected_parameter_list(names)
                    )));
                }
                if !seen.insert(name.clone()) {
                    return Err(FfiError::invalid_argument(format!(
                        "duplicate named argument {name}; pass each named placeholder once"
                    )));
                }
                let value = binding.value.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "named argument {name} has no value; pass a non-missing value or a typed null such as datafusion.NullOf(...)"
                    ))
                })?;
                params.insert(name, value);
            }

            for name in names {
                if !seen.contains(name) {
                    return Err(FfiError::invalid_argument(format!(
                        "missing named argument {name}; pass sql.Named({name:?}, value)"
                    )));
                }
            }

            Ok(Some(ParamValues::from(params)))
        }
    }
}

fn expected_parameter_list(names: &BTreeSet<String>) -> String {
    names
        .iter()
        .map(|name| format!("${name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn bind_value(stmt: *mut dfgo_statement, index: i64, value: ScalarValue) -> Result<(), FfiError> {
    if stmt.is_null() {
        return Err(FfiError::invalid_argument("statement handle is null"));
    }
    if index <= 0 {
        return Err(FfiError::invalid_argument(format!(
            "parameter index must be positive, got {index}"
        )));
    }

    // SAFETY: `stmt` was checked for null above and is expected to be a live
    // handle previously returned by dfgo_prepare.
    let stmt = unsafe { &*stmt };
    let mut bindings = stmt
        .bindings
        .lock()
        .map_err(|e| FfiError::native(e.to_string()))?;
    let index =
        usize::try_from(index - 1).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
    if bindings.len() <= index {
        // Grow sparse database/sql ordinals into explicit slots so later shape
        // validation can distinguish "slot missing" from "value missing".
        bindings.resize_with(index + 1, || Binding {
            name: None,
            value: None,
        });
    }
    bindings[index].value = Some(value);
    Ok(())
}

fn session_config_from_dsn(dsn: &str) -> Result<SessionConfig, FfiError> {
    // Information schema is enabled by default because database/sql clients
    // commonly introspect columns and types after preparing or running queries.
    let mut config = SessionConfig::new().with_information_schema(true);

    // Supported DSNs are intentionally lightweight: the query string, if any,
    // contains DataFusion configuration options. The URL host/path are ignored
    // by the Go layer before this point.
    let Some((_, query)) = dsn.split_once('?') else {
        return Ok(config);
    };

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key.is_empty() {
            continue;
        }

        config
            .options_mut()
            .set(key.as_ref(), value.as_ref())
            .map_err(|e| {
                FfiError::invalid_argument(format!("invalid DataFusion config option {key}: {e}"))
            })?;
    }

    Ok(config)
}

fn prepare_query(query: String) -> Result<PreparedQuery, FfiError> {
    let dialect = GenericDialect {};
    // Use sqlparser's tokenizer instead of scanning strings manually. That
    // keeps placeholders inside string literals and comments untouched.
    let tokens = Tokenizer::new(&dialect, &query)
        .tokenize_with_location()
        .map_err(|e| FfiError::invalid_argument(e.to_string()))?;

    let mut positional_max = 0_i64;
    let mut named = BTreeSet::new();
    let mut question_count = 0_i64;
    let mut replacements = Vec::new();

    for token in tokens {
        let Token::Placeholder(placeholder) = token.token else {
            continue;
        };

        if placeholder == "?" {
            // DataFusion accepts `$1` style positional placeholders, so rewrite
            // database/sql question marks during prepare. Locations from the
            // tokenizer are line/column based and must be converted to byte
            // offsets before slicing the original UTF-8 SQL string.
            question_count += 1;
            replacements.push((
                location_offset(&query, token.span.start)?,
                location_offset(&query, token.span.end)?,
                format!("${question_count}"),
            ));
            continue;
        }

        // `$1` and `$name` placeholders pass through to DataFusion unchanged.
        // Other placeholder syntaxes are rejected here so callers get a stable
        // invalid_argument error rather than parser-dependent behavior later.
        let Some(id) = placeholder.strip_prefix('$') else {
            return Err(FfiError::invalid_argument(format!(
                "unsupported placeholder syntax {placeholder}; use ?, $1, or $name"
            )));
        };
        if id.is_empty() {
            return Err(FfiError::invalid_argument("placeholder name is empty"));
        }

        if id.chars().all(|c| c.is_ascii_digit()) {
            let index = id.parse::<i64>().map_err(|e| {
                FfiError::invalid_argument(format!("invalid placeholder {placeholder}: {e}"))
            })?;
            if index <= 0 {
                return Err(FfiError::invalid_argument(format!(
                    "invalid placeholder {placeholder}; indexes are 1-based"
                )));
            }
            positional_max = positional_max.max(index);
        } else {
            named.insert(id.to_owned());
        }
    }

    if question_count > 0 && (positional_max > 0 || !named.is_empty()) {
        // Mixing placeholder families makes NumInput and database/sql argument
        // normalization ambiguous, especially after `?` gets rewritten to `$n`.
        return Err(FfiError::invalid_argument(
            "mixed question-mark, named, and dollar-numbered parameters are not supported",
        ));
    }
    if positional_max > 0 && !named.is_empty() {
        return Err(FfiError::invalid_argument(
            "mixed named and positional parameters are not supported",
        ));
    }
    if question_count > 0 {
        return Ok(PreparedQuery {
            query: rewrite_query(&query, replacements),
            params: ParameterMetadata::Positional {
                count: question_count,
            },
        });
    }
    if !named.is_empty() {
        return Ok(PreparedQuery {
            query,
            params: ParameterMetadata::Named { names: named },
        });
    }
    if positional_max > 0 {
        return Ok(PreparedQuery {
            query,
            params: ParameterMetadata::Positional {
                count: positional_max,
            },
        });
    }
    Ok(PreparedQuery {
        query,
        params: ParameterMetadata::None,
    })
}

fn statement_serializes(stmt: &DFStatement) -> bool {
    // database/sql serializes operations that may mutate connection/session
    // state. Pure queries and SHOW-like statements can run concurrently on the
    // same prepared statement.
    match stmt {
        DFStatement::Statement(stmt) => sql_statement_serializes(stmt),
        DFStatement::Explain(stmt) => statement_serializes(&stmt.statement),
        _ => true,
    }
}

fn sql_statement_serializes(stmt: &SQLStatement) -> bool {
    match stmt {
        SQLStatement::Query(_)
        | SQLStatement::ExplainTable { .. }
        | SQLStatement::ShowFunctions { .. }
        | SQLStatement::ShowVariable { .. }
        | SQLStatement::ShowStatus { .. }
        | SQLStatement::ShowVariables { .. }
        | SQLStatement::ShowCreate { .. }
        | SQLStatement::ShowColumns { .. }
        | SQLStatement::ShowDatabases { .. }
        | SQLStatement::ShowSchemas { .. }
        | SQLStatement::ShowCharset(_)
        | SQLStatement::ShowObjects(_)
        | SQLStatement::ShowTables { .. }
        | SQLStatement::ShowViews { .. }
        | SQLStatement::ShowCollation { .. } => false,
        SQLStatement::Explain { statement, .. } => sql_statement_serializes(statement),
        _ => true,
    }
}

fn rewrite_query(query: &str, replacements: Vec<(usize, usize, String)>) -> String {
    // Replacements are produced in tokenizer order. Building a new String avoids
    // in-place byte shifting while preserving every untouched byte exactly.
    let mut rewritten = String::with_capacity(query.len() + replacements.len());
    let mut last = 0;
    for (start, end, replacement) in replacements {
        rewritten.push_str(&query[last..start]);
        rewritten.push_str(&replacement);
        last = end;
    }
    rewritten.push_str(&query[last..]);
    rewritten
}

fn location_offset(query: &str, target: Location) -> Result<usize, FfiError> {
    // sqlparser reports line/column positions, while Rust string slicing needs
    // byte offsets. Count Unicode scalar values to find the exact char boundary
    // rather than assuming byte-oriented columns.
    if target.line == 0 && target.column == 0 {
        return Err(FfiError::invalid_argument(
            "placeholder span has empty source location",
        ));
    }

    let mut line = 1_u64;
    let mut column = 1_u64;
    for (idx, ch) in query.char_indices() {
        if line == target.line && column == target.column {
            return Ok(idx);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    if line == target.line && column == target.column {
        return Ok(query.len());
    }

    Err(FfiError::invalid_argument(format!(
        "placeholder span location {target} is outside query text"
    )))
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

fn open_connection(
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
        let prepared = prepare_query(cstr_to_string(query, "query")?)?;
        let statements = DFParser::parse_sql(&prepared.query)
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
        // SAFETY: `conn` is non-null and live; the prepared statement clones the
        // Arc it needs, so it can outlive the borrowed connection reference.
        let conn = unsafe { &*conn };
        let stmt = dfgo_statement {
            inner: conn.inner.clone(),
            query: prepared.query,
            params: prepared.params,
            serializes,
            bindings: Mutex::new(Vec::new()),
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
        // not already closed. Dropping it also drops any cached bindings.
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

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_clear_bindings(
    stmt: *mut dfgo_statement,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if stmt.is_null() {
            return Err(FfiError::invalid_argument("statement handle is null"));
        }

        // SAFETY: `stmt` is non-null and borrowed for the duration of the clear.
        let stmt = unsafe { &*stmt };
        stmt.bindings
            .lock()
            .map_err(|e| FfiError::native(e.to_string()))?
            .clear();
        Ok(())
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `name` must point to a valid
/// NUL-terminated C string. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_set_param_name(
    stmt: *mut dfgo_statement,
    index: i64,
    name: *const c_char,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if stmt.is_null() {
            return Err(FfiError::invalid_argument("statement handle is null"));
        }
        if index <= 0 {
            return Err(FfiError::invalid_argument(format!(
                "parameter index must be positive, got {index}"
            )));
        }

        let name = cstr_to_string(name, "parameter name")?;
        if name.is_empty() {
            return Err(FfiError::invalid_argument("parameter name is empty"));
        }

        // SAFETY: `stmt` is non-null and borrowed while updating its binding
        // table. The mutex protects concurrent database/sql use of a statement.
        let stmt = unsafe { &*stmt };
        let mut bindings = stmt
            .bindings
            .lock()
            .map_err(|e| FfiError::native(e.to_string()))?;
        let index =
            usize::try_from(index - 1).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
        if bindings.len() <= index {
            // A name can arrive before its value. Preserve the partially-filled
            // slot so the later value binder or execution validator can finish
            // or report the correct missing-value error.
            bindings.resize_with(index + 1, || Binding {
                name: None,
                value: None,
            });
        }
        bindings[index].name = Some(name);
        Ok(())
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_null(
    stmt: *mut dfgo_statement,
    index: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || bind_value(stmt, index, ScalarValue::Null))
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_bool(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i32,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Boolean(Some(value != 0)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_int64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Int64(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_uint64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: u64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::UInt64(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_float64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: f64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Float64(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_date32(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i32,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Date32(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_time64_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Time64Nanosecond(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_timestamp_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        // Plain time.Time values from Go bind as UTC timestamps. Callers that
        // need an explicit Arrow time zone use dfgo_statement_bind_timestamp_ns_tz.
        bind_value(
            stmt,
            index,
            ScalarValue::TimestampNanosecond(Some(value), Some(Arc::<str>::from("UTC"))),
        )
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `timezone` must be null when
/// `timezone_len` is zero, or point to `timezone_len` readable bytes for the
/// duration of this call. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_timestamp_ns_tz(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    timezone: *const c_char,
    timezone_len: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(
            stmt,
            index,
            ScalarValue::TimestampNanosecond(
                Some(value),
                optional_timezone(timezone, timezone_len)?,
            ),
        )
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_duration_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::DurationNanosecond(Some(value)))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `value` must be null when `len` is
/// zero, or point to `len` readable bytes for the duration of this call. `err`,
/// when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_decimal128(
    stmt: *mut dfgo_statement,
    index: i64,
    value: *const c_char,
    len: i64,
    precision: u8,
    scale: i8,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        // Decimal inputs are copied into a Rust String before parsing, so the Go
        // C string only needs to live for this call.
        let value = bytes_to_string(value, len, "decimal value")?;
        let scaled = parse_decimal128(&value, precision, scale)?;
        bind_value(
            stmt,
            index,
            ScalarValue::Decimal128(Some(scaled), precision, scale),
        )
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `timezone` must be null when
/// `timezone_len` is zero, or point to `timezone_len` readable bytes for the
/// duration of this call. `err`, when non-null, must point to writable
/// error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_typed_null(
    stmt: *mut dfgo_statement,
    index: i64,
    type_code: i32,
    precision: u8,
    scale: i8,
    timezone: *const c_char,
    timezone_len: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        // The typed-null payload is converted into a full ScalarValue now so
        // execution does not need to inspect the original C arguments later.
        let value = typed_null(
            type_code,
            precision,
            scale,
            optional_timezone(timezone, timezone_len)?,
        )?;
        bind_value(stmt, index, value)
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `value` must be null when `len` is
/// zero, or point to `len` readable bytes for the duration of this call. `err`,
/// when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_string(
    stmt: *mut dfgo_statement,
    index: i64,
    value: *const c_char,
    len: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if value.is_null() && len != 0 {
            return Err(FfiError::invalid_argument("string value pointer is null"));
        }
        if len < 0 {
            return Err(FfiError::invalid_argument(format!(
                "string length must be non-negative, got {len}"
            )));
        }

        let bytes = if len == 0 {
            &[]
        } else {
            // SAFETY: non-empty strings require a non-null pointer, negative
            // lengths were rejected, and the slice is copied into an owned
            // String before this FFI call returns.
            unsafe {
                slice::from_raw_parts(
                    value.cast::<u8>(),
                    usize::try_from(len).map_err(|e| FfiError::invalid_argument(e.to_string()))?,
                )
            }
        };
        let value = std::str::from_utf8(bytes).map_err(|e| {
            FfiError::invalid_argument(format!("string parameter is not valid UTF-8: {e}"))
        })?;

        bind_value(stmt, index, ScalarValue::Utf8(Some(value.to_owned())))
    })
}

/// # Safety
///
/// `stmt` must be a live statement handle. `value` must be null when `len` is
/// zero, or point to `len` readable bytes for the duration of this call. `err`,
/// when non-null, must point to writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_bind_binary(
    stmt: *mut dfgo_statement,
    index: i64,
    value: *const u8,
    len: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if value.is_null() && len != 0 {
            return Err(FfiError::invalid_argument("binary value pointer is null"));
        }
        if len < 0 {
            return Err(FfiError::invalid_argument(format!(
                "binary length must be non-negative, got {len}"
            )));
        }

        let bytes = if len == 0 {
            &[]
        } else {
            // SAFETY: non-empty binary values require a non-null pointer,
            // negative lengths were rejected, and the bytes are copied into an
            // owned Vec before this FFI call returns.
            unsafe {
                slice::from_raw_parts(
                    value,
                    usize::try_from(len).map_err(|e| FfiError::invalid_argument(e.to_string()))?,
                )
            }
        };

        bind_value(stmt, index, ScalarValue::Binary(Some(bytes.to_vec())))
    })
}

/// # Safety
///
/// `stmt` and `cancel` must be live handles. `out` must point to writable
/// storage for one result-stream handle. `err`, when non-null, must point to
/// writable error-handle storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_statement_execute(
    stmt: *mut dfgo_statement,
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

        // SAFETY: all handles were checked for null. They are borrowed only for
        // this call; cloned Arcs and ScalarValues keep the execution state alive
        // after the borrow ends.
        let stmt = unsafe { &*stmt };
        // SAFETY: `cancel` is non-null and borrowed just long enough to clone
        // its Arc-backed token.
        let cancel = unsafe { &*cancel }.cancel.clone();
        // Clone bindings while holding the mutex, then release the lock before
        // planning/execution. That avoids blocking concurrent statement cleanup
        // or rebinding while DataFusion does async work.
        let bindings = stmt
            .bindings
            .lock()
            .map_err(|e| FfiError::native(e.to_string()))?
            .clone();
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

        // SAFETY: `out` is non-null. Ownership of the result handle moves to the
        // caller until dfgo_result_close.
        unsafe {
            *out = Box::into_raw(Box::new(result));
        }
        Ok(())
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
/// `dfgo_statement_execute` that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dfgo_result_close(result: *mut dfgo_result_stream) {
    if !result.is_null() {
        // SAFETY: `result` is non-null and borrowed briefly to signal
        // cancellation before ownership is retaken and dropped below.
        let result_ref = unsafe { &*result };
        result_ref.cancel.cancel();
        // SAFETY: `result` must be a live handle returned from
        // dfgo_statement_execute and not already closed.
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
mod tests {
    use super::*;

    #[test]
    fn rewrites_question_marks_with_unicode_comments_and_multiline_sql() {
        let query = "select '雪?' as literal, ? as first, -- ? comment\n  'Ω' || ? as second, /* ? block */ ? as third";

        let prepared = prepare_query(query.to_owned()).expect("prepare query");

        assert_eq!(prepared.params.count(), 3);
        assert_eq!(
            prepared.query,
            "select '雪?' as literal, $1 as first, -- ? comment\n  'Ω' || $2 as second, /* ? block */ $3 as third"
        );
    }

    #[test]
    fn rejects_malformed_or_mixed_placeholder_variants() {
        for query in [
            "select ?1",
            "select ?, $1",
            "select ?, $value",
            "select $1, $value",
            "select $0",
            "select $",
        ] {
            assert!(
                prepare_query(query.to_owned()).is_err(),
                "expected {query:?} to fail"
            );
        }
    }

    #[test]
    fn location_offset_handles_multibyte_characters_and_line_endings() {
        let query = "αβ\n雪 ?\nline";

        assert_eq!(
            location_offset(query, Location { line: 2, column: 3 }).expect("offset"),
            "αβ\n雪 ".len()
        );
        assert_eq!(
            location_offset(query, Location { line: 3, column: 5 }).expect("offset"),
            "αβ\n雪 ?\nline".len()
        );
        assert!(
            location_offset(
                query,
                Location {
                    line: 99,
                    column: 1
                }
            )
            .is_err()
        );
    }

    #[test]
    fn parses_decimal128_strings_to_scaled_values() {
        assert_eq!(parse_decimal128("123.45", 10, 2).unwrap(), 12345);
        assert_eq!(parse_decimal128("-.5", 10, 3).unwrap(), -500);
        assert_eq!(parse_decimal128("+0", 1, 0).unwrap(), 0);
        assert!(parse_decimal128("123.456", 10, 2).is_err());
        assert!(parse_decimal128("1000", 3, 0).is_err());
    }
}
