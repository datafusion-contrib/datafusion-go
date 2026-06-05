// FFI entry points take raw C pointers that are validated before dereferencing.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

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

#[allow(non_camel_case_types)]
mod ffi_types {
    use super::*;

    pub struct dfgo_database {
        pub(super) runtime: Arc<Runtime>,
        pub(super) config: SessionConfig,
        pub(super) shared_ctx: SessionContext,
    }

    pub struct dfgo_connection {
        pub(super) inner: Arc<Inner>,
    }

    pub struct dfgo_statement {
        pub(super) inner: Arc<Inner>,
        pub(super) query: String,
        pub(super) params: ParameterMetadata,
        pub(super) serializes: bool,
        pub(super) bindings: Mutex<Vec<Binding>>,
    }

    pub struct dfgo_result_stream {
        pub(super) stream: Option<FFI_ArrowArrayStream>,
        pub(super) cancel: Arc<CancelToken>,
    }

    pub struct dfgo_cancel_token {
        pub(super) cancel: Arc<CancelToken>,
    }

    pub struct dfgo_error {
        pub(super) kind: CString,
        pub(super) message: CString,
    }
}

use ffi_types::*;

const DFG_OK: i32 = 0;
const DFG_ERR: i32 = 1;
const DFGO_ABI_VERSION: i32 = 1;
const DATAFUSION_VERSION: &[u8] = b"53.1.0\0";
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
    kind: &'static str,
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
    runtime: Arc<Runtime>,
    ctx: SessionContext,
}

#[derive(Clone)]
struct Binding {
    name: Option<String>,
    value: Option<ScalarValue>,
}

#[derive(Clone, Debug)]
enum ParameterMetadata {
    None,
    Positional { count: i64 },
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
    cancelled: AtomicBool,
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
    inner: Arc<Inner>,
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    cancel: Arc<CancelToken>,
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

fn set_error(err: *mut *mut dfgo_error, ffi_err: FfiError) {
    if err.is_null() {
        return;
    }

    let message = ffi_err.message.replace('\0', "\\0");
    let error = dfgo_error {
        kind: CString::new(ffi_err.kind).expect("static error kind has no nul bytes"),
        message: CString::new(message).expect("nul bytes were replaced"),
    };

    unsafe {
        *err = Box::into_raw(Box::new(error));
    }
}

fn clear_error(err: *mut *mut dfgo_error) {
    if !err.is_null() {
        unsafe {
            *err = ptr::null_mut();
        }
    }
}

fn cstr_to_string(ptr: *const c_char, name: &str) -> Result<String, FfiError> {
    if ptr.is_null() {
        return Err(FfiError::invalid_argument(format!("{name} is null")));
    }

    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|e| FfiError::invalid_argument(format!("{name} is not valid UTF-8: {e}")))
}

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
        unsafe {
            Ok(slice::from_raw_parts(
                ptr,
                usize::try_from(len).map_err(|e| FfiError::invalid_argument(e.to_string()))?,
            ))
        }
    }
}

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

    // from_raw moves the callback pointers out of the caller's stream struct.
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
        Ok(None)
    } else {
        Ok(Some(Arc::<str>::from(timezone.as_str())))
    }
}

fn validate_decimal_type(precision: u8, scale: i8) -> Result<(), FfiError> {
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

    let mut scaled_digits = String::with_capacity(int_part.len() + scale);
    scaled_digits.push_str(int_part);
    scaled_digits.push_str(frac_part);
    for _ in frac_part.len()..scale {
        scaled_digits.push('0');
    }

    let significant = scaled_digits.trim_start_matches('0');
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

    Ok(FFI_ArrowArrayStream::new(Box::new(reader)))
}

fn param_values(
    metadata: &ParameterMetadata,
    bindings: Vec<Binding>,
) -> Result<Option<ParamValues>, FfiError> {
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

    let stmt = unsafe { &*stmt };
    let mut bindings = stmt
        .bindings
        .lock()
        .map_err(|e| FfiError::native(e.to_string()))?;
    let index =
        usize::try_from(index - 1).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
    if bindings.len() <= index {
        bindings.resize_with(index + 1, || Binding {
            name: None,
            value: None,
        });
    }
    bindings[index].value = Some(value);
    Ok(())
}

fn session_config_from_dsn(dsn: &str) -> Result<SessionConfig, FfiError> {
    let mut config = SessionConfig::new().with_information_schema(true);

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
            question_count += 1;
            replacements.push((
                location_offset(&query, token.span.start)?,
                location_offset(&query, token.span.end)?,
                format!("${question_count}"),
            ));
            continue;
        }

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

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_abi_version() -> i32 {
    DFGO_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_datafusion_version() -> *const c_char {
    DATAFUSION_VERSION.as_ptr().cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_database_open(
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
            String::new()
        } else {
            cstr_to_string(dsn, "dsn")?
        };

        let runtime = Runtime::new().map_err(|e| FfiError::native(e.to_string()))?;
        let config = session_config_from_dsn(&dsn)?;
        let shared_ctx = SessionContext::new_with_config(config.clone());
        let db = dfgo_database {
            runtime: Arc::new(runtime),
            config,
            shared_ctx,
        };

        unsafe {
            *out = Box::into_raw(Box::new(db));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_database_close(db: *mut dfgo_database) {
    if !db.is_null() {
        unsafe {
            drop(Box::from_raw(db));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_connection_open_isolated(
    db: *mut dfgo_database,
    out: *mut *mut dfgo_connection,
    err: *mut *mut dfgo_error,
) -> i32 {
    open_connection(db, out, err, false)
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_connection_open_shared(
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

        let db = unsafe { &*db };
        let ctx = if shared {
            db.shared_ctx.clone()
        } else {
            SessionContext::new_with_config(db.config.clone())
        };
        let conn = dfgo_connection {
            inner: Arc::new(Inner {
                runtime: db.runtime.clone(),
                ctx,
            }),
        };

        unsafe {
            *out = Box::into_raw(Box::new(conn));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_connection_close(conn: *mut dfgo_connection) {
    if !conn.is_null() {
        unsafe {
            drop(Box::from_raw(conn));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_connection_register_arrow_ipc(
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
        let conn = unsafe { &*conn };
        register_record_batches(&conn.inner, &name, schema, batches)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_connection_register_arrow_stream(
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
        let conn = unsafe { &*conn };
        register_record_batches(&conn.inner, &name, schema, batches)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_prepare(
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
        let conn = unsafe { &*conn };
        let stmt = dfgo_statement {
            inner: conn.inner.clone(),
            query: prepared.query,
            params: prepared.params,
            serializes,
            bindings: Mutex::new(Vec::new()),
        };

        unsafe {
            *out = Box::into_raw(Box::new(stmt));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_close(stmt: *mut dfgo_statement) {
    if !stmt.is_null() {
        unsafe {
            drop(Box::from_raw(stmt));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_num_params(stmt: *mut dfgo_statement) -> i64 {
    if stmt.is_null() {
        return -1;
    }
    unsafe { (*stmt).params.count() }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_serializes(stmt: *mut dfgo_statement) -> i32 {
    if stmt.is_null() {
        return 0;
    }
    if unsafe { (*stmt).serializes } { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_cancel_token_create(
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

        unsafe {
            *out = Box::into_raw(Box::new(token));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_cancel_token_cancel(token: *mut dfgo_cancel_token) {
    if !token.is_null() {
        let token = unsafe { &*token };
        token.cancel.cancel();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_cancel_token_close(token: *mut dfgo_cancel_token) {
    if !token.is_null() {
        unsafe {
            drop(Box::from_raw(token));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_clear_bindings(
    stmt: *mut dfgo_statement,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        if stmt.is_null() {
            return Err(FfiError::invalid_argument("statement handle is null"));
        }

        let stmt = unsafe { &*stmt };
        stmt.bindings
            .lock()
            .map_err(|e| FfiError::native(e.to_string()))?
            .clear();
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_set_param_name(
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

        let stmt = unsafe { &*stmt };
        let mut bindings = stmt
            .bindings
            .lock()
            .map_err(|e| FfiError::native(e.to_string()))?;
        let index =
            usize::try_from(index - 1).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
        if bindings.len() <= index {
            bindings.resize_with(index + 1, || Binding {
                name: None,
                value: None,
            });
        }
        bindings[index].name = Some(name);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_null(
    stmt: *mut dfgo_statement,
    index: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || bind_value(stmt, index, ScalarValue::Null))
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_bool(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i32,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Boolean(Some(value != 0)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_int64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Int64(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_uint64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: u64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::UInt64(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_float64(
    stmt: *mut dfgo_statement,
    index: i64,
    value: f64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Float64(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_date32(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i32,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Date32(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_time64_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::Time64Nanosecond(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_timestamp_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(
            stmt,
            index,
            ScalarValue::TimestampNanosecond(Some(value), Some(Arc::<str>::from("UTC"))),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_timestamp_ns_tz(
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

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_duration_ns(
    stmt: *mut dfgo_statement,
    index: i64,
    value: i64,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        bind_value(stmt, index, ScalarValue::DurationNanosecond(Some(value)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_decimal128(
    stmt: *mut dfgo_statement,
    index: i64,
    value: *const c_char,
    len: i64,
    precision: u8,
    scale: i8,
    err: *mut *mut dfgo_error,
) -> i32 {
    run_ffi(err, || {
        let value = bytes_to_string(value, len, "decimal value")?;
        let scaled = parse_decimal128(&value, precision, scale)?;
        bind_value(
            stmt,
            index,
            ScalarValue::Decimal128(Some(scaled), precision, scale),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_typed_null(
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
        let value = typed_null(
            type_code,
            precision,
            scale,
            optional_timezone(timezone, timezone_len)?,
        )?;
        bind_value(stmt, index, value)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_string(
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

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_bind_binary(
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

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_statement_execute(
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

        let stmt = unsafe { &*stmt };
        let cancel = unsafe { &*cancel }.cancel.clone();
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

        unsafe {
            *out = Box::into_raw(Box::new(result));
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_result_export_arrow_stream(
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

        let result = unsafe { &mut *result };
        let stream = result
            .stream
            .take()
            .ok_or_else(|| FfiError::invalid_argument("result stream has already been exported"))?;

        unsafe {
            ptr::write(out, stream);
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_result_close(result: *mut dfgo_result_stream) {
    if !result.is_null() {
        let result_ref = unsafe { &*result };
        result_ref.cancel.cancel();
        unsafe {
            drop(Box::from_raw(result));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_result_cancel(result: *mut dfgo_result_stream) {
    if !result.is_null() {
        let result = unsafe { &*result };
        result.cancel.cancel();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_error_message(err: *const dfgo_error) -> *const c_char {
    if err.is_null() {
        return ptr::null();
    }

    unsafe { (*err).message.as_ptr() }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_error_kind(err: *const dfgo_error) -> *const c_char {
    if err.is_null() {
        return ptr::null();
    }

    unsafe { (*err).kind.as_ptr() }
}

#[unsafe(no_mangle)]
pub extern "C" fn dfgo_error_free(err: *mut dfgo_error) {
    if !err.is_null() {
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
