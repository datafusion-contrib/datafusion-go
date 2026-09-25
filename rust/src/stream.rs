//! Execution, cancellation, and the lifetime of exported Arrow results.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::Notify;

use crate::error::{CANCELLED_MESSAGE, FfiError};
use crate::query::{Binding, ParameterMetadata, param_values};
use crate::session::Inner;

pub(crate) struct CancelToken {
    // Atomic state gives a cheap fast path for synchronous callbacks.
    pub(crate) cancelled: AtomicBool,
    pub(crate) error_kind: AtomicU8,
    // Notify wakes async DataFusion work that is waiting inside tokio::select!.
    pub(crate) notify: Notify,
    pub(crate) operation: Mutex<Option<Arc<crate::callbacks::Owner>>>,
}

impl CancelToken {
    pub(crate) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            error_kind: AtomicU8::new(0),
            notify: Notify::new(),
            operation: Mutex::new(None),
        }
    }

    pub(crate) fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
            if let Some(op) = self.operation.lock().expect("operation mutex").as_ref() {
                op.cancel();
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub(crate) async fn cancelled(&self) {
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
pub(crate) struct CancelledError;

impl fmt::Display for CancelledError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CANCELLED_MESSAGE)
    }
}

impl std::error::Error for CancelledError {}

// Raised when polling the DataFusion stream panics — most plausibly inside a
// foreign FFI table provider's execution. The panic is contained before it can
// unwind across the Arrow C stream callback into Go, and reported as a terminal
// stream error instead.
#[derive(Debug)]
pub(crate) struct ReaderPanicError;

impl fmt::Display for ReaderPanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("panic while reading query results across datafusion-go native boundary")
    }
}

impl std::error::Error for ReaderPanicError {}

pub(crate) struct StreamingReader {
    // Hold Inner so the runtime and SessionContext stay alive for every C stream
    // callback. The Go side may close statements/connections while rows remain.
    pub(crate) inner: Arc<Inner>,
    pub(crate) schema: SchemaRef,
    pub(crate) stream: SendableRecordBatchStream,
    pub(crate) cancel: Arc<CancelToken>,
    // Once Arrow sees EOF or an error, the C stream must remain terminal.
    pub(crate) done: bool,
}

impl Iterator for StreamingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.cancel.is_cancelled() {
            self.cancel.error_kind.store(1, Ordering::SeqCst);
            self.done = true;
            return Some(Err(cancelled_arrow_error()));
        }

        let cancel = self.cancel.clone();
        let stream = &mut self.stream;
        let runtime = &self.inner.runtime;
        // Arrow's C stream API is pull-based and synchronous. DataFusion's
        // stream is async, so each pull blocks this crate's runtime until either
        // the next batch arrives or cancellation wins the select.
        //
        // Polling runs the table provider's execution, which for a foreign FFI
        // provider is arbitrary producer code. A panic there must not unwind
        // across the Arrow C stream callback into Go (undefined behavior), so it
        // is contained here and reported as a terminal error, mirroring the
        // `run_ffi` guard on the synchronous entry points.
        let polled = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::select! {
                    _ = cancel.cancelled() => Some(Err(cancelled_datafusion_error())),
                    item = stream.next() => item,
                }
            })
        }));

        let next = match polled {
            Ok(next) => next,
            Err(_) => {
                self.cancel.error_kind.store(2, Ordering::SeqCst);
                self.done = true;
                return Some(Err(ArrowError::ExternalError(Box::new(ReaderPanicError))));
            }
        };

        match next {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => {
                self.cancel.error_kind.store(
                    if self.cancel.is_cancelled() { 1 } else { 3 },
                    Ordering::SeqCst,
                );
                self.done = true;
                Some(Err(ArrowError::ExternalError(Box::new(
                    // Arrow's C callback puts this text in a CString and
                    // aborts on interior NULs, including values in cast errors.
                    std::io::Error::other(err.to_string().replace('\0', "\\0")),
                ))))
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

pub(crate) fn cancelled_datafusion_error() -> DataFusionError {
    DataFusionError::Execution(CANCELLED_MESSAGE.to_owned())
}

pub(crate) fn cancelled_arrow_error() -> ArrowError {
    ArrowError::ExternalError(Box::new(CancelledError))
}

// Plan and execute with owned per-call bindings, then transfer the reader to
// an Arrow C stream while retaining the session for future callbacks.
pub(crate) fn execute_to_stream(
    inner: Arc<Inner>,
    query: &str,
    cached: Option<&crate::query::CachedStatement>,
    params: &ParameterMetadata,
    bindings: Vec<Binding>,
    cancel: Arc<CancelToken>,
) -> Result<FFI_ArrowArrayStream, FfiError> {
    // Validate argument shape before any catalog or session mutation.
    let values = param_values(params, bindings)?;
    let stream = inner.runtime.block_on(async {
        // Check cancellation around both planning and execution. DataFusion may
        // still do CPU work between await points, but these gates keep canceled
        // contexts from starting avoidable work and make streaming reads stop.
        let mut state = inner.ctx.state();
        let operation = cancel.operation.lock().expect("operation mutex").clone();
        if let Some(op) = &operation {
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(crate::callbacks::QueryOperation(op.clone()));
        }
        let plan = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FfiError::cancelled()),
            plan = async {
                let cached = cached.filter(|c| c.parser_settings == format!("{:?}", state.config_options().sql_parser));
                if cached.is_some() || (operation.is_some() && crate::go_catalog::has_remote(&state)) {
                    let statement = match cached {
                        Some(c) => c.statement.clone(),
                        None => state.sql_to_statement(query, &state.config_options().sql_parser.dialect)?,
                    };
                    if let Some(op) = &operation && crate::go_catalog::has_remote(&state) { crate::go_catalog::resolve(&mut state, &statement, op.clone()).await?; }
                    state.statement_to_plan(statement).await
                } else { state.create_logical_plan(query).await }
            } => plan.map_err(FfiError::from)?,
        };

        let plan = if let Some(values) = values {
            plan.with_param_values(values).map_err(FfiError::from)?
        } else {
            plan
        };

        // SessionContext::sql executes DDL immediately. Bind the logical plan
        // first so CREATE TABLE/VIEW never execute with unresolved parameters.
        let df = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FfiError::cancelled()),
            df = async {
                // CTAS executes its input inside execute_logical_plan, before a
                // DataFrame is returned. Give that execution its query context
                // while retaining the shared catalog. SET/CREATE FUNCTION keep
                // using the original session so mutations retain prior semantics.
                if let Some(op) = &operation && matches!(&plan, datafusion::logical_expr::LogicalPlan::Ddl(datafusion::logical_expr::DdlStatement::CreateMemoryTable(_))) {
                    let mut execution_state = inner.ctx.state();
                    execution_state.config_mut().options_mut().extensions.insert(crate::callbacks::QueryOperation(op.clone()));
                    datafusion::prelude::SessionContext::new_with_state(execution_state).execute_logical_plan(plan).await
                } else { inner.ctx.execute_logical_plan(plan).await }
            } => df.map_err(FfiError::from)?,
        };

        let df = if let Some(op) = operation {
            let (mut state, plan) = df.into_parts();
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(crate::callbacks::QueryOperation(op));
            datafusion::dataframe::DataFrame::new(state, plan)
        } else { df };
        tokio::select! {
            biased;
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

#[cfg(test)]
mod tests;
