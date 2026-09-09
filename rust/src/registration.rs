//! Consume Arrow input into session-owned in-memory tables.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use datafusion::datasource::MemTable;

use crate::error::FfiError;
use crate::session::Inner;

pub(crate) fn register_record_batches(
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

pub(crate) fn ipc_batches(data: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), FfiError> {
    // StreamReader copies message bodies into Rust-owned Arrow buffers. Only
    // the synchronous decoder borrows the input during this FFI call; returned
    // batches never borrow it, so a second full IPC copy is unnecessary.
    let mut reader = StreamReader::try_new(Cursor::new(data), None)?;
    let schema = reader.schema();
    let batches = reader.by_ref().collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((schema, batches))
}

pub(crate) fn arrow_stream_batches(
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
