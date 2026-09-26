//! C Data transport for callback batches. Go outputs own C-allocated buffers;
//! native UDF arguments can be borrowed without serialization. The exchange is
//! created and consumed inside the blocking closure, so dropping its future
//! cannot free an in-flight input or leak an unobserved output.
use crate::callbacks::{Owner, blocking_work};
use datafusion::arrow::array::{Array, StructArray};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use std::sync::Arc;

#[repr(C)]
pub(crate) struct ArrowExchange {
    pub(crate) input: FFI_ArrowArray,
    pub(crate) input_schema: FFI_ArrowSchema,
    pub(crate) output: FFI_ArrowArray,
    pub(crate) output_schema: FFI_ArrowSchema,
}

pub(crate) async fn batch_call(
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    input: Option<RecordBatch>,
) -> Result<Option<RecordBatch>> {
    blocking_work(owner, operation, move |owner, operation| {
        let opcode = if input.is_some() { 13 } else { 12 };
        let mut exchange = ArrowExchange {
            input: FFI_ArrowArray::empty(),
            input_schema: FFI_ArrowSchema::empty(),
            output: FFI_ArrowArray::empty(),
            output_schema: FFI_ArrowSchema::empty(),
        };
        if let Some(batch) = input {
            exchange.input_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())?;
            exchange.input = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
        }
        // SAFETY: exchange is initialized, exclusively borrowed for the callback,
        // and dropped here on every exit. The v2 contract moves input ownership
        // and returns C-owned output with Arrow release callbacks.
        let (bytes, _) = unsafe {
            owner.call_raw(
                operation,
                opcode,
                (&mut exchange as *mut ArrowExchange).cast(),
                std::mem::size_of::<ArrowExchange>(),
            )?
        };
        if !bytes.is_empty() {
            let (_, mut batches) = crate::registration::ipc_batches(&bytes)
                .map_err(|e| DataFusionError::Execution(e.message))?;
            if batches.len() != 1 {
                return Err(DataFusionError::Execution("invalid fallback batch".into()));
            }
            return Ok(batches.pop());
        }
        if exchange.output.is_released() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::try_from(&exchange.output_schema)?);
        let output = std::mem::replace(&mut exchange.output, FFI_ArrowArray::empty());
        // SAFETY: a successful v2 callback exported a valid Arrow struct array.
        let data = unsafe { from_ffi(output, &exchange.output_schema)? };
        if !matches!(
            data.data_type(),
            datafusion::arrow::datatypes::DataType::Struct(_)
        ) {
            return Err(DataFusionError::Execution(
                "Go callback output is not a record batch".into(),
            ));
        }
        let batch = RecordBatch::from(StructArray::from(data)).with_schema(schema)?;
        Ok(Some(batch))
    })
    .await
}
