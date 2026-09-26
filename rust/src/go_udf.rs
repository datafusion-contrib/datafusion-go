//! Vectorized Go functions, scheduled off DataFusion workers with bounded concurrency.
use crate::callbacks::{Owner, QueryOperation, blocking_call};
use crate::registration::ipc_batches;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
struct GoUdf {
    owner: Arc<Owner>,
    name: String,
    signature: Signature,
    arguments: SchemaRef,
    result: DataType,
}
impl PartialEq for GoUdf {
    fn eq(&self, other: &Self) -> bool {
        self.owner.handle == other.owner.handle
    }
}
impl Eq for GoUdf {}
impl Hash for GoUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner.handle.hash(state)
    }
}
pub(crate) fn create(
    owner: Arc<Owner>,
    name: String,
    volatility: Volatility,
) -> Result<datafusion::logical_expr::ScalarUDF> {
    let (bytes, _) = owner.call(0, 6, &[])?;
    let (schema, _) = ipc_batches(&bytes).map_err(|e| DataFusionError::Execution(e.message))?;
    if schema.fields().is_empty() {
        return Err(DataFusionError::Execution(
            "Go UDF signature lacks result".into(),
        ));
    }
    let n = schema.fields().len() - 1;
    let arguments = Arc::new(Schema::new(schema.fields()[..n].to_vec()));
    let result = schema.field(n).data_type().clone();
    let signature = Signature::exact(
        arguments
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect(),
        volatility,
    );
    Ok(AsyncScalarUDF::new(Arc::new(GoUdf {
        owner,
        name,
        signature,
        arguments,
        result,
    }))
    .into_scalar_udf())
}
impl ScalarUDFImpl for GoUdf {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.result.clone())
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Execution(
            "Go UDF requires async execution".into(),
        ))
    }
}
#[async_trait]
impl AsyncScalarUDFImpl for GoUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let op = args
            .config_options
            .extensions
            .get::<QueryOperation>()
            .ok_or_else(|| DataFusionError::Execution("missing Go UDF query context".into()))?
            .0
            .clone();
        let (_, handle) = self.owner.call(op.handle, 1, &[])?;
        let op = self.owner.child(handle);
        let rows = args.number_rows;
        let arrays = args
            .args
            .into_iter()
            .map(|v| v.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.arguments.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let batch = if self.owner.callbacks.version >= 2 {
            crate::callback_arrow::batch_call(self.owner.clone(), op, Some(batch))
                .await?
                .ok_or_else(|| DataFusionError::Execution("missing Go UDF output".into()))?
        } else {
            let mut data = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut data, &self.arguments)?;
                writer.write(&batch)?;
                writer.finish()?;
            }
            let (bytes, _) = blocking_call(self.owner.clone(), op, 7, data).await?;
            let (_, mut batches) =
                ipc_batches(&bytes).map_err(|e| DataFusionError::Execution(e.message))?;
            if batches.len() != 1 {
                return Err(DataFusionError::Execution("invalid Go UDF output".into()));
            }
            batches.pop().expect("one batch")
        };
        if batch.num_columns() != 1
            || batch.num_rows() != rows
            || batch.column(0).data_type() != &self.result
        {
            return Err(DataFusionError::Execution(
                "Go UDF output type or length mismatch".into(),
            ));
        }
        Ok(ColumnarValue::Array(batch.column(0).clone()))
    }
}
