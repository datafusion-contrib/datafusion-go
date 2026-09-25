//! Lazy, cancellable Go tables. Plans retain callback owners, never raw handles.
use crate::callbacks::{Owner, QueryOperation, blocking_call};
use crate::pushdown;
use crate::registration::ipc_batches;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GoTable {
    owner: Arc<Owner>,
    schema: SchemaRef,
    pushdown: bool,
    writable: bool,
}
impl GoTable {
    pub(crate) fn new(owner: Arc<Owner>, capabilities: i32) -> Result<Self> {
        let (bytes, _) = owner.call(0, 3, &[])?;
        let (schema, _) = ipc_batches(&bytes).map_err(|e| DataFusionError::Execution(e.message))?;
        Ok(Self {
            owner,
            schema,
            pushdown: capabilities & 1 != 0,
            writable: capabilities & 2 != 0,
        })
    }
}
#[async_trait]
impl TableProvider for GoTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.pushdown && pushdown::classify(f, &self.schema).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        operation: datafusion::logical_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.writable {
            return Err(DataFusionError::NotImplemented(
                "Go table does not support INSERT".into(),
            ));
        }
        use datafusion::common::SchemaExt;
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        Ok(Arc::new(datafusion::datasource::sink::DataSinkExec::new(
            input,
            Arc::new(GoSink {
                owner: self.owner.clone(),
                schema: self.schema.clone(),
                operation,
            }),
            None,
        )))
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        let filters = pushdown::filters_to_json(filters, &self.schema)
            .map(|s| serde_json::from_str::<serde_json::Value>(s.to_str().expect("JSON UTF-8")))
            .transpose()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let request=serde_json::to_vec(&serde_json::json!({"projection":projection,"filters":filters,"limit":limit.map(|n|n as i64).unwrap_or(-1)})).map_err(|e|DataFusionError::Execution(e.to_string()))?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(GoExec {
            owner: self.owner.clone(),
            schema,
            properties,
            request,
            projection: if self.pushdown {
                None
            } else {
                projection.cloned()
            },
        }))
    }
}
#[derive(Debug)]
struct GoExec {
    owner: Arc<Owner>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    request: Vec<u8>,
    projection: Option<Vec<usize>>,
}
impl DisplayAs for GoExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GoTableExec")
    }
}
impl ExecutionPlan for GoExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "GoTableExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "Go table has no children".into(),
            ))
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "invalid Go scan partition".into(),
            ));
        }
        let op = context
            .session_config()
            .options()
            .extensions
            .get::<QueryOperation>()
            .ok_or_else(|| DataFusionError::Execution("missing Go query context".into()))?
            .0
            .clone();
        let (_, handle) = self.owner.call(op.handle, 1, &[])?;
        let operation = self.owner.child(handle);
        let state = ScanState {
            owner: self.owner.clone(),
            operation,
            reader: None,
            request: self.request.clone(),
            schema: self.schema.clone(),
            projection: self.projection.clone(),
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            if state.reader.is_none() {
                let (bytes, handle) = blocking_call(
                    state.owner.clone(),
                    state.operation.clone(),
                    4,
                    state.request.clone(),
                )
                .await?;
                let reader = handle.ok_or_else(|| {
                    DataFusionError::Execution("Go scan returned no reader handle".into())
                })?;
                let (schema, _) =
                    ipc_batches(&bytes).map_err(|e| DataFusionError::Execution(e.message))?;
                let projected = match &state.projection {
                    Some(p) => schema.project(p)?,
                    None => schema.as_ref().clone(),
                };
                if projected != *state.schema {
                    return Err(DataFusionError::Execution(
                        "Go provider returned incorrect projected schema".into(),
                    ));
                }
                state.reader = Some(reader);
            }
            let (bytes, _) = blocking_call(
                state.reader.as_ref().expect("opened reader").clone(),
                state.operation.clone(),
                5,
                Vec::new(),
            )
            .await?;
            if bytes.is_empty() {
                return Ok(None);
            }
            let (_, mut batches) =
                ipc_batches(&bytes).map_err(|e| DataFusionError::Execution(e.message))?;
            if batches.len() != 1 {
                return Err(DataFusionError::Execution(
                    "Go reader must return one batch".into(),
                ));
            }
            let batch = batches.pop().expect("one batch");
            let batch = match &state.projection {
                Some(p) => batch.project(p)?,
                None => batch,
            };
            if batch.schema() != state.schema {
                return Err(DataFusionError::Execution(
                    "Go reader schema changed".into(),
                ));
            }
            Ok(Some((batch, state)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
struct ScanState {
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    reader: Option<Arc<Owner>>,
    request: Vec<u8>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
}
impl Drop for ScanState {
    fn drop(&mut self) {
        self.operation.cancel();
    }
}

#[derive(Debug)]
struct GoSink {
    owner: Arc<Owner>,
    schema: SchemaRef,
    operation: datafusion::logical_expr::dml::InsertOp,
}
impl DisplayAs for GoSink {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GoDataSink")
    }
}
struct InputReader {
    schema: SchemaRef,
    rx: tokio::sync::mpsc::Receiver<Option<Result<datafusion::arrow::record_batch::RecordBatch>>>,
    finished: bool,
}
impl Iterator for InputReader {
    type Item = std::result::Result<
        datafusion::arrow::record_batch::RecordBatch,
        datafusion::arrow::error::ArrowError,
    >;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let item = match self.rx.blocking_recv() {
            Some(Some(item)) => item,
            Some(None) => {
                self.finished = true;
                return None;
            }
            None => {
                self.finished = true;
                Err(DataFusionError::Execution(
                    "INSERT input ended without completion (canceled or panicked)".into(),
                ))
            }
        };
        Some(item.map_err(|e| {
            datafusion::arrow::error::ArrowError::ExternalError(Box::new(std::io::Error::other(
                e.to_string().replace('\0', "\\0"),
            )))
        }))
    }
}
impl datafusion::arrow::array::RecordBatchReader for InputReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
struct Pump(tokio::task::JoinHandle<()>);
impl Drop for Pump {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[async_trait]
impl datafusion::datasource::sink::DataSink for GoSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    async fn write_all(
        &self,
        mut input: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
        use futures::StreamExt;
        let root = context
            .session_config()
            .options()
            .extensions
            .get::<QueryOperation>()
            .ok_or_else(|| DataFusionError::Execution("missing Go insert context".into()))?
            .0
            .clone();
        let (_, handle) = self.owner.call(root.handle, 1, &[])?;
        let operation = self.owner.child(handle);
        let _cancel = crate::callbacks::CancelOnDrop(Some(operation.clone()));
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let mut pump = Pump(tokio::spawn(async move {
            while let Some(item) = input.next().await {
                if tx.send(Some(item)).await.is_err() {
                    return;
                }
            }
            // Only a real upstream EOF is successful completion. An aborted or
            // panicked producer closes the channel without this terminal marker.
            let _ = tx.send(None).await;
        }));
        static WRITERS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        let permit = WRITERS
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(64)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let owner = self.owner.clone();
        let schema = self.schema.clone();
        let code = match self.operation {
            datafusion::logical_expr::dml::InsertOp::Append => 9,
            datafusion::logical_expr::dml::InsertOp::Overwrite => 10,
            datafusion::logical_expr::dml::InsertOp::Replace => 11,
        };
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut stream = FFI_ArrowArrayStream::new(Box::new(InputReader {
                schema,
                rx,
                finished: false,
            }));
            // SAFETY: the stack stream is exclusively lent to this synchronous Go call;
            // importing it moves its release callback. Go releases the reader on return.
            let (bytes, _) = unsafe {
                owner.call_raw(
                    operation.handle,
                    code,
                    (&mut stream as *mut FFI_ArrowArrayStream).cast(),
                    std::mem::size_of::<FFI_ArrowArrayStream>(),
                )
            }?;
            String::from_utf8_lossy(&bytes)
                .parse::<u64>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        })
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        // A provider may return early. Abort a pending upstream pull as well as
        // a blocked send; never await a source that the consumer abandoned.
        pump.0.abort();
        if let Err(error) = (&mut pump.0).await
            && !error.is_cancelled()
        {
            return Err(DataFusionError::Execution(error.to_string()));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};

    #[test]
    fn insert_input_requires_explicit_completion() {
        // A producer panic/abort and a successful EOF both drop the sender.
        // Only the explicit terminal message may let a provider commit.
        for completed in [false, true] {
            let schema = Arc::new(Schema::empty());
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.blocking_send(Some(Ok(RecordBatch::new_empty(schema.clone()))))
                .unwrap();
            if completed {
                tx.blocking_send(None).unwrap();
            }
            drop(tx);
            let mut reader = InputReader {
                schema,
                rx,
                finished: false,
            };
            assert!(reader.next().unwrap().is_ok());
            if completed {
                assert!(reader.next().is_none());
            } else {
                assert!(
                    reader
                        .next()
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("without completion")
                );
            }
            assert!(reader.next().is_none());
        }
    }
}
