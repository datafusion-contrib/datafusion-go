use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use super::*;

#[test]
fn failure_and_cancellation_at_every_batch() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::atomic::{AtomicUsize, Ordering};

    for cancel in [false, true] {
        for fail_at in 0..=4 {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let inner = Arc::new(Inner {
                runtime: Arc::new(Runtime::new().unwrap()),
                ctx: SessionContext::new(),
            });
            let session_lifetime = Arc::downgrade(&inner);
            let runtime_lifetime = Arc::downgrade(&inner.runtime);
            let polls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&polls);
            let input = futures::stream::iter((0..=4).map(move |step| {
                count.fetch_add(1, Ordering::SeqCst);
                if step == fail_at {
                    Err(DataFusionError::Execution(
                        "injected stream failure".to_owned(),
                    ))
                } else {
                    Ok(batch.clone())
                }
            }));
            let mut reader = StreamingReader {
                inner,
                schema: Arc::clone(&schema),
                stream: Box::pin(RecordBatchStreamAdapter::new(schema, input)),
                cancel: Arc::new(CancelToken::new()),
                done: false,
            };
            for _ in 0..fail_at {
                assert_eq!(reader.next().unwrap().unwrap().num_rows(), 3);
            }
            if cancel {
                reader.cancel.cancel();
            }
            let error = reader.next().unwrap().unwrap_err().to_string();
            assert!(error.contains(if cancel {
                CANCELLED_MESSAGE
            } else {
                "injected stream failure"
            }));
            let terminal_polls = polls.load(Ordering::SeqCst);
            assert!(reader.next().is_none());
            assert!(reader.next().is_none());
            assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
            drop(reader);
            assert!(session_lifetime.upgrade().is_none());
            assert!(runtime_lifetime.upgrade().is_none());
        }
    }
}

#[test]
fn panic_while_polling_stream_is_contained() {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    fn panicking_batch() -> Result<RecordBatch, DataFusionError> {
        panic!("boom from a foreign table provider scan");
    }

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "a",
        DataType::Int64,
        false,
    )]));
    // The panic fires when the stream is first polled, inside next().
    let panicking = stream::once(async { panicking_batch() });
    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        panicking,
    ));

    let mut reader = StreamingReader {
        inner: Arc::new(Inner {
            runtime: Arc::new(Runtime::new().expect("runtime")),
            ctx: SessionContext::new(),
        }),
        schema,
        stream,
        cancel: Arc::new(CancelToken::new()),
        done: false,
    };

    match reader.next() {
        Some(Err(ArrowError::ExternalError(err))) => {
            assert!(err.downcast_ref::<ReaderPanicError>().is_some());
        }
        other => panic!("expected a contained panic error, got {other:?}"),
    }
    // The reader is terminal after a contained panic and never re-polls.
    assert!(reader.next().is_none());
}
