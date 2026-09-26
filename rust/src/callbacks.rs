//! Versioned callbacks owned by the Go executable, compatible with dlopen and static links.
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::common::{DataFusionError, Result};
use std::any::Any;
use std::ffi::c_void;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

pub(crate) const RELEASE: i32 = 0;
pub(crate) const CANCEL: i32 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Callbacks {
    pub(crate) version: u64,
    pub(crate) invoke: unsafe extern "C" fn(
        u64,
        u64,
        i32,
        *const u8,
        i64,
        *mut *mut u8,
        *mut i64,
        *mut u64,
    ) -> i32,
    pub(crate) free_bytes: unsafe extern "C" fn(*mut u8),
}

#[derive(Debug)]
pub(crate) struct Owner {
    pub(crate) handle: u64,
    pub(crate) callbacks: Callbacks,
}

impl Owner {
    // SAFETY: the registration entry point requires a live v1/v2 callback table.
    pub(crate) unsafe fn new(handle: u64, callbacks: *const c_void) -> Result<Arc<Self>> {
        if callbacks.is_null() || handle == 0 {
            return Err(DataFusionError::Execution(
                "invalid Go callback registration".into(),
            ));
        }
        let callbacks = unsafe { *(callbacks as *const Callbacks) };
        if callbacks.version != 1 && callbacks.version != 2 {
            return Err(DataFusionError::Execution(
                "unsupported Go callback ABI".into(),
            ));
        }
        Ok(Arc::new(Self { handle, callbacks }))
    }
    pub(crate) fn child(&self, handle: u64) -> Arc<Self> {
        Arc::new(Self {
            handle,
            callbacks: self.callbacks,
        })
    }
    pub(crate) fn call(&self, operation: u64, opcode: i32, input: &[u8]) -> Result<(Vec<u8>, u64)> {
        // SAFETY: ordinary callback inputs are borrowed read-only for this call.
        unsafe { self.call_raw(operation, opcode, input.as_ptr(), input.len()) }
    }
    // INSERT and v2 Arrow exchange inputs are borrowed mutable structs which Go
    // consumes by moving their callbacks. No immutable Rust reference is created.
    pub(crate) unsafe fn call_raw(
        &self,
        operation: u64,
        opcode: i32,
        input: *const u8,
        input_len: usize,
    ) -> Result<(Vec<u8>, u64)> {
        let mut data = std::ptr::null_mut();
        let mut len = 0;
        let mut handle = 0;
        // SAFETY: callbacks remain loaded for process lifetime. Inputs are borrowed
        // synchronously; outputs are copied before returning to their allocator.
        let status = unsafe {
            (self.callbacks.invoke)(
                self.handle,
                operation,
                opcode,
                input,
                input_len as i64,
                &mut data,
                &mut len,
                &mut handle,
            )
        };
        let bytes = if len >= 0 && (!data.is_null() || len == 0) {
            if len == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(data, len as usize).to_vec() }
            }
        } else {
            Vec::new()
        };
        unsafe {
            (self.callbacks.free_bytes)(data);
        }
        if status != 0 {
            return Err(DataFusionError::Execution(
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }
        if len < 0 || (data.is_null() && len != 0) {
            return Err(DataFusionError::Execution(
                "invalid Go callback output".into(),
            ));
        }
        Ok((bytes, handle))
    }
    pub(crate) fn cancel(&self) {
        let _ = self.call(0, CANCEL, &[]);
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.call(0, RELEASE, &[]);
    }
}

// A dropped future cancels Go I/O, but the blocking closure retains every owner
// and its concurrency permit until it actually returns. Cancellation never frees
// a handle under an in-flight callback.
pub(crate) struct CancelOnDrop(pub(crate) Option<Arc<Owner>>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(op) = &self.0 {
            op.cancel();
        }
    }
}
pub(crate) async fn blocking_call(
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    opcode: i32,
    input: Vec<u8>,
) -> Result<(Vec<u8>, Option<Arc<Owner>>)> {
    blocking_work(owner, operation, move |owner, operation| {
        owner
            .call(operation, opcode, &input)
            .map(|(bytes, handle)| {
                (
                    bytes,
                    if handle == 0 {
                        None
                    } else {
                        Some(owner.child(handle))
                    },
                )
            })
    })
    .await
}

pub(crate) async fn blocking_work<T: Send + 'static>(
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    work: impl FnOnce(&Owner, u64) -> Result<T> + Send + 'static,
) -> Result<T> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let mut guard = CancelOnDrop(Some(operation.clone()));
    let permit = SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(64)))
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work(&owner, operation.handle)
    })
    .await
    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    guard.0 = None;
    result
}

#[derive(Clone, Debug)]
pub(crate) struct QueryOperation(pub(crate) Arc<Owner>);
impl ConfigExtension for QueryOperation {
    const PREFIX: &'static str = "dfgo_operation";
}
impl ExtensionOptions for QueryOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, _: &str, _: &str) -> Result<()> {
        Err(DataFusionError::Execution(
            "query operation is internal".into(),
        ))
    }
    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}
