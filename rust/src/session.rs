//! Session configuration and shared ownership of execution resources.

use std::sync::Arc;

use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

use crate::error::FfiError;

pub(crate) struct Inner {
    // DataFusion APIs are async, but the Arrow C stream callbacks are
    // synchronous. Keeping the runtime here lets callbacks block_on future work
    // while ensuring the runtime outlives any exported stream.
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) ctx: SessionContext,
}

pub(crate) fn session_config_from_dsn(dsn: &str) -> Result<SessionConfig, FfiError> {
    // Information schema is enabled by default because database/sql clients
    // commonly introspect columns and types after preparing or running queries.
    let mut config = SessionConfig::new().with_information_schema(true);
    config.options_mut().extensions.insert(driver_options(dsn)?);

    // Supported DSNs are intentionally lightweight: the query string, if any,
    // contains DataFusion configuration options. The URL host/path are ignored
    // by the Go layer before this point.
    let Some((_, query)) = dsn.split_once('?') else {
        return Ok(config);
    };

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key.is_empty()
            || matches!(
                key.as_ref(),
                "datafusion.go.runtime_workers"
                    | "datafusion.go.shared_runtime"
                    | "datafusion.go.cache_statements"
            )
        {
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

// Driver-only options are opt-in and never change DataFusion's defaults.
#[derive(Clone, Debug, Default)]
pub(crate) struct DriverOptions {
    pub(crate) cache_statements: bool,
    workers: usize,
    shared_runtime: bool,
}
impl datafusion::common::config::ConfigExtension for DriverOptions {
    const PREFIX: &'static str = "dfgo";
}
impl datafusion::common::config::ExtensionOptions for DriverOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn datafusion::common::config::ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, _: &str, _: &str) -> datafusion::common::Result<()> {
        Err(datafusion::common::DataFusionError::Execution(
            "driver options are configured at session creation".into(),
        ))
    }
    fn entries(&self) -> Vec<datafusion::common::config::ConfigEntry> {
        vec![]
    }
}
fn driver_options(dsn: &str) -> Result<DriverOptions, FfiError> {
    let mut options = DriverOptions::default();
    if let Some((_, query)) = dsn.split_once('?') {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            let invalid =
                || FfiError::invalid_argument(format!("invalid driver option {key}={value}"));
            match key.as_ref() {
                "datafusion.go.cache_statements" => {
                    options.cache_statements = value.parse().map_err(|_| invalid())?
                }
                "datafusion.go.shared_runtime" => {
                    options.shared_runtime = value.parse().map_err(|_| invalid())?
                }
                "datafusion.go.runtime_workers" => {
                    options.workers = value.parse().map_err(|_| invalid())?;
                    if options.workers == 0 {
                        return Err(invalid());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(options)
}
pub(crate) fn runtime_from_dsn(dsn: &str) -> Result<Arc<Runtime>, FfiError> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock, Weak};
    static SHARED: OnceLock<Mutex<HashMap<usize, Weak<Runtime>>>> = OnceLock::new();
    let options = driver_options(dsn)?;
    let build = || {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if options.workers != 0 {
            builder.worker_threads(options.workers);
        }
        builder
            .build()
            .map(Arc::new)
            .map_err(|e| FfiError::native(e.to_string()))
    };
    if !options.shared_runtime {
        return build();
    }
    let mut runtimes = SHARED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| FfiError::native("shared runtime lock poisoned"))?;
    runtimes.retain(|_, runtime| runtime.strong_count() != 0);
    if let Some(runtime) = runtimes.get(&options.workers).and_then(Weak::upgrade) {
        return Ok(runtime);
    }
    let runtime = build()?;
    runtimes.insert(options.workers, Arc::downgrade(&runtime));
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_sharing_is_opt_in_and_reference_counted() {
        let a = runtime_from_dsn("?datafusion.go.runtime_workers=1").unwrap();
        let b = runtime_from_dsn("?datafusion.go.runtime_workers=1").unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        let a =
            runtime_from_dsn("?datafusion.go.runtime_workers=1&datafusion.go.shared_runtime=true")
                .unwrap();
        let b =
            runtime_from_dsn("?datafusion.go.runtime_workers=1&datafusion.go.shared_runtime=true")
                .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let weak = Arc::downgrade(&a);
        drop(a);
        drop(b);
        assert!(weak.upgrade().is_none());
        assert!(runtime_from_dsn("?datafusion.go.runtime_workers=0").is_err());
        assert!(runtime_from_dsn("?datafusion.go.shared_runtime=maybe").is_err());
    }
}
