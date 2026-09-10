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
