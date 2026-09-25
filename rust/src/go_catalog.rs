//! Resolve only referenced remote tables, once per query, before synchronous planning.
use crate::callbacks::{Owner, blocking_call};
use crate::go_provider::GoTable;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider, SchemaProvider,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion_sql::parser::Statement;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GoCatalog {
    pub(crate) owner: Arc<Owner>,
}
impl CatalogProvider for GoCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }
    fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
        None
    }
}
pub(crate) async fn resolve(
    state: &mut SessionState,
    statement: &Statement,
    operation: Arc<Owner>,
) -> Result<()> {
    let references = state.resolve_table_references(statement)?;
    let list = Arc::new(MemoryCatalogProviderList::new());
    for name in state.catalog_list().catalog_names() {
        let Some(catalog) = state.catalog_list().catalog(&name) else {
            continue;
        };
        let Some(remote) = catalog.downcast_ref::<GoCatalog>() else {
            list.register_catalog(name, catalog);
            continue;
        };
        let snapshot = Arc::new(MemoryCatalogProvider::new());
        for reference in &references {
            if reference
                .catalog()
                .unwrap_or(&state.config_options().catalog.default_catalog)
                != name
            {
                continue;
            }
            let schema_name = reference
                .schema()
                .unwrap_or(&state.config_options().catalog.default_schema);
            let schema = if let Some(schema) = snapshot.schema(schema_name) {
                schema
            } else {
                let schema = Arc::new(MemorySchemaProvider::new());
                snapshot.register_schema(schema_name, schema.clone())?;
                schema
            };
            if schema.table_exist(reference.table()) {
                continue;
            }
            let request = serde_json::to_vec(
                &serde_json::json!({"schema":schema_name,"table":reference.table()}),
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let (bytes, table) =
                blocking_call(remote.owner.clone(), operation.clone(), 8, request).await?;
            if let Some(owner) = table {
                let capabilities = bytes
                    .first()
                    .map(|b| i32::from(*b) - i32::from(b'0'))
                    .unwrap_or(0);
                schema.register_table(
                    reference.table().to_owned(),
                    Arc::new(GoTable::new(owner, capabilities)?),
                )?;
            }
        }
        list.register_catalog(name, snapshot);
    }
    state.register_catalog_list(list);
    Ok(())
}
pub(crate) fn has_remote(state: &SessionState) -> bool {
    state.catalog_list().catalog_names().iter().any(|name| {
        state
            .catalog_list()
            .catalog(name)
            .is_some_and(|c| c.is::<GoCatalog>())
    })
}
