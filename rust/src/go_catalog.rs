//! Per-query remote table resolution and optional lazy metadata discovery.
use crate::callbacks::{Owner, blocking_call, blocking_work};
use crate::go_provider::GoTable;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::TableType;
use datafusion_sql::parser::Statement;
use datafusion_sql::sqlparser::ast::Statement as SQLStatement;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::OnceCell;

#[derive(Debug)]
pub(crate) struct GoCatalog {
    pub(crate) owner: Arc<Owner>,
    pub(crate) discoverable: bool,
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
    let discovery = if state.config().information_schema() {
        discovery_for(
            statement,
            &references,
            &state.config_options().catalog.default_schema,
        )
    } else {
        Discovery::None
    };
    let list = Arc::new(MemoryCatalogProviderList::new());
    for name in state.catalog_list().catalog_names() {
        let Some(catalog) = state.catalog_list().catalog(&name) else {
            continue;
        };
        let Some(remote) = catalog.downcast_ref::<GoCatalog>() else {
            list.register_catalog(name, catalog);
            continue;
        };
        if remote.discoverable && discovery != Discovery::None {
            let snapshot = discover(
                state,
                remote,
                &name,
                &references,
                operation.clone(),
                discovery,
            )
            .await?;
            list.register_catalog(name, snapshot);
            continue;
        }
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
            if schema_name == "information_schema" && state.config().information_schema() {
                continue;
            }
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
            if let Some(table) = resolve_table(
                remote.owner.clone(),
                operation.clone(),
                schema_name,
                reference.table(),
            )
            .await?
            {
                schema.register_table(reference.table().to_owned(), table)?;
            }
        }
        list.register_catalog(name, snapshot);
    }
    state.register_catalog_list(list);
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Discovery {
    None,
    Schemas,
    Tables,
}

fn discovery_for(
    statement: &Statement,
    references: &[TableReference],
    default_schema: &str,
) -> Discovery {
    // DataFusion synthesizes *all* information_schema references for SHOW,
    // including SHOW settings/functions, which must not cause remote I/O.
    match statement {
        Statement::Explain(explain) => {
            return discovery_for(&explain.statement, references, default_schema);
        }
        Statement::Statement(sql) => match sql.as_ref() {
            SQLStatement::ShowTables { .. }
            | SQLStatement::ShowColumns { .. }
            | SQLStatement::ShowCreate { .. } => return Discovery::Tables,
            SQLStatement::ShowVariable { .. }
            | SQLStatement::ShowVariables { .. }
            | SQLStatement::ShowFunctions { .. }
            | SQLStatement::ShowStatus { .. }
            | SQLStatement::ShowCollation { .. } => return Discovery::None,
            _ => {}
        },
        _ => {}
    }
    references
        .iter()
        .filter(|r| r.schema().unwrap_or(default_schema) == "information_schema")
        .map(|r| match r.table().to_ascii_lowercase().as_str() {
            "schemata" => Discovery::Schemas,
            "tables" | "columns" | "views" => Discovery::Tables,
            _ => Discovery::None,
        })
        .max()
        .unwrap_or(Discovery::None)
}

async fn names(
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    schema: Option<&str>,
) -> Result<Vec<String>> {
    let (opcode, input) = match schema {
        Some(name) => (
            15,
            serde_json::to_vec(name).map_err(|e| DataFusionError::Execution(e.to_string()))?,
        ),
        None => (14, vec![]),
    };
    let (bytes, _) = blocking_call(owner, operation, opcode, input).await?;
    serde_json::from_slice(&bytes).map_err(|e| DataFusionError::Execution(e.to_string()))
}

async fn resolve_table(
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    schema: &str,
    table: &str,
) -> Result<Option<Arc<dyn TableProvider>>> {
    let request = serde_json::to_vec(&serde_json::json!({"schema":schema,"table":table}))
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let (bytes, table) = blocking_call(owner, operation.clone(), 8, request).await?;
    let Some(table) = table else {
        return Ok(None);
    };
    let capabilities = bytes
        .first()
        .map(|b| i32::from(*b) - i32::from(b'0'))
        .unwrap_or(0);
    // Schema callbacks also run off the executor. The returned provider retains
    // its handle after this task ends, including when result consumption is lazy.
    let provider = blocking_work(table.clone(), operation, move |_, _| {
        GoTable::new(table, capabilities)
    })
    .await?;
    Ok(Some(Arc::new(provider)))
}

type ResolvedTable = OnceCell<Option<Arc<dyn TableProvider>>>;

#[derive(Debug)]
struct DiscoveredSchema {
    owner: Arc<Owner>,
    operation: Arc<Owner>,
    name: String,
    tables: BTreeMap<String, ResolvedTable>,
}

#[async_trait]
impl SchemaProvider for DiscoveredSchema {
    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        // Go TableProvider currently exposes base tables only. Listing names
        // needs neither ResolveTable nor Schema/Scan calls for every table.
        Ok(self.tables.contains_key(name).then_some(TableType::Base))
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(cell) = self.tables.get(name) else {
            return Ok(None);
        };
        cell.get_or_try_init(|| {
            resolve_table(self.owner.clone(), self.operation.clone(), &self.name, name)
        })
        .await
        .cloned()
    }
}

async fn discover(
    state: &SessionState,
    remote: &GoCatalog,
    catalog: &str,
    references: &[TableReference],
    operation: Arc<Owner>,
    mode: Discovery,
) -> Result<Arc<MemoryCatalogProvider>> {
    let mut schemas: BTreeSet<String> = names(remote.owner.clone(), operation.clone(), None)
        .await?
        .into_iter()
        .collect();
    let config = &state.config_options().catalog;
    let direct: Vec<_> = references
        .iter()
        .filter(|r| r.catalog().unwrap_or(&config.default_catalog) == catalog)
        .filter(|r| r.schema().unwrap_or(&config.default_schema) != "information_schema")
        .collect();
    // A listing can be incomplete or race with a new table. Direct references
    // continue to resolve even if a catalog did not list them.
    schemas.extend(
        direct
            .iter()
            .map(|r| r.schema().unwrap_or(&config.default_schema).to_owned()),
    );
    schemas.remove("information_schema");
    let snapshot = Arc::new(MemoryCatalogProvider::new());
    for schema in schemas {
        let mut tables: BTreeSet<String> = if mode == Discovery::Tables {
            names(remote.owner.clone(), operation.clone(), Some(&schema))
                .await?
                .into_iter()
                .collect()
        } else {
            BTreeSet::new()
        };
        tables.extend(
            direct
                .iter()
                .filter(|r| r.schema().unwrap_or(&config.default_schema) == schema)
                .map(|r| r.table().to_owned()),
        );
        let provider = DiscoveredSchema {
            owner: remote.owner.clone(),
            operation: operation.clone(),
            name: schema.clone(),
            tables: tables
                .into_iter()
                .map(|name| (name, OnceCell::new()))
                .collect(),
        };
        snapshot.register_schema(&schema, Arc::new(provider))?;
    }
    Ok(snapshot)
}
pub(crate) fn has_remote(state: &SessionState) -> bool {
    state.catalog_list().catalog_names().iter().any(|name| {
        state
            .catalog_list()
            .catalog(name)
            .is_some_and(|c| c.is::<GoCatalog>())
    })
}
