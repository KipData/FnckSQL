pub mod aggregate;
mod create_table;
mod delete;
mod distinct;
mod drop_table;
pub mod expr;
mod insert;
mod select;
mod show;
pub mod copy;
mod truncate;
mod update;

use sqlparser::ast::{Ident, ObjectName, ObjectType, SetExpr, Statement};
use std::collections::BTreeMap;

use crate::catalog::{CatalogError, TableCatalog, TableName, DEFAULT_SCHEMA_NAME};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::JoinType;
use crate::planner::LogicalPlan;
use crate::storage::Storage;
use crate::types::errors::TypeError;

pub enum InputRefType {
    AggCall,
    GroupBy,
}

#[derive(Clone)]
pub struct BinderContext<S: Storage> {
    pub(crate) storage: S,
    pub(crate) bind_table: BTreeMap<TableName, (TableCatalog, Option<JoinType>)>,
    aliases: BTreeMap<String, ScalarExpression>,
    group_by_exprs: Vec<ScalarExpression>,
    pub(crate) agg_calls: Vec<ScalarExpression>,
}

impl<S: Storage> BinderContext<S> {
    pub fn new(storage: S) -> Self {
        BinderContext {
            storage,
            bind_table: Default::default(),
            aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
        }
    }

    // Tips: The order of this index is based on Aggregate being bound first.
    pub fn input_ref_index(&self, ty: InputRefType) -> usize {
        match ty {
            InputRefType::AggCall => self.agg_calls.len(),
            InputRefType::GroupBy => self.agg_calls.len() + self.group_by_exprs.len(),
        }
    }

    pub fn add_alias(&mut self, alias: String, expr: ScalarExpression) {
        if self.aliases.contains_key(&alias) {
            return;
        }

        self.aliases.insert(alias, expr);
    }

    pub fn has_agg_call(&self, expr: &ScalarExpression) -> bool {
        self.group_by_exprs.contains(expr)
    }
}

pub struct Binder<S: Storage> {
    context: BinderContext<S>,
}

impl<S: Storage> Binder<S> {
    pub fn new(context: BinderContext<S>) -> Self {
        Binder { context }
    }

    pub async fn bind(mut self, stmt: &Statement) -> Result<LogicalPlan, BindError> {
        let plan = match stmt {
            Statement::Query(query) => self.bind_query(query).await?,
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => self.bind_create_table(name, &columns, &constraints)?,
            Statement::Drop {
                object_type, names, ..
            } => match object_type {
                ObjectType::Table => self.bind_drop_table(&names[0])?,
                _ => todo!(),
            },
            Statement::Insert {
                table_name,
                columns,
                source,
                overwrite,
                ..
            } => {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.bind_insert(table_name.to_owned(), columns, &values.rows, *overwrite)
                        .await?
                } else {
                    todo!()
                }
            }
            Statement::Update {
                table,
                selection,
                assignments,
                ..
            } => {
                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_update(table, selection, assignments).await?
                }
            }
            Statement::Delete {
                from, selection, ..
            } => {
                let table = &from[0];

                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_delete(table, selection).await?
                }
            }
            Statement::Truncate { table_name, .. } => self.bind_truncate(table_name).await?,
            Statement::ShowTables { .. } => self.bind_show_tables()?,
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source.clone(), *to, target.clone(), &options).await?,
            _ => return Err(BindError::UnsupportedStmt(stmt.to_string())),
        };
        Ok(plan)
    }
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect(),
    )
}

/// Split an object name into `(schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<(&str, &str), BindError> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("unsupported statement {0}")]
    UnsupportedStmt(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("invalid column {0}")]
    InvalidColumn(String),
    #[error("ambiguous column {0}")]
    AmbiguousColumn(String),
    #[error("binary operator types mismatch: {0} != {1}")]
    BinaryOpTypeMismatch(String, String),
    #[error("subquery in FROM must have an alias")]
    SubqueryMustHaveAlias,
    #[error("agg miss: {0}")]
    AggMiss(String),
    #[error("catalog error: {0}")]
    CatalogError(#[from] CatalogError),
    #[error("type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("copy error: {0}")]
    UnsupportedCopySource(String),
}

#[cfg(test)]
pub mod test {
    use crate::binder::{Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::ExecutorError;
    use crate::planner::LogicalPlan;
    use crate::storage::kip::KipStorage;
    use crate::storage::{Storage, StorageError};
    use crate::types::LogicalType::Integer;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) async fn build_test_catalog(
        path: impl Into<PathBuf> + Send,
    ) -> Result<KipStorage, StorageError> {
        let storage = KipStorage::new(path).await?;

        let _ = storage
            .create_table(
                Arc::new("t1".to_string()),
                vec![
                    ColumnCatalog::new(
                        "c1".to_string(),
                        false,
                        ColumnDesc::new(Integer, true, false),
                        None,
                    ),
                    ColumnCatalog::new(
                        "c2".to_string(),
                        false,
                        ColumnDesc::new(Integer, false, true),
                        None,
                    ),
                ],
            )
            .await?;

        let _ = storage
            .create_table(
                Arc::new("t2".to_string()),
                vec![
                    ColumnCatalog::new(
                        "c3".to_string(),
                        false,
                        ColumnDesc::new(Integer, true, false),
                        None,
                    ),
                    ColumnCatalog::new(
                        "c4".to_string(),
                        false,
                        ColumnDesc::new(Integer, false, false),
                        None,
                    ),
                ],
            )
            .await?;

        Ok(storage)
    }

    pub async fn select_sql_run(sql: &str) -> Result<LogicalPlan, ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = build_test_catalog(temp_dir.path()).await?;
        let binder = Binder::new(BinderContext::new(storage));
        let stmt = crate::parser::parse_sql(sql)?;

        Ok(binder.bind(&stmt[0]).await?)
    }
}
