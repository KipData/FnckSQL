pub mod aggregate;
mod create_table;
pub mod expr;
mod select;
mod insert;

use std::collections::HashMap;

use anyhow::Result;
use sqlparser::ast::{Ident, ObjectName, SetExpr, Statement};

use crate::catalog::{RootCatalog, DEFAULT_SCHEMA_NAME, CatalogError};
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::types::TableIdx;
#[derive(Clone)]
pub struct BinderContext {
    catalog: RootCatalog,
    bind_table: HashMap<String, TableIdx>,
    aliases: HashMap<String, ScalarExpression>,
    group_by_exprs: Vec<ScalarExpression>,
    agg_calls: Vec<ScalarExpression>,
    index: u16,
}

impl BinderContext {
    pub fn new(catalog: RootCatalog) -> Self {
        BinderContext {
            catalog,
            bind_table: Default::default(),
            aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
            index: 0,
        }
    }

    pub fn index(&mut self) -> u16 {
        let index = self.index;
        self.index += 1;
        index
    }

    pub fn add_alias(&mut self, alias: String, expr: ScalarExpression) {
        if self.aliases.contains_key(&alias) {
            return;
        }

        self.aliases.insert(alias, expr);
    }
}

pub struct Binder {
    context: BinderContext,
}

impl Binder {
    pub fn new(context: BinderContext) -> Self {
        Binder { context }
    }

    pub fn bind(mut self, stmt: &Statement) -> Result<LogicalPlan> {
        let plan = match stmt {
            Statement::Query(query) => {
                let plan = self.bind_query(query)?;
                LogicalPlan::Select(plan)
            }
            Statement::CreateTable { name, columns, .. } => {
                let plan = self.bind_create_table(name, &columns)?;
                LogicalPlan::CreateTable(plan)
            }
            Statement::Insert { table_name, columns, source, .. } => {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    let plan = self.bind_insert(table_name.to_owned(), columns, &values.rows)?;
                    LogicalPlan::Insert(plan)
                } else {
                    todo!()
                }
            }
            _ => unimplemented!(),
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
fn split_name(name: &ObjectName) -> Result<(&str, &str)> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(anyhow::anyhow!("Invalid table name: {:?}", name)),
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
    #[error("catalog error")]
    CatalogError(#[from] CatalogError),
}
