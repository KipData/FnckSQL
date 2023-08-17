pub mod aggregate;
mod create_table;
pub mod expr;
mod select;
mod insert;

use std::collections::BTreeMap;

use anyhow::Result;
use sqlparser::ast::{Ident, ObjectName, SetExpr, Statement};

use crate::catalog::{RootCatalog, DEFAULT_SCHEMA_NAME, CatalogError};
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::planner::operator::join::JoinType;
use crate::types::TableId;
#[derive(Debug, Clone)]
pub struct BinderContext {
    pub(crate) catalog: RootCatalog,
    pub(crate) bind_table: BTreeMap<String, (TableId, Option<JoinType>)>,
    aliases: BTreeMap<String, ScalarExpression>,
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
            Statement::Query(query) => self.bind_query(query)?,
            Statement::CreateTable { name, columns, .. } => self.bind_create_table(name, &columns)?,
            Statement::Insert { table_name, columns, source, .. } => {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.bind_insert(table_name.to_owned(), columns, &values.rows)?
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

#[cfg(test)]
pub mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::planner::LogicalPlan;
    use crate::types::LogicalType::Integer;
    use anyhow::Result;
    use crate::binder::{Binder, BinderContext};

    fn test_root_catalog() -> Result<RootCatalog> {
        let mut root = RootCatalog::new();

        let cols_t1 = vec![
            ColumnCatalog::new("c1".to_string(), false, ColumnDesc::new(Integer, true)),
            ColumnCatalog::new("c2".to_string(), false, ColumnDesc::new(Integer, false)),
        ];
        let _ = root.add_table("t1".to_string(), cols_t1)?;

        let cols_t2 = vec![
            ColumnCatalog::new("c3".to_string(), false, ColumnDesc::new(Integer, true)),
            ColumnCatalog::new("c4".to_string(), false, ColumnDesc::new(Integer, false)),
        ];
        let _ = root.add_table("t2".to_string(), cols_t2)?;
        Ok(root)
    }

    pub fn select_sql_run(sql: &str) -> Result<LogicalPlan> {
        let root = test_root_catalog()?;

        let binder = Binder::new(BinderContext::new(root));
        let stmt = crate::parser::parse_sql(sql).unwrap();

        Ok(binder.bind(&stmt[0])?)
    }
}
