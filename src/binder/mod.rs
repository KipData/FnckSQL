pub mod aggregate;
mod create;
pub mod expr;
mod select;

use std::collections::HashMap;

use crate::{catalog::CatalogRef, expression::ScalarExpression, planner::LogicalPlan};

use crate::catalog::{Root, DEFAULT_SCHEMA_NAME};
use crate::types::TableId;
use anyhow::Result;
use sqlparser::ast::{Ident, ObjectName, Statement};
#[derive(Clone)]
pub struct BinderContext {
    catalog: Root,
    bind_table: HashMap<String, TableId>,
    aliases: HashMap<String, ScalarExpression>,
    group_by_exprs: Vec<ScalarExpression>,
    agg_calls: Vec<ScalarExpression>,
    index: u16,
}

impl BinderContext {
    pub fn new(catalog: Root) -> Self {
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
                let plan = self.bind_create_table(name.to_owned(), &columns)?;
                LogicalPlan::CreateTable(plan)
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
