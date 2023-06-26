pub mod aggregate;
mod create;
pub mod expr;
mod select;

use std::collections::HashMap;

use crate::{
    catalog::CatalogRef,
    expression::ScalarExpression,
    planner::LogicalPlan,
};

use anyhow::Result;
use sqlparser::ast::Statement;
use crate::types::TableId;

pub struct BinderContext {
    catalog: CatalogRef,
    bind_table: HashMap<String, TableId>,
    aliases: HashMap<String, ScalarExpression>,
    group_by_exprs: Vec<ScalarExpression>,
    agg_calls: Vec<ScalarExpression>,
    index: u16,
}

impl BinderContext {
    pub fn new(catalog: CatalogRef) -> Self {
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
            _ => unimplemented!(),
        };
        Ok(plan)
    }
}
