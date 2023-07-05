use crate::binder::{Binder, BinderContext};
use crate::catalog::Root;
use crate::parser;
use anyhow::Result;
use serde::de::Unexpected::Option;
use sqlparser::ast::Statement;
use std::sync::Arc;

use crate::planner::logical_select_plan::LogicalSelectPlan;

use super::LogicalPlan;

#[derive(Clone)]
pub struct PlanBuilder {
    context: BinderContext,
}

impl PlanBuilder {
    pub fn new() -> Self {
        PlanBuilder {
            context: BinderContext::new(Arc::new(Root::new())),
        }
    }

    /// Build a logical plan.
    ///
    /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
    /// Scan(t1)
    ///   Sort(a)
    ///     Limit(1)
    ///       Project(a,b)
    pub fn build_sql<'a>(&self, sql: &'a str) -> Result<LogicalPlan> {
        let stmts = parser::parse_sql(sql)?;

        println!("stmt:{:#}", stmts[0]);

        // TODO: add plan-cache for fast return.

        let mut binder = Binder::new(self.context.clone());

        let logical_plan = binder.bind(&stmts[0])?;

        println!("logical_plan:{:?}", logical_plan);

        // let mut optimizer = Optimizer::new(self.context.clone());

        // plan = optimizer.optimize(plan)?;

        // tracing::info!("optimize after {:?}", plan);

        Ok(logical_plan)

        //todo!()
    }
}
