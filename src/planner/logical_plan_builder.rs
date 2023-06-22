use std::sync::Arc;

use crate::parser;
use anyhow::Result;

use super::LogicalPlan;

pub struct PlanBuilder {}

impl PlanBuilder {
    pub fn new() -> Self {
        PlanBuilder {}
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

        // TODO: add plan-cache for fast return.

        // let mut binder = Binder::new(self.context.clone());

        // let mut plan = match &stmts[0] {
        //     Statement::Query(query) => binder.bind_query(query)?,
        //     _ => unreachable!(),
        // };

        // tracing::info!("optimize before {:?}", plan);

        // let mut optimizer = Optimizer::new(self.context.clone());

        // plan = optimizer.optimize(plan)?;

        // tracing::info!("optimize after {:?}", plan);

        // Ok(plan)

        todo!()
    }
}
