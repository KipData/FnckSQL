use crate::planner::LogicalPlan;
use crate::{expression::ScalarExpression, planner::operator::Operator};
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ScalarExpression>,
    pub agg_calls: Vec<ScalarExpression>,
}

impl AggregateOperator {
    pub fn build(
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
            }),
            vec![children],
        )
    }
}

impl fmt::Display for AggregateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let calls = self
            .agg_calls
            .iter()
            .map(|call| format!("{}", call))
            .join(", ");
        write!(f, "Aggregate [{}]", calls)?;

        if !self.groupby_exprs.is_empty() {
            let groupbys = self
                .groupby_exprs
                .iter()
                .map(|groupby| format!("{}", groupby))
                .join(", ");
            write!(f, " -> Group By [{}]", groupbys)?;
        }

        Ok(())
    }
}
