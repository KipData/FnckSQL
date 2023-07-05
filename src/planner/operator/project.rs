use std::sync::Arc;

use itertools::Itertools;

use crate::{expression::ScalarExpression, planner::logical_select_plan::LogicalSelectPlan};

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectOperator {
    pub columns: Vec<ScalarExpression>,
}

impl ProjectOperator {
    pub fn new(
        columns: impl IntoIterator<Item = ScalarExpression>,
        children: LogicalSelectPlan,
    ) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Project(ProjectOperator {
                columns: columns.into_iter().collect_vec(),
            })),
            children: vec![Arc::new(children)],
        }
    }
}
