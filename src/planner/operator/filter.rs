use std::fmt::Formatter;
use std::{fmt, vec};

use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct FilterOperator {
    pub predicate: ScalarExpression,
    pub having: bool,
}

impl FilterOperator {
    pub fn build(predicate: ScalarExpression, children: LogicalPlan, having: bool) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Filter(FilterOperator { predicate, having }),
            childrens: vec![children],
            physical_option: None,
        }
    }
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Filter {}, Is Having: {}", self.predicate, self.having)?;

        Ok(())
    }
}
