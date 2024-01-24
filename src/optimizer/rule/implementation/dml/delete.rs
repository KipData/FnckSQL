use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use lazy_static::lazy_static;

lazy_static! {
    static ref DELETE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Delete(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct DeleteImplementation;

single_mapping!(DeleteImplementation, DELETE_PATTERN, PhysicalOption::Delete);
