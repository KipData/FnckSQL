use lazy_static::lazy_static;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::OptimizerError;
use crate::single_mapping;


lazy_static! {
    static ref INSERT_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Insert(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct InsertImplementation;

single_mapping!(InsertImplementation, INSERT_PATTERN, PhysicalOption::Insert);