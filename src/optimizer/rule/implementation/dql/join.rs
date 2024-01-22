use lazy_static::lazy_static;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::OptimizerError;
use crate::single_mapping;


lazy_static! {
    static ref JOIN_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Join(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct HashJoinImplementation;

single_mapping!(HashJoinImplementation, JOIN_PATTERN, PhysicalOption::HashJoin);