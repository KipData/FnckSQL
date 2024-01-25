use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::storage::Transaction;
use crate::optimizer::rule::implementation::HistogramLoader;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use lazy_static::lazy_static;

lazy_static! {
    static ref TRUNCATE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::DropTable(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct TruncateImplementation;

single_mapping!(
    TruncateImplementation,
    TRUNCATE_PATTERN,
    PhysicalOption::Truncate
);
