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
    static ref COPY_FROM_FILE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::CopyFromFile(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct CopyFromFileImplementation;

single_mapping!(
    CopyFromFileImplementation,
    COPY_FROM_FILE_PATTERN,
    PhysicalOption::CopyFromFile
);
