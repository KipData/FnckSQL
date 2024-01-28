use crate::optimizer::core::column_meta::ColumnMetaLoader;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use crate::storage::Transaction;
use lazy_static::lazy_static;

lazy_static! {
    static ref COPY_TO_FILE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::CopyToFile(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct CopyToFileImplementation;

single_mapping!(
    CopyToFileImplementation,
    COPY_TO_FILE_PATTERN,
    PhysicalOption::CopyToFile
);
