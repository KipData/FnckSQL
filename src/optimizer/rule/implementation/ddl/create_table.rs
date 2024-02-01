use crate::errors::DatabaseError;
use crate::optimizer::core::column_meta::ColumnMetaLoader;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use crate::storage::Transaction;
use lazy_static::lazy_static;

lazy_static! {
    static ref CREATE_TABLE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::CreateTable(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct CreateTableImplementation;

single_mapping!(
    CreateTableImplementation,
    CREATE_TABLE_PATTERN,
    PhysicalOption::CreateTable
);
