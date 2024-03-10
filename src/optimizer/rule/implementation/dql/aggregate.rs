use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use crate::storage::Transaction;
use lazy_static::lazy_static;

lazy_static! {
    static ref GROUP_BY_AGGREGATE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| {
                if let Operator::Aggregate(op) = op {
                    return !op.groupby_exprs.is_empty();
                }
                false
            },
            children: PatternChildrenPredicate::None,
        }
    };
    static ref SIMPLE_AGGREGATE_PATTERN: Pattern = {
        Pattern {
            predicate: |op| {
                if let Operator::Aggregate(op) = op {
                    return op.groupby_exprs.is_empty();
                }
                false
            },
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct GroupByAggregateImplementation;

single_mapping!(
    GroupByAggregateImplementation,
    GROUP_BY_AGGREGATE_PATTERN,
    PhysicalOption::HashAggregate
);

pub struct SimpleAggregateImplementation;

single_mapping!(
    SimpleAggregateImplementation,
    SIMPLE_AGGREGATE_PATTERN,
    PhysicalOption::SimpleAggregate
);
