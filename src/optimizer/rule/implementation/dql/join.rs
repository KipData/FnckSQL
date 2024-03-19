use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::join::{JoinCondition, JoinOperator};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::storage::Transaction;
use lazy_static::lazy_static;

lazy_static! {
    static ref JOIN_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Join(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct JoinImplementation;

impl MatchPattern for JoinImplementation {
    fn pattern(&self) -> &Pattern {
        &JOIN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for JoinImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        _: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        let mut physical_option = PhysicalOption::NestLoopJoin;

        if let Operator::Join(JoinOperator {
            on: JoinCondition::On { on, .. },
            ..
        }) = op
        {
            if !on.is_empty() {
                physical_option = PhysicalOption::HashJoin;
            }
        }
        group_expr.append_expr(Expression {
            op: physical_option,
            cost: None,
        });
        Ok(())
    }
}
