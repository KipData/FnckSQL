use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use lazy_static::lazy_static;

lazy_static! {
    static ref SCAN_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Scan(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct SeqScanImplementation;

single_mapping!(SeqScanImplementation, SCAN_PATTERN, PhysicalOption::SeqScan);

pub struct IndexScanImplementation;

impl MatchPattern for IndexScanImplementation {
    fn pattern(&self) -> &Pattern {
        &SCAN_PATTERN
    }
}

impl ImplementationRule for IndexScanImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        group_expr: &mut GroupExpression,
    ) -> Result<(), OptimizerError> {
        if let Operator::Scan(scan_op) = op {
            for index_info in scan_op.index_infos.iter() {
                if index_info.binaries.is_none() {
                    continue;
                }

                group_expr.append_expr(Expression {
                    ops: vec![PhysicalOption::IndexScan(index_info.clone())],
                })
            }

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}
