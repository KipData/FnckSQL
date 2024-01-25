use crate::optimizer::core::histogram::Histogram;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::rule::implementation::HistogramLoader;
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::storage::Transaction;
use crate::types::ColumnId;
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

impl MatchPattern for SeqScanImplementation {
    fn pattern(&self) -> &Pattern {
        &SCAN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for SeqScanImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &HistogramLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), OptimizerError> {
        if let Operator::Scan(scan_op) = op {
            let histograms = loader.load(scan_op.table_name.clone())?;
            let mut cost = None;

            if let Some(histogram) = find_histogram(histograms, &scan_op.primary_key) {
                cost = Some(histogram.values_len());
            }

            group_expr.append_expr(Expression {
                ops: vec![PhysicalOption::SeqScan],
                cost,
            });

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

pub struct IndexScanImplementation;

impl MatchPattern for IndexScanImplementation {
    fn pattern(&self) -> &Pattern {
        &SCAN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for IndexScanImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &HistogramLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), OptimizerError> {
        if let Operator::Scan(scan_op) = op {
            let histograms = loader.load(scan_op.table_name.clone())?;
            for index_info in scan_op.index_infos.iter() {
                if index_info.binaries.is_none() {
                    continue;
                }
                let mut cost = None;

                if let Some(binaries) = &index_info.binaries {
                    // FIXME: Only UniqueIndex
                    if let Some(histogram) =
                        find_histogram(histograms, &index_info.meta.column_ids[0])
                    {
                        // need to return table query(non-covering index)
                        cost = Some(histogram.collect_count(binaries) * 2);
                    }
                }

                group_expr.append_expr(Expression {
                    ops: vec![PhysicalOption::IndexScan(index_info.clone())],
                    cost,
                })
            }

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

fn find_histogram<'a>(
    histograms: &'a Vec<Histogram>,
    column_id: &ColumnId,
) -> Option<&'a Histogram> {
    histograms
        .binary_search_by(|histogram| histogram.column_id().cmp(column_id))
        .ok()
        .map(|i| &histograms[i])
}
