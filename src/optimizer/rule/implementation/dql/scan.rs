use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::{StatisticMetaLoader, StatisticsMeta};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::storage::Transaction;
use crate::types::index::{IndexId, IndexType};
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
        loader: &StatisticMetaLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::Scan(scan_op) = op {
            let statistics_metas = loader.load(scan_op.table_name.clone())?;
            let mut cost = None;

            if let Some(statistics_meta) =
                find_statistics_meta(statistics_metas, &scan_op.primary_key)
            {
                cost = Some(statistics_meta.histogram().values_len());
            }

            group_expr.append_expr(Expression {
                op: PhysicalOption::SeqScan,
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
        loader: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::Scan(scan_op) = op {
            let statistics_metas = loader.load(scan_op.table_name.clone())?;
            for index_info in scan_op.index_infos.iter() {
                if index_info.range.is_none() {
                    continue;
                }
                let mut cost = None;

                if let Some(range) = &index_info.range {
                    // FIXME: Only UniqueIndex
                    if let Some(statistics_meta) =
                        find_statistics_meta(statistics_metas, &index_info.meta.id)
                    {
                        let mut row_count = statistics_meta.collect_count(range)?;

                        if !matches!(index_info.meta.ty, IndexType::PrimaryKey) {
                            // need to return table query(non-covering index)
                            row_count *= 2;
                        }
                        cost = Some(row_count);
                    }
                }

                group_expr.append_expr(Expression {
                    op: PhysicalOption::IndexScan(index_info.clone()),
                    cost,
                })
            }

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

fn find_statistics_meta<'a>(
    statistics_metas: &'a [StatisticsMeta],
    index_id: &IndexId,
) -> Option<&'a StatisticsMeta> {
    assert!(statistics_metas.is_sorted_by_key(StatisticsMeta::index_id));
    statistics_metas
        .binary_search_by(|statistics_meta| statistics_meta.index_id().cmp(index_id))
        .ok()
        .map(|i| &statistics_metas[i])
}
