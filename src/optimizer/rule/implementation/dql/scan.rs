use crate::errors::DatabaseError;
use crate::optimizer::core::column_meta::{ColumnMeta, ColumnMetaLoader};
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
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
        loader: &ColumnMetaLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::Scan(scan_op) = op {
            let column_metas = loader.load(scan_op.table_name.clone())?;
            let mut cost = None;

            if let Some(column_meta) = find_column_meta(column_metas, &scan_op.primary_key) {
                cost = Some(column_meta.histogram().values_len());
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
        loader: &ColumnMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::Scan(scan_op) = op {
            let column_metas = loader.load(scan_op.table_name.clone())?;
            for index_info in scan_op.index_infos.iter() {
                if index_info.binaries.is_none() {
                    continue;
                }
                let mut cost = None;

                if let Some(binaries) = &index_info.binaries {
                    // FIXME: Only UniqueIndex
                    if let Some(column_meta) =
                        find_column_meta(column_metas, &index_info.meta.column_ids[0])
                    {
                        // need to return table query(non-covering index)
                        cost = Some(column_meta.collect_count(binaries) * 2);
                    }
                }
                if matches!(cost, Some(0)) {
                    continue;
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

fn find_column_meta<'a>(
    column_metas: &'a Vec<ColumnMeta>,
    column_id: &ColumnId,
) -> Option<&'a ColumnMeta> {
    assert!(column_metas.is_sorted_by_key(ColumnMeta::column_id));
    column_metas
        .binary_search_by(|column_meta| column_meta.column_id().cmp(column_id))
        .ok()
        .map(|i| &column_metas[i])
}
