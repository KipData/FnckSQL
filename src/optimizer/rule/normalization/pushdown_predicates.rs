use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::range_detacher::RangeDetacher;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::types::index::{IndexInfo, IndexType};
use crate::types::LogicalType;
use itertools::Itertools;
use lazy_static::lazy_static;

lazy_static! {
    static ref PUSH_PREDICATE_THROUGH_JOIN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Join(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };

    static ref PUSH_PREDICATE_INTO_SCAN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::TableScan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };

    // TODO: 感觉是只是处理projection中的alias反向替换为filter中表达式
    static ref PUSH_PREDICATE_THROUGH_NON_JOIN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Project(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

fn split_conjunctive_predicates(expr: &ScalarExpression) -> Vec<ScalarExpression> {
    match expr {
        ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr,
            right_expr,
            ..
        } => split_conjunctive_predicates(left_expr)
            .into_iter()
            .chain(split_conjunctive_predicates(right_expr))
            .collect_vec(),
        _ => vec![expr.clone()],
    }
}

/// reduce filters into a filter, and then build a new LogicalFilter node with input child.
/// if filters is empty, return the input child.
fn reduce_filters(filters: Vec<ScalarExpression>, having: bool) -> Option<FilterOperator> {
    filters
        .into_iter()
        .reduce(|a, b| ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr: Box::new(a),
            right_expr: Box::new(b),
            evaluator: None,
            ty: LogicalType::Boolean,
        })
        .map(|f| FilterOperator {
            predicate: f,
            having,
        })
}

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
/// If left equals right, return true.
pub fn is_subset_cols(left: &[ColumnRef], right: &[ColumnRef]) -> bool {
    left.iter().all(|l| right.contains(l))
}

/// Comments copied from Spark Catalyst PushPredicateThroughJoin
///
/// Pushes down `Filter` operators where the `condition` can be
/// evaluated using only the attributes of the left or right side of a join.  Other
/// `Filter` conditions are moved into the `condition` of the `Join`.
///
/// And also pushes down the join filter, where the `condition` can be evaluated using only the
/// attributes of the left or right side of sub query when applicable.
pub struct PushPredicateThroughJoin;

impl MatchPattern for PushPredicateThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_THROUGH_JOIN
    }
}

impl NormalizationRule for PushPredicateThroughJoin {
    // TODO: pushdown_predicates need to consider output columns
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        let child_id = match graph.eldest_child_at(node_id) {
            Some(child_id) => child_id,
            None => return Ok(()),
        };
        if let Operator::Join(child_op) = graph.operator(child_id) {
            if !matches!(
                child_op.join_type,
                JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightOuter
            ) {
                return Ok(());
            }

            let join_childs = graph.children_at(child_id).collect_vec();
            let left_columns = graph.operator(join_childs[0]).referenced_columns(true);
            let right_columns = graph.operator(join_childs[1]).referenced_columns(true);

            let mut new_ops = (None, None, None);

            if let Operator::Filter(op) = graph.operator(node_id) {
                let filter_exprs = split_conjunctive_predicates(&op.predicate);

                let (left_filters, rest): (Vec<_>, Vec<_>) = filter_exprs
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(true), &left_columns));
                let (right_filters, common_filters): (Vec<_>, Vec<_>) = rest
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(true), &right_columns));

                let replace_filters = match child_op.join_type {
                    JoinType::Inner => {
                        if !left_filters.is_empty() {
                            if let Some(left_filter_op) = reduce_filters(left_filters, op.having) {
                                new_ops.0 = Some(Operator::Filter(left_filter_op));
                            }
                        }

                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having)
                            {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters
                    }
                    JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => {
                        if !left_filters.is_empty() {
                            if let Some(left_filter_op) = reduce_filters(left_filters, op.having) {
                                new_ops.0 = Some(Operator::Filter(left_filter_op));
                            }
                        }

                        common_filters
                            .into_iter()
                            .chain(right_filters)
                            .collect_vec()
                    }
                    JoinType::RightOuter => {
                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having)
                            {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters.into_iter().chain(left_filters).collect_vec()
                    }
                    _ => vec![],
                };

                if !replace_filters.is_empty() {
                    if let Some(replace_filter_op) = reduce_filters(replace_filters, op.having) {
                        new_ops.2 = Some(Operator::Filter(replace_filter_op));
                    }
                }
            }

            if let Some(left_op) = new_ops.0 {
                graph.add_node(child_id, Some(join_childs[0]), left_op);
            }

            if let Some(right_op) = new_ops.1 {
                graph.add_node(child_id, Some(join_childs[1]), right_op);
            }

            if let Some(common_op) = new_ops.2 {
                graph.replace_node(node_id, common_op);
            } else {
                graph.remove_node(node_id, false);
            }
        }

        Ok(())
    }
}

pub struct PushPredicateIntoScan;

impl MatchPattern for PushPredicateIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_INTO_SCAN
    }
}

impl NormalizationRule for PushPredicateIntoScan {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Filter(op) = graph.operator(node_id).clone() {
            if let Some(child_id) = graph.eldest_child_at(node_id) {
                if let Operator::TableScan(child_op) = graph.operator_mut(child_id) {
                    //FIXME: now only support `unique` and `primary key`
                    for IndexInfo { meta, range } in &mut child_op.index_infos {
                        if range.is_some() {
                            continue;
                        }
                        *range = match meta.ty {
                            IndexType::PrimaryKey | IndexType::Unique | IndexType::Normal => {
                                RangeDetacher::new(meta.table_name.as_str(), &meta.column_ids[0])
                                    .detach(&op.predicate)
                            }
                            IndexType::Composite => {
                                let mut res = None;
                                let mut eq_ranges = Vec::with_capacity(meta.column_ids.len());

                                for column_id in meta.column_ids.iter() {
                                    if let Some(range) =
                                        RangeDetacher::new(meta.table_name.as_str(), column_id)
                                            .detach(&op.predicate)
                                    {
                                        if range.only_eq() {
                                            eq_ranges.push(range);
                                            continue;
                                        }
                                        res = range.combining_eqs(&eq_ranges);
                                    }
                                    break;
                                }
                                if res.is_none() {
                                    if let Some(range) = eq_ranges.pop() {
                                        res = range.combining_eqs(&eq_ranges);
                                    }
                                }
                                res
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use std::sync::Arc;

    #[test]
    fn test_push_predicate_into_scan() -> Result<(), DatabaseError> {
        // 1 - c2 < 0 => c2 > 1
        let plan = select_sql_run("select * from t1 where -(1 - c2) > 0")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .batch(
                "test_push_predicate_into_scan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateIntoScan],
            )
            .find_best::<RocksTransaction>(None)?;

        if let Operator::TableScan(op) = &best_plan.childrens[0].childrens[0].operator {
            let mock_range = Range::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(1)))),
                max: Bound::Unbounded,
            };

            assert_eq!(op.index_infos[1].range, Some(mock_range));
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_left_join() -> Result<(), DatabaseError> {
        let plan =
            select_sql_run("select * from t1 left join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        if let Operator::Filter(op) = &best_plan.childrens[0].childrens[0].childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_right_join() -> Result<(), DatabaseError> {
        let plan =
            select_sql_run("select * from t1 right join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        if let Operator::Filter(op) = &best_plan.childrens[0].childrens[0].childrens[1].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_inner_join() -> Result<(), DatabaseError> {
        let plan =
            select_sql_run("select * from t1 inner join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        if let Operator::Join(_) = &best_plan.childrens[0].operator {
        } else {
            unreachable!("Should be a filter operator")
        }

        if let Operator::Filter(op) = &best_plan.childrens[0].childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        if let Operator::Filter(op) = &best_plan.childrens[0].childrens[1].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }
}
