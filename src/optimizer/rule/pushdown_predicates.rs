use itertools::Itertools;
use lazy_static::lazy_static;
use crate::catalog::ColumnCatalog;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::opt_expr::OptExprNode;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::types::LogicalType;

lazy_static! {
    static ref PUSH_PREDICATE_THROUGH_JOIN: Pattern = {
        Pattern {
            predicate: |op| match op {
                Operator::Filter(_) => true,
                _ => false
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| match op {
                    Operator::Join(_) => true,
                    _ => false
                },
                children: PatternChildrenPredicate::None,
            }]),
        }
    };

    // TODO
    static ref PUSH_PREDICATE_THROUGH_NON_JOIN: Pattern = {
        Pattern {
            predicate: |op| match op {
                Operator::Filter(_) => true,
                _ => false
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| match op {
                    Operator::Project(_) => true,
                    _ => false
                },
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
        _ => vec![expr.clone()]
    }
}

/// reduce filters into a filter, and then build a new LogicalFilter node with input child.
/// if filters is empty, return the input child.
fn reduce_filters(filters: Vec<ScalarExpression>, having: bool) -> Option<FilterOperator> {
    filters
        .into_iter()
        .reduce(|a, b| {
            ScalarExpression::Binary {
                op: BinaryOperator::And,
                left_expr: Box::new(a),
                right_expr: Box::new(b),
                ty: LogicalType::Boolean
            }
        })
        .map(|f| FilterOperator {
            predicate: f,
            having
        })
}

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
/// If left equals right, return true.
pub fn is_subset_cols(left: &[ColumnCatalog], right: &[ColumnCatalog]) -> bool {
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

impl Rule for PushPredicateThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_THROUGH_JOIN
    }

    // TODO: pushdown_predicates need to consider output columns
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        let child_id = graph.children_at(node_id)[0];
        if let Operator::Join(child_op) = graph.operator(child_id) {
            if !matches!(child_op.join_type, JoinType::Inner | JoinType::Left | JoinType::Right) {
                return ;
            }

            let join_childs = graph.children_at(child_id);
            let left_columns = graph
                .operator(join_childs[0])
                .referenced_columns();
            let right_columns = graph
                .operator(join_childs[1])
                .referenced_columns();

            let mut new_ops = (None, None, None);

            if let Operator::Filter(op) = graph.operator(node_id) {
                let filter_exprs = split_conjunctive_predicates(&op.predicate);

                let (left_filters, rest): (Vec<_>, Vec<_>) = filter_exprs
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(), &left_columns));
                let (right_filters, common_filters): (Vec<_>, Vec<_>) = rest
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(), &right_columns));

                let replace_filters = match child_op.join_type {
                    JoinType::Inner => {
                        if !left_filters.is_empty() {
                            if let Some(left_filter_op) = reduce_filters(left_filters, op.having) {
                                new_ops.0 = Some(Operator::Filter(left_filter_op));
                            }
                        }

                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having) {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters
                    }
                    JoinType::Left => {
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
                    JoinType::Right => {
                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having) {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters
                            .into_iter()
                            .chain(left_filters)
                            .collect_vec()
                    }
                    _ => vec![]
                };

                if !replace_filters.is_empty() {
                    if let Some(replace_filter_op) = reduce_filters(replace_filters, op.having) {
                        new_ops.2 = Some(Operator::Filter(replace_filter_op));
                    }
                }
            }

            if let Some(left_op) = new_ops.0 {
                graph.add_node(
                    child_id,
                    Some(join_childs[0]),
                    OptExprNode::OperatorRef(left_op)
                );
            }

            if let Some(right_op) = new_ops.1 {
                graph.add_node(
                    child_id,
                    Some(join_childs[1]),
                    OptExprNode::OperatorRef(right_op)
                );
            }

            if let Some(common_op) = new_ops.2 {
                graph.replace_node(
                    node_id,
                    OptExprNode::OperatorRef(common_op)
                );
            } else {
                graph.remove_node(node_id, false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use crate::binder::test::select_sql_run;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::Operator;
    use crate::types::LogicalType;

    #[test]
    fn test_push_predicate_through_join_in_left_join() -> Result<()> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::PushPredicateThroughJoin]
            )
            .find_best();

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!()
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
                _ => unreachable!()
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_right_join() -> Result<()> {
        let plan = select_sql_run("select * from t1 right join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::PushPredicateThroughJoin]
            )
            .find_best();

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!()
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
                _ => unreachable!()
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_inner_join() -> Result<()> {
        let plan = select_sql_run("select * from t1 inner join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::PushPredicateThroughJoin]
            )
            .find_best();

        if let Operator::Join(op) = &best_plan.childrens[0].operator {

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
                _ => unreachable!()
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
                _ => unreachable!()
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }
}