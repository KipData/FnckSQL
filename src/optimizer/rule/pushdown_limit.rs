use std::cmp;
use lazy_static::lazy_static;
use crate::optimizer::core::opt_expr::OptExprNode;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::join::JoinType;
use crate::planner::operator::limit::LimitOperator;
use crate::planner::operator::Operator;
lazy_static! {
    static ref LIMIT_PROJECT_TRANSPOSE_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Project(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref ELIMINATE_LIMITS_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Limit(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref PUSH_LIMIT_THROUGH_JOIN_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Join(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref PUSH_LIMIT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Scan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

pub struct LimitProjectTranspose;

impl Rule for LimitProjectTranspose {
    fn pattern(&self) -> &Pattern {
        &LIMIT_PROJECT_TRANSPOSE_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        graph.swap_node(
            node_id,
            graph.children_at(node_id)[0]
        );
    }
}

/// Combines two adjacent Limit operators into one, merging the expressions into one single
/// expression.
pub struct EliminateLimits;

impl Rule for EliminateLimits {
    fn pattern(&self) -> &Pattern {
        &ELIMINATE_LIMITS_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        if let Operator::Limit(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Limit(child_op) = graph.operator(child_id) {
                let new_limit_op = LimitOperator {
                    offset: op.offset + child_op.offset,
                    limit: cmp::min(op.limit, child_op.limit),
                };

                graph.remove_node(child_id, false);
                graph.replace_node(
                    node_id,
                    OptExprNode::OperatorRef(
                        Operator::Limit(new_limit_op)
                    )
                );
            }
        }
    }
}

/// Add extra limits below JOIN:
/// 1. For LEFT OUTER and RIGHT OUTER JOIN, we push limits to the left and right sides,
/// respectively.
///
/// TODO: 2. For INNER and CROSS JOIN, we push limits to both the left and right sides
/// TODO: if join condition is empty.
pub struct PushLimitThroughJoin;

impl Rule for PushLimitThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_THROUGH_JOIN_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        let child_id = graph.children_at(node_id)[0];
        let join_type = if let Operator::Join(op) = graph.operator(child_id) {
            Some(op.join_type)
        } else {
            None
        };

        if let Some(ty) = join_type {
            if let Some(grandson_id) = match ty {
                JoinType::Left => Some(graph.children_at(child_id)[0]),
                JoinType::Right => Some(graph.children_at(child_id)[1]),
                _ => None
            } {
                let limit_node = graph.remove_node(node_id, false).unwrap();

                graph.add_node(
                    child_id,
                    Some(grandson_id),
                    limit_node
                );
            }
        }
    }
}

/// Push down `Limit` past a `Scan`.
pub struct PushLimitIntoScan;

impl Rule for PushLimitIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        if let Operator::Limit(limit_op) = graph.operator(node_id) {
            let child_index = graph.children_at(node_id)[0];
            if let Operator::Scan(scan_op) = graph.operator(child_index) {
                let mut new_scan_op = scan_op.clone();

                new_scan_op.limit = (Some(limit_op.offset), Some(limit_op.limit));

                graph.remove_node(node_id, false);
                graph.replace_node(
                    child_index,
                    OptExprNode::OperatorRef(Operator::Scan(new_scan_op))
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::execution_ap::ExecutorError;
    use crate::optimizer::core::opt_expr::OptExprNode;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::Operator;

    #[test]
    fn test_limit_project_transpose() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select c1, c2 from t1 limit 1")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_limit_project_transpose".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::LimitProjectTranspose]
            )
             .find_best();

        if let Operator::Project(_) = &best_plan.operator {

        } else {
            unreachable!("Should be a project operator")
        }

        if let Operator::Limit(_) = &best_plan.childrens[0].operator {

        } else {
            unreachable!("Should be a limit operator")
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_limits() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select c1, c2 from t1 limit 1 offset 1")?;

        let mut optimizer = HepOptimizer::new(plan.clone())
            .batch(
                "test_eliminate_limits".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::EliminateLimits]
            );

        let new_limit_op = LimitOperator {
            offset: 2,
            limit: 1,
        };

        optimizer.graph.add_root(
            OptExprNode::OperatorRef(Operator::Limit(new_limit_op))
        );

        let best_plan = optimizer.find_best();

        if let Operator::Limit(op) = &best_plan.operator {
            assert_eq!(op.limit, 1);
            assert_eq!(op.offset, 3);
        } else {
            unreachable!("Should be a project operator")
        }

        if let Operator::Limit(_) = &best_plan.childrens[0].operator {
            unreachable!("Should not be a limit operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_limit_through_join() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 left join t2 on c1 = c3 limit 1")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_push_limit_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    RuleImpl::LimitProjectTranspose,
                    RuleImpl::PushLimitThroughJoin
                ]
            )
            .find_best();

        if let Operator::Join(_) = &best_plan.childrens[0].operator {
        } else {
            unreachable!("Should be a project operator")
        }

        if let Operator::Limit(op) = &best_plan.childrens[0].childrens[0].operator {
            assert_eq!(op.limit, 1);
        } else {
            unreachable!("Should be a project operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_limit_into_table_scan() -> Result<(), ExecutorError> {
        let plan = select_sql_run("select * from t1 limit 1 offset 1")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_push_limit_into_table_scan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    RuleImpl::LimitProjectTranspose,
                    RuleImpl::PushLimitIntoTableScan
                ]
            )
            .find_best();

        if let Operator::Scan(op) = &best_plan.childrens[0].operator {
            assert_eq!(op.limit, (Some(1), Some(1)))
        } else {
            unreachable!("Should be a project operator")
        }

        Ok(())
    }
}
