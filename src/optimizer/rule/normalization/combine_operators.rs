use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::normalization::is_subset_exprs;
use crate::planner::operator::Operator;
use crate::types::LogicalType;
use lazy_static::lazy_static;
use std::collections::HashSet;

lazy_static! {
    static ref COLLAPSE_PROJECT_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Project(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Project(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref COMBINE_FILTERS_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Filter(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref COLLAPSE_GROUP_BY_AGG: Pattern = {
        Pattern {
            predicate: |op| match op {
                Operator::Aggregate(agg_op) => !agg_op.groupby_exprs.is_empty(),
                _ => false,
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| match op {
                    Operator::Aggregate(agg_op) => !agg_op.groupby_exprs.is_empty(),
                    _ => false,
                },
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Combine two adjacent project operators into one.
pub struct CollapseProject;

impl MatchPattern for CollapseProject {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_PROJECT_RULE
    }
}

impl NormalizationRule for CollapseProject {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Project(op) = graph.operator(node_id) {
            if let Some(child_id) = graph.eldest_child_at(node_id) {
                if let Operator::Project(child_op) = graph.operator(child_id) {
                    if is_subset_exprs(&op.exprs, &child_op.exprs) {
                        graph.remove_node(child_id, false);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl MatchPattern for CombineFilter {
    fn pattern(&self) -> &Pattern {
        &COMBINE_FILTERS_RULE
    }
}

impl NormalizationRule for CombineFilter {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Filter(op) = graph.operator(node_id).clone() {
            if let Some(child_id) = graph.eldest_child_at(node_id) {
                if let Operator::Filter(child_op) = graph.operator_mut(child_id) {
                    child_op.predicate = ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(op.predicate),
                        right_expr: Box::new(child_op.predicate.clone()),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    };
                    child_op.having = op.having || child_op.having;

                    graph.remove_node(node_id, false);
                }
            }
        }

        Ok(())
    }
}

pub struct CollapseGroupByAgg;

impl MatchPattern for CollapseGroupByAgg {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_GROUP_BY_AGG
    }
}

impl NormalizationRule for CollapseGroupByAgg {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Aggregate(op) = graph.operator(node_id).clone() {
            // if it is an aggregation operator containing agg_call
            if !op.agg_calls.is_empty() {
                return Ok(());
            }

            if let Some(Operator::Aggregate(child_op)) = graph
                .eldest_child_at(node_id)
                .map(|child_id| graph.operator_mut(child_id))
            {
                if op.groupby_exprs.len() != child_op.groupby_exprs.len() {
                    return Ok(());
                }
                let mut expr_set = HashSet::new();

                for expr in op.groupby_exprs.iter() {
                    expr_set.insert(expr);
                }
                for expr in child_op.groupby_exprs.iter() {
                    expr_set.remove(expr);
                }
                if expr_set.is_empty() {
                    graph.remove_node(node_id, false);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::ScalarExpression::Constant;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::graph::HepNodeId;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn test_collapse_project() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1, c2 from t1")?;

        let mut optimizer = HepOptimizer::new(plan.clone()).batch(
            "test_collapse_project".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CollapseProject],
        );

        let mut new_project_op = optimizer.graph.operator(HepNodeId::new(0)).clone();

        if let Operator::Project(op) = &mut new_project_op {
            op.exprs.remove(0);
        } else {
            unreachable!("Should be a project operator")
        }

        optimizer.graph.add_root(new_project_op);

        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        if let Operator::Project(op) = &best_plan.operator {
            debug_assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator")
        }

        if let Operator::TableScan(_) = &best_plan.childrens[0].operator {
            debug_assert_eq!(best_plan.childrens[0].childrens.len(), 0)
        } else {
            unreachable!("Should be a scan operator")
        }

        Ok(())
    }

    #[test]
    fn test_combine_filter() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 where c1 > 1")?;

        let mut optimizer = HepOptimizer::new(plan.clone()).batch(
            "test_combine_filter".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CombineFilter],
        );

        let mut new_filter_op = optimizer.graph.operator(HepNodeId::new(1)).clone();

        if let Operator::Filter(op) = &mut new_filter_op {
            op.predicate = ScalarExpression::Binary {
                op: BinaryOperator::Eq,
                left_expr: Box::new(Constant(DataValue::Int8(Some(1)))),
                right_expr: Box::new(Constant(DataValue::Int8(Some(1)))),
                evaluator: None,
                ty: LogicalType::Boolean,
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        optimizer
            .graph
            .add_node(HepNodeId::new(0), Some(HepNodeId::new(1)), new_filter_op);

        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            if let ScalarExpression::Binary { op, .. } = &op.predicate {
                debug_assert_eq!(op, &BinaryOperator::And);
            } else {
                unreachable!("Should be a and operator")
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_collapse_group_by_agg() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select distinct c1, c2 from t1 group by c1, c2")?;

        let optimizer = HepOptimizer::new(plan.clone()).batch(
            "test_collapse_group_by_agg".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CollapseGroupByAgg],
        );

        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        if let Operator::Aggregate(_) = &best_plan.childrens[0].operator {
            if let Operator::Aggregate(_) = &best_plan.childrens[0].childrens[0].operator {
                unreachable!("Should not be a agg operator")
            } else {
                return Ok(());
            }
        }
        unreachable!("Should be a agg operator")
    }
}
