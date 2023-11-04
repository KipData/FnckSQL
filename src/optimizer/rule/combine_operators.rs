use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::is_subset_exprs;
use crate::optimizer::OptimizerError;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::Operator;
use crate::types::LogicalType;
use lazy_static::lazy_static;

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
}

/// Combine two adjacent project operators into one.
pub struct CollapseProject;

impl Rule for CollapseProject {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_PROJECT_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Project(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Project(child_op) = graph.operator(child_id) {
                if is_subset_exprs(&op.exprs, &child_op.exprs) {
                    graph.remove_node(child_id, false);
                } else {
                    graph.remove_node(node_id, false);
                }
            }
        }

        Ok(())
    }
}

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl Rule for CombineFilter {
    fn pattern(&self) -> &Pattern {
        &COMBINE_FILTERS_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Filter(child_op) = graph.operator(child_id) {
                let new_filter_op = FilterOperator {
                    predicate: ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(op.predicate.clone()),
                        right_expr: Box::new(child_op.predicate.clone()),
                        ty: LogicalType::Boolean,
                    },
                    having: op.having || child_op.having,
                };
                graph.replace_node(node_id, Operator::Filter(new_filter_op));
                graph.remove_node(child_id, false);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::db::DatabaseError;
    use crate::expression::ScalarExpression::Constant;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::graph::HepNodeId;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::Operator;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_collapse_project() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select c1, c2 from t1").await?;

        let mut optimizer = HepOptimizer::new(plan.clone()).batch(
            "test_collapse_project".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![RuleImpl::CollapseProject],
        );

        let mut new_project_op = optimizer.graph.operator(HepNodeId::new(0)).clone();

        if let Operator::Project(op) = &mut new_project_op {
            op.exprs.remove(0);
        } else {
            unreachable!("Should be a project operator")
        }

        optimizer.graph.add_root(new_project_op);

        let best_plan = optimizer.find_best()?;

        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator")
        }

        if let Operator::Scan(_) = &best_plan.childrens[0].operator {
            assert_eq!(best_plan.childrens[0].childrens.len(), 0)
        } else {
            unreachable!("Should be a scan operator")
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_combine_filter() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select * from t1 where c1 > 1").await?;

        let mut optimizer = HepOptimizer::new(plan.clone()).batch(
            "test_combine_filter".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![RuleImpl::CombineFilter],
        );

        let mut new_filter_op = optimizer.graph.operator(HepNodeId::new(1)).clone();

        if let Operator::Filter(op) = &mut new_filter_op {
            op.predicate = ScalarExpression::Binary {
                op: BinaryOperator::Eq,
                left_expr: Box::new(Constant(Arc::new(DataValue::Int8(Some(1))))),
                right_expr: Box::new(Constant(Arc::new(DataValue::Int8(Some(1))))),
                ty: LogicalType::Boolean,
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        optimizer
            .graph
            .add_node(HepNodeId::new(0), Some(HepNodeId::new(1)), new_filter_op);

        let best_plan = optimizer.find_best()?;

        if let Operator::Filter(op) = &best_plan.childrens[0].operator {
            if let ScalarExpression::Binary { op, .. } = &op.predicate {
                assert_eq!(op, &BinaryOperator::And);
            } else {
                unreachable!("Should be a and operator")
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }
}
