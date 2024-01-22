use crate::catalog::{ColumnRef, ColumnSummary};
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::Operator;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::sync::Arc;
use itertools::Itertools;

lazy_static! {
    static ref COLUMN_PRUNING_RULE: Pattern = {
        Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct ColumnPruning;

impl ColumnPruning {
    fn clear_exprs(
        column_references: &mut HashSet<ColumnSummary>,
        exprs: &mut Vec<ScalarExpression>,
    ) {
        exprs.retain(|expr| {
            if column_references.contains(expr.output_column().summary()) {
                return true;
            }
            expr.referenced_columns(false)
                .iter()
                .any(|column| column_references.contains(column.summary()))
        })
    }

    fn _apply(
        column_references: &mut HashSet<ColumnSummary>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) {
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Aggregate(op) => {
                if !all_referenced {
                    Self::clear_exprs(column_references, &mut op.agg_calls);

                    if op.agg_calls.is_empty() && op.groupby_exprs.is_empty() {
                        let value = Arc::new(DataValue::Utf8(Some("*".to_string())));
                        // only single COUNT(*) is not depend on any column
                        // removed all expressions from the aggregate: push a COUNT(*)
                        op.agg_calls.push(ScalarExpression::AggCall {
                            distinct: false,
                            kind: AggKind::Count,
                            args: vec![ScalarExpression::Constant(value)],
                            ty: LogicalType::Integer,
                        })
                    }
                }
                let op_ref_columns = operator.referenced_columns(false);

                Self::recollect_apply(op_ref_columns, false, node_id, graph);
            }
            Operator::Project(op) => {
                let has_count_star = op.exprs.iter().any(ScalarExpression::has_count_star);
                if !has_count_star {
                    if !all_referenced {
                        Self::clear_exprs(column_references, &mut op.exprs);
                    }
                    let op_ref_columns = operator.referenced_columns(false);

                    Self::recollect_apply(op_ref_columns, false, node_id, graph);
                }
            }
            Operator::Sort(_op) => {
                if !all_referenced {
                    // Todo: Order Project
                    // https://github.com/duckdb/duckdb/blob/main/src/optimizer/remove_unused_columns.cpp#L174
                }
                for child_id in graph.children_at(node_id).collect_vec() {
                    Self::_apply(column_references, true, child_id, graph);
                }
            }
            Operator::Scan(op) => {
                if !all_referenced {
                    Self::clear_exprs(column_references, &mut op.columns);
                }
            }
            Operator::Limit(_) | Operator::Join(_) | Operator::Filter(_) => {
                for column in operator.referenced_columns(false) {
                    column_references.insert(column.summary().clone());
                }
                for child_id in graph.children_at(node_id).collect_vec() {
                    Self::_apply(column_references, all_referenced, child_id, graph);
                }
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) => (),
            // DDL Based on Other Plan
            Operator::Insert(_) | Operator::Update(_) | Operator::Delete(_) => {
                let op_ref_columns = operator.referenced_columns(false);

                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::recollect_apply(op_ref_columns, true, child_id, graph);
                } else {
                    unreachable!();
                }
            }
            // DDL Single Plan
            Operator::CreateTable(_)
            | Operator::DropTable(_)
            | Operator::Truncate(_)
            | Operator::Show(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_) => (),
        }
    }

    fn recollect_apply(
        referenced_columns: Vec<ColumnRef>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) {
        for child_id in graph.children_at(node_id).collect_vec() {
            let mut new_references: HashSet<ColumnSummary> = referenced_columns
                .iter()
                .map(|column| column.summary())
                .cloned()
                .collect();

            Self::_apply(&mut new_references, all_referenced, child_id, graph);
        }
    }
}


impl MatchPattern for ColumnPruning {
    fn pattern(&self) -> &Pattern {
        &COLUMN_PRUNING_RULE
    }
}

impl NormalizationRule for ColumnPruning {

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        Self::_apply(&mut HashSet::new(), true, node_id, graph);
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::db::DatabaseError;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;

    #[tokio::test]
    async fn test_column_pruning() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select c1, c3 from t1 left join t2 on c1 = c3").await?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_column_pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .find_best()?;

        assert_eq!(best_plan.childrens.len(), 1);
        match best_plan.operator {
            Operator::Project(op) => {
                assert_eq!(op.exprs.len(), 2);
            }
            _ => unreachable!("Should be a project operator"),
        }
        match &best_plan.childrens[0].operator {
            Operator::Join(op) => match &op.on {
                JoinCondition::On { on, filter } => {
                    assert_eq!(on.len(), 1);
                    assert!(filter.is_none());
                }
                _ => unreachable!("Should be a on condition"),
            },
            _ => unreachable!("Should be a join operator"),
        }

        assert_eq!(best_plan.childrens[0].childrens.len(), 2);

        for grandson_plan in &best_plan.childrens[0].childrens {
            match &grandson_plan.operator {
                Operator::Scan(op) => {
                    assert_eq!(op.columns.len(), 1);
                }
                _ => unreachable!("Should be a scan operator"),
            }
        }

        Ok(())
    }
}
