use crate::errors::DatabaseError;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use itertools::Itertools;
use lazy_static::lazy_static;
lazy_static! {
    static ref CONSTANT_CALCULATION_RULE: Pattern = {
        Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    };
    static ref SIMPLIFY_FILTER_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| !matches!(op, Operator::Aggregate(_)),
                children: PatternChildrenPredicate::Recursive,
            }]),
        }
    };
}

#[derive(Copy, Clone)]
pub struct ConstantCalculation;

impl ConstantCalculation {
    fn _apply(node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Aggregate(op) => {
                for expr in op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()) {
                    expr.constant_calculation()?;
                }
            }
            Operator::Filter(op) => {
                op.predicate.constant_calculation()?;
            }
            Operator::Join(op) => {
                if let JoinCondition::On { on, filter } = &mut op.on {
                    for (left_expr, right_expr) in on {
                        left_expr.constant_calculation()?;
                        right_expr.constant_calculation()?;
                    }
                    if let Some(expr) = filter {
                        expr.constant_calculation()?;
                    }
                }
            }
            Operator::Project(op) => {
                for expr in &mut op.exprs {
                    expr.constant_calculation()?;
                }
            }
            Operator::Sort(op) => {
                for field in &mut op.sort_fields {
                    field.expr.constant_calculation()?;
                }
            }
            _ => (),
        }
        for child_id in graph.children_at(node_id).collect_vec() {
            Self::_apply(child_id, graph)?;
        }

        Ok(())
    }
}

impl MatchPattern for ConstantCalculation {
    fn pattern(&self) -> &Pattern {
        &CONSTANT_CALCULATION_RULE
    }
}

impl NormalizationRule for ConstantCalculation {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}

#[derive(Copy, Clone)]
pub struct SimplifyFilter;

impl MatchPattern for SimplifyFilter {
    fn pattern(&self) -> &Pattern {
        &SIMPLIFY_FILTER_RULE
    }
}

impl NormalizationRule for SimplifyFilter {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Operator::Filter(mut filter_op) = graph.operator(node_id).clone() {
            filter_op.predicate.simplify()?;
            filter_op.predicate.constant_calculation()?;

            graph.replace_node(node_id, Operator::Filter(filter_op))
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::binder::test::build_t1_table;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::{Range, RangeDetacher};
    use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::planner::LogicalPlan;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use crate::types::{ColumnId, LogicalType};
    use std::collections::Bound;
    use std::sync::Arc;

    #[test]
    fn test_constant_calculation_omitted() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // (2 + (-1)) < -(c1 + 1)
        let plan =
            table_state.plan("select c1 + (2 + 1), 2 + 1 from t1 where (2 + (-1)) < -(c1 + 1)")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_simplification".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::SimplifyFilter,
                    NormalizationRuleImpl::ConstantCalculation,
                ],
            )
            .find_best::<RocksTransaction>(None)?;
        if let Operator::Project(project_op) = best_plan.clone().operator {
            let constant_expr = ScalarExpression::Constant(DataValue::Int32(Some(3)));
            if let ScalarExpression::Binary { right_expr, .. } = &project_op.exprs[0] {
                debug_assert_eq!(right_expr.as_ref(), &constant_expr);
            } else {
                unreachable!();
            }
            debug_assert_eq!(&project_op.exprs[1], &constant_expr);
        } else {
            unreachable!();
        }
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&filter_op.predicate)
                .unwrap();
            debug_assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Excluded(DataValue::Int32(Some(-2))),
                }
            );
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[test]
    fn test_simplify_filter_single_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = table_state.plan("select * from t1 where -(c1 + 1) > 1")?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 = table_state.plan("select * from t1 where -(1 - c1) > 1")?;
        // c1 < -1
        let plan_3 = table_state.plan("select * from t1 where -c1 > 1")?;
        // c1 > 0
        let plan_4 = table_state.plan("select * from t1 where c1 + 1 > 1")?;

        // c1 + 1 < -1 => c1 < -2
        let plan_5 = table_state.plan("select * from t1 where 1 < -(c1 + 1)")?;
        // 1 - c1 < -1 => c1 > 2
        let plan_6 = table_state.plan("select * from t1 where 1 < -(1 - c1)")?;
        // c1 < -1
        let plan_7 = table_state.plan("select * from t1 where 1 < -c1")?;
        // c1 > 0
        let plan_8 = table_state.plan("select * from t1 where 1 < c1 + 1")?;
        // c1 < 24
        let plan_9 = table_state.plan("select * from t1 where (-1 - c1) + 1 > 24")?;
        // c1 < 24
        let plan_10 = table_state.plan("select * from t1 where 24 < (-1 - c1) + 1")?;

        let op = |plan: LogicalPlan| -> Result<Option<Range>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![NormalizationRuleImpl::SimplifyFilter],
                )
                .find_best::<RocksTransaction>(None)?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                Ok(
                    RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                        .detach(&filter_op.predicate),
                )
            } else {
                Ok(None)
            }
        };

        let op_1 = op(plan_1)?;
        let op_2 = op(plan_2)?;
        let op_3 = op(plan_3)?;
        let op_4 = op(plan_4)?;
        let op_5 = op(plan_9)?;

        debug_assert!(op_1.is_some());
        debug_assert!(op_2.is_some());
        debug_assert!(op_3.is_some());
        debug_assert!(op_4.is_some());
        debug_assert!(op_5.is_some());

        debug_assert_eq!(op_1, op(plan_5)?);
        debug_assert_eq!(op_2, op(plan_6)?);
        debug_assert_eq!(op_3, op(plan_7)?);
        debug_assert_eq!(op_4, op(plan_8)?);
        debug_assert_eq!(op_5, op(plan_10)?);

        Ok(())
    }

    #[test]
    fn test_simplify_filter_repeating_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 where -(c1 + 1) > c2")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .find_best::<RocksTransaction>(None)?;
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            let c1_col = ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c1".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: *table_state.column_id_by_name("c1"),
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?,
                false,
            );
            let c2_col = ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c2".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: *table_state.column_id_by_name("c2"),
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Integer, None, true, None)?,
                false,
            );

            // -(c1 + 1) > c2 => c1 < -c2 - 1
            debug_assert_eq!(
                filter_op.predicate,
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    left_expr: Box::new(ScalarExpression::Unary {
                        op: UnaryOperator::Minus,
                        expr: Box::new(ScalarExpression::Binary {
                            op: BinaryOperator::Plus,
                            left_expr: Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                                c1_col
                            ))),
                            right_expr: Box::new(ScalarExpression::Constant(DataValue::Int32(
                                Some(1)
                            ))),
                            evaluator: None,
                            ty: LogicalType::Integer,
                        }),
                        evaluator: None,
                        ty: LogicalType::Integer,
                    }),
                    right_expr: Box::new(ScalarExpression::ColumnRef(ColumnRef::from(c2_col))),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                }
            )
        } else {
            unreachable!()
        }

        Ok(())
    }

    fn plan_filter(
        plan: &LogicalPlan,
        column_id: &ColumnId,
    ) -> Result<Option<Range>, DatabaseError> {
        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .find_best::<RocksTransaction>(None)?;
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            Ok(RangeDetacher::new("t1", &column_id).detach(&filter_op.predicate))
        } else {
            Ok(None)
        }
    }

    #[test]
    fn test_simplify_filter_multiple_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = table_state.plan("select * from t1 where -(c1 + 1) > 1 and -(1 - c2) > 1")?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 = table_state.plan("select * from t1 where -(1 - c1) > 1 and -(c2 + 1) > 1")?;
        // c1 < -1
        let plan_3 = table_state.plan("select * from t1 where -c1 > 1 and c2 + 1 > 1")?;
        // c1 > 0
        let plan_4 = table_state.plan("select * from t1 where c1 + 1 > 1 and -c2 > 1")?;

        let range_1_c1 = plan_filter(&plan_1, table_state.column_id_by_name("c1"))?.unwrap();
        let range_1_c2 = plan_filter(&plan_1, table_state.column_id_by_name("c2"))?.unwrap();

        let range_2_c1 = plan_filter(&plan_2, table_state.column_id_by_name("c1"))?.unwrap();
        let range_2_c2 = plan_filter(&plan_2, table_state.column_id_by_name("c2"))?.unwrap();

        let range_3_c1 = plan_filter(&plan_3, table_state.column_id_by_name("c1"))?.unwrap();
        let range_3_c2 = plan_filter(&plan_3, table_state.column_id_by_name("c2"))?.unwrap();

        let range_4_c1 = plan_filter(&plan_4, table_state.column_id_by_name("c1"))?.unwrap();
        let range_4_c2 = plan_filter(&plan_4, table_state.column_id_by_name("c2"))?.unwrap();

        debug_assert_eq!(
            range_1_c1,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(Some(-2)))
            }
        );
        debug_assert_eq!(
            range_1_c2,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(Some(2))),
                max: Bound::Unbounded
            }
        );
        debug_assert_eq!(
            range_2_c1,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(Some(2))),
                max: Bound::Unbounded
            }
        );
        debug_assert_eq!(
            range_2_c2,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(Some(-2)))
            }
        );
        debug_assert_eq!(
            range_3_c1,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(Some(-1)))
            }
        );
        debug_assert_eq!(
            range_3_c2,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(Some(0))),
                max: Bound::Unbounded
            }
        );
        debug_assert_eq!(
            range_4_c1,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(Some(0))),
                max: Bound::Unbounded
            }
        );
        debug_assert_eq!(
            range_4_c2,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(Some(-1)))
            }
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_multiple_column_in_or() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // c1 > c2 or c1 > 1
        let plan_1 = table_state.plan("select * from t1 where c1 > c2 or c1 > 1")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            None
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_multiple_dispersed_same_column_in_or() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan_1 = table_state.plan("select * from t1 where c1 = 4 and c1 > c2 or c1 > 1")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            Some(Range::Scope {
                min: Bound::Excluded(DataValue::Int32(Some(1))),
                max: Bound::Unbounded,
            })
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_is_null() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan_1 = table_state.plan("select * from t1 where c1 is null")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            Some(Range::Eq(DataValue::Null))
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_is_not_null() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan_1 = table_state.plan("select * from t1 where c1 is not null")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            None
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_in() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan_1 = table_state.plan("select * from t1 where c1 in (1, 2, 3)")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            Some(Range::SortedRanges(vec![
                Range::Eq(DataValue::Int32(Some(1))),
                Range::Eq(DataValue::Int32(Some(2))),
                Range::Eq(DataValue::Int32(Some(3))),
            ]))
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_not_in() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan_1 = table_state.plan("select * from t1 where c1 not in (1, 2, 3)")?;

        debug_assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"))?,
            None
        );

        Ok(())
    }
}
