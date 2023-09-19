use lazy_static::lazy_static;
use crate::optimizer::core::opt_expr::OptExprNode;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::Operator;
lazy_static! {
    static ref SIMPLIFY_FILTER_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Copy, Clone)]
pub struct SimplifyFilter;

impl Rule for SimplifyFilter {
    fn pattern(&self) -> &Pattern {
        &SIMPLIFY_FILTER_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(mut filter_op) = graph.operator(node_id).clone() {
            filter_op.predicate.simplify()?;

            graph.replace_node(
                node_id,
                OptExprNode::OperatorRef(Operator::Filter(filter_op))
            )
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::Bound;
    use std::sync::Arc;
    use crate::binder::test::select_sql_run;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::DatabaseError;
    use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::expression::simplify::ConstantBinary;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::LogicalPlan;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::Operator;
    use crate::types::LogicalType;
    use crate::types::value::DataValue;

    #[tokio::test]
    async fn test_simplify_filter_single_column() -> Result<(), DatabaseError> {
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = select_sql_run("select * from t1 where -(c1 + 1) > 1").await?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 = select_sql_run("select * from t1 where -(1 - c1) > 1").await?;
        // c1 < -1
        let plan_3 = select_sql_run("select * from t1 where -c1 > 1").await?;
        // c1 > 0
        let plan_4 = select_sql_run("select * from t1 where c1 + 1 > 1").await?;

        // c1 + 1 < -1 => c1 < -2
        let plan_5 = select_sql_run("select * from t1 where 1 < -(c1 + 1)").await?;
        // 1 - c1 < -1 => c1 > 2
        let plan_6 = select_sql_run("select * from t1 where 1 < -(1 - c1)").await?;
        // c1 < -1
        let plan_7 = select_sql_run("select * from t1 where 1 < -c1").await?;
        // c1 > 0
        let plan_8 = select_sql_run("select * from t1 where 1 < c1 + 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<ConstantBinary>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter]
                )
                .find_best()?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                println!("{expr}: {:#?}", filter_op.predicate.convert_binary(&0).unwrap());

                Ok(filter_op.predicate.convert_binary(&0).unwrap())
            } else {
                Ok(None)
            }
        };

        assert_eq!(op(plan_1, "-(c1 + 1) > 1")?, op(plan_5, "1 < -(c1 + 1)")?);
        assert_eq!(op(plan_2, "-(1 - c1) > 1")?, op(plan_6, "1 < -(1 - c1)")?);
        assert_eq!(op(plan_3, "-c1 > 1")?, op(plan_7, "1 < -c1")?);
        assert_eq!(op(plan_4, "c1 + 1 > 1")?, op(plan_8, "1 < c1 + 1")?);

        Ok(())
    }

    #[tokio::test]
    async fn test_simplify_filter_repeating_column() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select * from t1 where -(c1 + 1) > c2").await?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::SimplifyFilter]
            )
            .find_best()?;
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            let c1_col = ColumnCatalog {
                id: Some(
                    0,
                ),
                name: "c1".to_string(),
                table_name: Some(
                    Arc::new("t1".to_string()),
                ),
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: true,
                    is_unique: true,
                },
            };
            let c2_col = ColumnCatalog {
                id: Some(
                    1,
                ),
                name: "c2".to_string(),
                table_name: Some(
                    Arc::new("t1".to_string()),
                ),
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: false,
                    is_unique: false,
                },
            };

            // -(c1 + 1) > c2 => c1 < -c2 - 1
            assert_eq!(
                filter_op.predicate,
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    left_expr: Box::new(ScalarExpression::ColumnRef(Arc::new(c1_col))),
                    right_expr: Box::new(ScalarExpression::Binary {
                        op: BinaryOperator::Minus,
                        left_expr: Box::new(ScalarExpression::Unary {
                            op: UnaryOperator::Minus,
                            expr: Box::new(ScalarExpression::ColumnRef(Arc::new(c2_col))),
                            ty: LogicalType::Integer,
                        }),
                        right_expr: Box::new(ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(1))))),
                        ty: LogicalType::Integer,
                    }),
                    ty: LogicalType::Boolean,
                }
            )
        } else {
            unreachable!()
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_simplify_filter_multiple_column() -> Result<(), DatabaseError> {
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = select_sql_run("select * from t1 where -(c1 + 1) > 1 and -(1 - c2) > 1").await?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 = select_sql_run("select * from t1 where -(1 - c1) > 1 and -(c2 + 1) > 1").await?;
        // c1 < -1
        let plan_3 = select_sql_run("select * from t1 where -c1 > 1 and c2 + 1 > 1").await?;
        // c1 > 0
        let plan_4 = select_sql_run("select * from t1 where c1 + 1 > 1 and -c2 > 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<FilterOperator>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter]
                )
                .find_best()?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                println!("{expr}: {:#?}", filter_op);

                Ok(Some(filter_op))
            } else {
                Ok(None)
            }
        };

        let op_1 = op(plan_1, "-(c1 + 1) > 1 and -(1 - c2) > 1")?.unwrap();
        let op_2 = op(plan_2, "-(1 - c1) > 1 and -(c2 + 1) > 1")?.unwrap();
        let op_3 = op(plan_3, "-c1 > 1 and c2 + 1 > 1")?.unwrap();
        let op_4 = op(plan_4, "c1 + 1 > 1 and -c2 > 1")?.unwrap();

        let cb_1_c1 = op_1.predicate.convert_binary(&0).unwrap();
        println!("op_1 => c1: {:#?}", cb_1_c1);
        assert_eq!(cb_1_c1, Some(ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-2))))
        }));

        let cb_1_c2 = op_1.predicate.convert_binary(&1).unwrap();
        println!("op_1 => c2: {:#?}", cb_1_c2);
        assert_eq!(cb_1_c2, Some(ConstantBinary::Scope {
            min: Bound::Excluded(Arc::new(DataValue::Int32(Some(2)))),
            max: Bound::Unbounded
        }));

        let cb_2_c1 = op_2.predicate.convert_binary(&0).unwrap();
        println!("op_2 => c1: {:#?}", cb_2_c1);
        assert_eq!(cb_2_c1, Some(ConstantBinary::Scope {
            min: Bound::Excluded(Arc::new(DataValue::Int32(Some(2)))),
            max: Bound::Unbounded
        }));

        let cb_2_c2 = op_2.predicate.convert_binary(&1).unwrap();
        println!("op_2 => c2: {:#?}", cb_2_c2);
        assert_eq!(cb_1_c1, Some(ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-2))))
        }));

        let cb_3_c1 = op_3.predicate.convert_binary(&0).unwrap();
        println!("op_3 => c1: {:#?}", cb_3_c1);
        assert_eq!(cb_3_c1, Some(ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-1))))
        }));

        let cb_3_c2 = op_3.predicate.convert_binary(&1).unwrap();
        println!("op_3 => c2: {:#?}", cb_3_c2);
        assert_eq!(cb_3_c2, Some(ConstantBinary::Scope {
            min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
            max: Bound::Unbounded
        }));

        let cb_4_c1 = op_4.predicate.convert_binary(&0).unwrap();
        println!("op_4 => c1: {:#?}", cb_4_c1);
        assert_eq!(cb_4_c1, Some(ConstantBinary::Scope {
            min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
            max: Bound::Unbounded
        }));

        let cb_4_c2 = op_4.predicate.convert_binary(&1).unwrap();
        println!("op_4 => c2: {:#?}", cb_4_c2);
        assert_eq!(cb_4_c2, Some(ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-1))))
        }));

        Ok(())
    }
}