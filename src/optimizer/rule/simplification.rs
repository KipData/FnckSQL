use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use lazy_static::lazy_static;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::types::value::{DataValue, ValueRef};
lazy_static! {
    static ref LIKE_REWRITE_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::None,
        }
    };
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
    fn _apply(node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
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
            Operator::Scan(op) => {
                for expr in &mut op.columns {
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
        for child_id in graph.children_at(node_id) {
            Self::_apply(child_id, graph)?;
        }

        Ok(())
    }
}

impl Rule for ConstantCalculation {
    fn pattern(&self) -> &Pattern {
        &CONSTANT_CALCULATION_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        Self::_apply(node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
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
            filter_op.predicate.constant_calculation()?;

            graph.replace_node(node_id, Operator::Filter(filter_op))
        }

        Ok(())
    }
}

pub struct LikeRewrite;

impl Rule for LikeRewrite {
    fn pattern(&self) -> &Pattern {
        &LIKE_REWRITE_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(mut filter_op) = graph.operator(node_id).clone() {
            // if is like expression
            if let ScalarExpression::Binary {
                op: BinaryOperator::Like,
                left_expr,
                right_expr,
                ty,
            } = &mut filter_op.predicate
            {
                // if left is column and right is constant
                if let ScalarExpression::ColumnRef(_) = left_expr.as_ref() {
                    if let ScalarExpression::Constant(value) = right_expr.as_ref() {
                        match value.as_ref() {
                            DataValue::Utf8(val_str) => {
                                let mut value = val_str.clone().unwrap_or_else(|| "".to_string());

                                if value.ends_with('%') {
                                    value.pop(); // remove '%'
                                    if let Some(last_char) = value.clone().pop() {
                                        if let Some(next_char) = increment_char(last_char) {
                                            let mut new_value = value.clone();
                                            new_value.pop();
                                            new_value.push(next_char);

                                            let new_expr = ScalarExpression::Binary {
                                                op: BinaryOperator::And,
                                                left_expr: Box::new(ScalarExpression::Binary {
                                                    op: BinaryOperator::GtEq,
                                                    left_expr: left_expr.clone(),
                                                    right_expr: Box::new(ScalarExpression::Constant(ValueRef::from(DataValue::Utf8(Some(value))))),
                                                    ty: ty.clone(),
                                                }),
                                                right_expr: Box::new(ScalarExpression::Binary {
                                                    op: BinaryOperator::Lt,
                                                    left_expr: left_expr.clone(),
                                                    right_expr: Box::new(ScalarExpression::Constant(ValueRef::from(DataValue::Utf8(Some(new_value))))),
                                                    ty: ty.clone(),
                                                }),
                                                ty: ty.clone(),
                                            };
                                            filter_op.predicate = new_expr;
                                        }
                                    }
                                }
                            }
                            _ => {
                                graph.version += 1;
                                return Ok(());
                            }
                        }
                    }
                }
            }
            graph.replace_node(node_id, Operator::Filter(filter_op))
        }
        // mark changed to skip this rule batch
        graph.version += 1;
        Ok(())
    }
}

fn increment_char(v: char) -> Option<char> {
    match v {
        'z' => None,
        'Z' => None,
        _  => std::char::from_u32(v as u32 + 1),
    }
}


#[cfg(test)]
mod test {
    use crate::binder::test::select_sql_run;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::db::DatabaseError;
    use crate::expression::simplify::ConstantBinary;
    use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::Operator;
    use crate::planner::LogicalPlan;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use std::sync::Arc;
    use crate::optimizer::rule::simplification::increment_char;


    #[test]
    fn test_increment_char() {
        assert_eq!(increment_char('a'), Some('b'));
        assert_eq!(increment_char('z'), None);
        assert_eq!(increment_char('A'), Some('B'));
    }

    #[tokio::test]
    async fn test_constant_calculation_omitted() -> Result<(), DatabaseError> {
        // (2 + (-1)) < -(c1 + 1)
        let plan =
            select_sql_run("select c1 + (2 + 1), 2 + 1 from t1 where (2 + (-1)) < -(c1 + 1)")
                .await?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_simplification".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::SimplifyFilter, RuleImpl::ConstantCalculation],
            )
            .find_best()?;
        if let Operator::Project(project_op) = best_plan.clone().operator {
            let constant_expr = ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(3))));
            if let ScalarExpression::Binary { right_expr, .. } = &project_op.exprs[0] {
                assert_eq!(right_expr.as_ref(), &constant_expr);
            } else {
                unreachable!();
            }
            assert_eq!(&project_op.exprs[1], &constant_expr);
        } else {
            unreachable!();
        }
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            let column_binary = filter_op.predicate.convert_binary(&0).unwrap();
            let final_binary = ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-2)))),
            };
            assert_eq!(column_binary, Some(final_binary));
        } else {
            unreachable!();
        }

        Ok(())
    }

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

        // c1 < 24
        let plan_9 = select_sql_run("select * from t1 where (-1 - c1) + 1 > 24").await?;

        // c1 < 24
        let plan_10 = select_sql_run("select * from t1 where 24 < (-1 - c1) + 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<ConstantBinary>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter],
                )
                .find_best()?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                println!(
                    "{expr}: {:#?}",
                    filter_op.predicate.convert_binary(&0).unwrap()
                );

                Ok(filter_op.predicate.convert_binary(&0).unwrap())
            } else {
                Ok(None)
            }
        };

        let op_1 = op(plan_1, "-(c1 + 1) > 1")?;
        let op_2 = op(plan_2, "-(1 - c1) > 1")?;
        let op_3 = op(plan_3, "-c1 > 1")?;
        let op_4 = op(plan_4, "c1 + 1 > 1")?;
        let op_5 = op(plan_9, "(-1 - c1) + 1 > 24")?;

        assert!(op_1.is_some());
        assert!(op_2.is_some());
        assert!(op_3.is_some());
        assert!(op_4.is_some());
        assert!(op_5.is_some());

        assert_eq!(op_1, op(plan_5, "1 < -(c1 + 1)")?);
        assert_eq!(op_2, op(plan_6, "1 < -(1 - c1)")?);
        assert_eq!(op_3, op(plan_7, "1 < -c1")?);
        assert_eq!(op_4, op(plan_8, "1 < c1 + 1")?);
        assert_eq!(op_5, op(plan_10, "24 < (-1 - c1) + 1")?);

        Ok(())
    }

    #[tokio::test]
    async fn test_simplify_filter_repeating_column() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select * from t1 where -(c1 + 1) > c2").await?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::SimplifyFilter],
            )
            .find_best()?;
        if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
            let c1_col = ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(0),
                    name: "c1".to_string(),
                    table_name: Some(Arc::new("t1".to_string())),
                },
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: true,
                    is_unique: false,
                },
                ref_expr: None,
            };
            let c2_col = ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(1),
                    name: "c2".to_string(),
                    table_name: Some(Arc::new("t1".to_string())),
                },
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: false,
                    is_unique: true,
                },
                ref_expr: None,
            };

            // -(c1 + 1) > c2 => c1 < -c2 - 1
            assert_eq!(
                filter_op.predicate,
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    left_expr: Box::new(ScalarExpression::Unary {
                        op: UnaryOperator::Minus,
                        expr: Box::new(ScalarExpression::Binary {
                            op: BinaryOperator::Plus,
                            left_expr: Box::new(ScalarExpression::ColumnRef(Arc::new(c1_col))),
                            right_expr: Box::new(ScalarExpression::Constant(Arc::new(
                                DataValue::Int32(Some(1))
                            ))),
                            ty: LogicalType::Integer,
                        }),
                        ty: LogicalType::Integer,
                    }),
                    right_expr: Box::new(ScalarExpression::ColumnRef(Arc::new(c2_col))),
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
        let plan_1 =
            select_sql_run("select * from t1 where -(c1 + 1) > 1 and -(1 - c2) > 1").await?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 =
            select_sql_run("select * from t1 where -(1 - c1) > 1 and -(c2 + 1) > 1").await?;
        // c1 < -1
        let plan_3 = select_sql_run("select * from t1 where -c1 > 1 and c2 + 1 > 1").await?;
        // c1 > 0
        let plan_4 = select_sql_run("select * from t1 where c1 + 1 > 1 and -c2 > 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<FilterOperator>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter],
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
        assert_eq!(
            cb_1_c1,
            Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-2)))),
            })
        );

        let cb_1_c2 = op_1.predicate.convert_binary(&1).unwrap();
        println!("op_1 => c2: {:#?}", cb_1_c2);
        assert_eq!(
            cb_1_c2,
            Some(ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(2)))),
                max: Bound::Unbounded,
            })
        );

        let cb_2_c1 = op_2.predicate.convert_binary(&0).unwrap();
        println!("op_2 => c1: {:#?}", cb_2_c1);
        assert_eq!(
            cb_2_c1,
            Some(ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(2)))),
                max: Bound::Unbounded,
            })
        );

        let cb_2_c2 = op_2.predicate.convert_binary(&1).unwrap();
        println!("op_2 => c2: {:#?}", cb_2_c2);
        assert_eq!(
            cb_1_c1,
            Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-2)))),
            })
        );

        let cb_3_c1 = op_3.predicate.convert_binary(&0).unwrap();
        println!("op_3 => c1: {:#?}", cb_3_c1);
        assert_eq!(
            cb_3_c1,
            Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-1)))),
            })
        );

        let cb_3_c2 = op_3.predicate.convert_binary(&1).unwrap();
        println!("op_3 => c2: {:#?}", cb_3_c2);
        assert_eq!(
            cb_3_c2,
            Some(ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
                max: Bound::Unbounded,
            })
        );

        let cb_4_c1 = op_4.predicate.convert_binary(&0).unwrap();
        println!("op_4 => c1: {:#?}", cb_4_c1);
        assert_eq!(
            cb_4_c1,
            Some(ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
                max: Bound::Unbounded,
            })
        );

        let cb_4_c2 = op_4.predicate.convert_binary(&1).unwrap();
        println!("op_4 => c2: {:#?}", cb_4_c2);
        assert_eq!(
            cb_4_c2,
            Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(-1)))),
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_simplify_filter_multiple_column_in_or() -> Result<(), DatabaseError> {
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = select_sql_run("select * from t1 where c1 > c2 or c1 > 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<FilterOperator>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter],
                )
                .find_best()?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                println!("{expr}: {:#?}", filter_op);

                Ok(Some(filter_op))
            } else {
                Ok(None)
            }
        };

        let op_1 = op(plan_1, "c1 > c2 or c1 > 1")?.unwrap();

        let cb_1_c1 = op_1.predicate.convert_binary(&0).unwrap();
        println!("op_1 => c1: {:#?}", cb_1_c1);
        assert_eq!(cb_1_c1, None);

        Ok(())
    }
}
