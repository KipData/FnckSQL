use std::sync::Arc;
use crate::expression::value_compute::binary_op_tp;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};

impl ScalarExpression {
    pub fn eval_column(&self, tuple: &Tuple) -> ValueRef {
        match &self {
            ScalarExpression::Constant(val) =>
                val.clone(),
            ScalarExpression::ColumnRef(col) => {
                let index = tuple
                    .columns
                    .binary_search_by(|tul_col| tul_col.name.cmp(&col.name))
                    .unwrap();

                tuple.values[index].clone()
            },
            ScalarExpression::InputRef{ index, .. } =>
                tuple.values[*index].clone(),
            ScalarExpression::Alias{ expr, .. } =>
                expr.eval_column(tuple),
            ScalarExpression::TypeCast{ expr, ty, .. } => {
                let value = expr.eval_column(tuple);

                Arc::new(DataValue::clone(&value).cast(ty))
            }
            ScalarExpression::Binary{ left_expr, right_expr, op, .. } => {
                let left = left_expr.eval_column(tuple);
                let right = right_expr.eval_column(tuple);
                Arc::new(binary_op_tp(&left, &right, op))
            }
            ScalarExpression::IsNull{ expr } => {
                Arc::new(DataValue::Boolean(Some(expr.nullable())))
            }
            ScalarExpression::Unary{ .. } => todo!(),
            ScalarExpression::AggCall{ .. } => todo!()
        }
    }
}