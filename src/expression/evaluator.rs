use std::sync::Arc;
use itertools::Itertools;
use crate::expression::value_compute::{binary_op, unary_op};
use crate::expression::ScalarExpression;
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};

impl ScalarExpression {
    pub fn eval_column(&self, tuple: &Tuple) -> Result<ValueRef, TypeError> {
        match &self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let (index, _) = tuple
                    .columns
                    .iter()
                    .find_position(|tul_col| tul_col.name == col.name)
                    .unwrap();

                Ok(tuple.values[index].clone())
            },
            ScalarExpression::InputRef{ index, .. } => Ok(tuple.values[*index].clone()),
            ScalarExpression::Alias{ expr, .. } => expr.eval_column(tuple),
            ScalarExpression::TypeCast{ expr, ty, .. } => {
                let value = expr.eval_column(tuple)?;

                Ok(Arc::new(DataValue::clone(&value).cast(ty)?))
            }
            ScalarExpression::Binary{ left_expr, right_expr, op, .. } => {
                let left = left_expr.eval_column(tuple)?;
                let right = right_expr.eval_column(tuple)?;

                Ok(Arc::new(binary_op(&left, &right, op)?))
            }
            ScalarExpression::IsNull{ expr } => {
                Ok(Arc::new(DataValue::Boolean(Some(expr.nullable()))))
            }
            ScalarExpression::Unary{ expr, op, .. } => {
                let value = expr.eval_column(tuple)?;

                Ok(Arc::new(unary_op(&value, op)?))
            },
            ScalarExpression::AggCall{ .. } => todo!()
        }
    }
}