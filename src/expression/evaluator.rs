use std::sync::Arc;
use itertools::Itertools;
use lazy_static::lazy_static;
use crate::expression::value_compute::{binary_op, unary_op};
use crate::expression::ScalarExpression;
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};

lazy_static! {
    static ref NULL_VALUE: ValueRef = {
        Arc::new(DataValue::Null)
    };
}

impl ScalarExpression {
    pub fn eval_column(&self, tuple: &Tuple) -> Result<ValueRef, TypeError> {
        match &self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let value = tuple
                    .columns
                    .iter()
                    .find_position(|tul_col| tul_col.name == col.name)
                    .map(|(i, _)| &tuple.values[i])
                    .unwrap_or(&NULL_VALUE)
                    .clone();

                Ok(value)
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
                let value = expr.eval_column(tuple)?;

                Ok(Arc::new(DataValue::Boolean(Some(value.is_null()))))
            },
            ScalarExpression::Unary{ expr, op, .. } => {
                let value = expr.eval_column(tuple)?;

                Ok(Arc::new(unary_op(&value, op)?))
            },
            ScalarExpression::AggCall{ .. } => todo!()
        }
    }
}