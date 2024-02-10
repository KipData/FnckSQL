use crate::catalog::ColumnSummary;
use crate::errors::DatabaseError;
use crate::expression::value_compute::{binary_op, unary_op};
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::cmp::Ordering;
use std::sync::Arc;

lazy_static! {
    static ref NULL_VALUE: ValueRef = Arc::new(DataValue::Null);
}

macro_rules! eval_to_num {
    ($num_expr:expr, $tuple:expr) => {
        if let Some(num_i32) = DataValue::clone($num_expr.eval($tuple)?.as_ref())
            .cast(&LogicalType::Integer)?
            .i32()
        {
            num_i32 as usize
        } else {
            return Ok(Arc::new(DataValue::Utf8(None)));
        }
    };
}

impl ScalarExpression {
    pub fn eval(&self, tuple: &Tuple) -> Result<ValueRef, DatabaseError> {
        if let Some(value) = Self::eval_with_summary(tuple, self.output_column().summary()) {
            return Ok(value.clone());
        }

        match &self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let value = Self::eval_with_summary(tuple, col.summary())
                    .unwrap_or(&NULL_VALUE)
                    .clone();

                Ok(value)
            }
            ScalarExpression::Alias { expr, alias } => {
                if let Some(value) = tuple
                    .schema_ref
                    .iter()
                    .find_position(|tul_col| tul_col.name() == alias)
                    .map(|(i, _)| &tuple.values[i])
                {
                    return Ok(value.clone());
                }

                expr.eval(tuple)
            }
            ScalarExpression::TypeCast { expr, ty, .. } => {
                let value = expr.eval(tuple)?;

                Ok(Arc::new(DataValue::clone(&value).cast(ty)?))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => {
                let left = left_expr.eval(tuple)?;
                let right = right_expr.eval(tuple)?;

                Ok(Arc::new(binary_op(&left, &right, op)?))
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = expr.eval(tuple)?.is_null();
                if *negated {
                    is_null = !is_null;
                }
                Ok(Arc::new(DataValue::Boolean(Some(is_null))))
            }
            ScalarExpression::In {
                expr,
                args,
                negated,
            } => {
                let value = expr.eval(tuple)?;
                if value.is_null() {
                    return Ok(Arc::new(DataValue::Boolean(None)));
                }
                let mut is_in = false;
                for arg in args {
                    let arg_value = arg.eval(tuple)?;

                    if arg_value.is_null() {
                        return Ok(Arc::new(DataValue::Boolean(None)));
                    }
                    if arg_value == value {
                        is_in = true;
                        break;
                    }
                }
                if *negated {
                    is_in = !is_in;
                }
                Ok(Arc::new(DataValue::Boolean(Some(is_in))))
            }
            ScalarExpression::Unary { expr, op, .. } => {
                let value = expr.eval(tuple)?;

                Ok(Arc::new(unary_op(&value, op)?))
            }
            ScalarExpression::AggCall { .. } => {
                let value = Self::eval_with_summary(tuple, self.output_column().summary())
                    .unwrap_or(&NULL_VALUE)
                    .clone();

                Ok(value)
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                negated,
            } => {
                let value = expr.eval(tuple)?;
                let left = left_expr.eval(tuple)?;
                let right = right_expr.eval(tuple)?;

                let mut is_between = match (
                    value.partial_cmp(&left).map(Ordering::is_ge),
                    value.partial_cmp(&right).map(Ordering::is_le),
                ) {
                    (Some(true), Some(true)) => true,
                    (None, _) | (_, None) => return Ok(Arc::new(DataValue::Boolean(None))),
                    _ => false,
                };
                if *negated {
                    is_between = !is_between;
                }
                Ok(Arc::new(DataValue::Boolean(Some(is_between))))
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                if let Some(mut string) = DataValue::clone(expr.eval(tuple)?.as_ref())
                    .cast(&LogicalType::Varchar(None))?
                    .utf8()
                {
                    if let Some(from_expr) = from_expr {
                        string = string.split_off(eval_to_num!(from_expr, tuple).saturating_sub(1));
                    }
                    if let Some(for_expr) = for_expr {
                        let _ = string.split_off(eval_to_num!(for_expr, tuple));
                    }

                    Ok(Arc::new(DataValue::Utf8(Some(string))))
                } else {
                    Ok(Arc::new(DataValue::Utf8(None)))
                }
            }
            ScalarExpression::Empty => unreachable!(),
        }
    }

    fn eval_with_summary<'a>(tuple: &'a Tuple, summary: &ColumnSummary) -> Option<&'a ValueRef> {
        tuple
            .schema_ref
            .iter()
            .find_position(|tul_col| tul_col.summary() == summary)
            .map(|(i, _)| &tuple.values[i])
    }
}
