use crate::errors::DatabaseError;
use crate::expression::function::ScalarFunction;
use crate::expression::{AliasType, ScalarExpression};
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
        match self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let value = tuple
                    .schema_ref
                    .iter()
                    .find_position(|tul_col| tul_col.summary() == col.summary())
                    .map(|(i, _)| &tuple.values[i])
                    .unwrap_or(&NULL_VALUE)
                    .clone();

                Ok(value)
            }
            ScalarExpression::Alias { expr, alias } => {
                if let Some(value) = tuple
                    .schema_ref
                    .iter()
                    .find_position(|tul_col| match alias {
                        AliasType::Name(alias) => {
                            tul_col.table_name().is_none() && tul_col.name() == alias
                        }
                        AliasType::Expr(alias_expr) => {
                            alias_expr.output_column().summary == tul_col.summary
                        }
                    })
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

                Ok(Arc::new(DataValue::binary_op(&left, &right, op)?))
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

                Ok(Arc::new(DataValue::unary_op(&value, op)?))
            }
            ScalarExpression::AggCall { .. } => {
                unreachable!("must use `NormalizationRuleImpl::ExpressionRemapper`")
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
            ScalarExpression::Reference { pos, .. } => {
                return Ok(tuple
                    .values
                    .get(*pos)
                    .unwrap_or_else(|| &NULL_VALUE)
                    .clone());
            }
            ScalarExpression::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    values.push(expr.eval(tuple)?);
                }
                Ok(Arc::new(DataValue::Tuple(
                    (!values.is_empty()).then_some(values),
                )))
            }
            ScalarExpression::Function(ScalarFunction { inner, args, .. }) => Ok(Arc::new(
                inner.eval(args, tuple)?.cast(inner.return_type())?,
            )),
            ScalarExpression::Empty => unreachable!(),
        }
    }
}
