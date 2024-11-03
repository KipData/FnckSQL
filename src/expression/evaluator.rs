use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression};
use crate::types::evaluator::EvaluatorFactory;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type, ValueRef};
use crate::types::LogicalType;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::ast::{CharLengthUnits, TrimWhereField};
use std::cmp;
use std::cmp::Ordering;
use std::sync::Arc;

lazy_static! {
    static ref NULL_VALUE: ValueRef = Arc::new(DataValue::Null);
}

macro_rules! eval_to_num {
    ($num_expr:expr, $tuple:expr, $schema:expr) => {
        if let Some(num_i32) = DataValue::clone($num_expr.eval($tuple, $schema)?.as_ref())
            .cast(&LogicalType::Integer)?
            .i32()
        {
            num_i32
        } else {
            return Ok(Arc::new(DataValue::Utf8 {
                value: None,
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }));
        }
    };
}

impl ScalarExpression {
    pub fn eval(&self, tuple: &Tuple, schema: &[ColumnRef]) -> Result<ValueRef, DatabaseError> {
        let check_cast = |value: ValueRef, return_type: &LogicalType| {
            if value.logical_type() != *return_type {
                return Ok(Arc::new(DataValue::clone(&value).cast(return_type)?));
            }
            Ok(value)
        };

        match self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let value = schema
                    .iter()
                    .find_position(|tul_col| tul_col.summary() == col.summary())
                    .map(|(i, _)| &tuple.values[i])
                    .unwrap_or(&NULL_VALUE)
                    .clone();

                Ok(value)
            }
            ScalarExpression::Alias { expr, alias } => {
                if let Some(value) = schema
                    .iter()
                    .find_position(|tul_col| match alias {
                        AliasType::Name(alias) => {
                            tul_col.table_name().is_none() && tul_col.name() == alias
                        }
                        AliasType::Expr(alias_expr) => {
                            alias_expr.output_column().summary() == tul_col.summary()
                        }
                    })
                    .map(|(i, _)| &tuple.values[i])
                {
                    return Ok(value.clone());
                }

                expr.eval(tuple, schema)
            }
            ScalarExpression::TypeCast { expr, ty, .. } => {
                let value = expr.eval(tuple, schema)?;

                Ok(Arc::new(DataValue::clone(&value).cast(ty)?))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                evaluator,
                ..
            } => {
                let left = left_expr.eval(tuple, schema)?;
                let right = right_expr.eval(tuple, schema)?;

                Ok(Arc::new(
                    evaluator
                        .as_ref()
                        .ok_or(DatabaseError::EvaluatorNotFound)?
                        .0
                        .binary_eval(&left, &right),
                ))
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = expr.eval(tuple, schema)?.is_null();
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
                let value = expr.eval(tuple, schema)?;
                if value.is_null() {
                    return Ok(Arc::new(DataValue::Boolean(None)));
                }
                let mut is_in = false;
                for arg in args {
                    let arg_value = arg.eval(tuple, schema)?;

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
            ScalarExpression::Unary {
                expr, evaluator, ..
            } => {
                let value = expr.eval(tuple, schema)?;

                Ok(Arc::new(
                    evaluator
                        .as_ref()
                        .ok_or(DatabaseError::EvaluatorNotFound)?
                        .0
                        .unary_eval(&value),
                ))
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
                let value = expr.eval(tuple, schema)?;
                let left = left_expr.eval(tuple, schema)?;
                let right = right_expr.eval(tuple, schema)?;

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
                if let Some(mut string) = DataValue::clone(expr.eval(tuple, schema)?.as_ref())
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                {
                    if let Some(from_expr) = from_expr {
                        let mut from = eval_to_num!(from_expr, tuple, schema).saturating_sub(1);
                        let len_i = string.len() as i32;

                        while from < 0 {
                            from += len_i + 1;
                        }
                        if from > len_i {
                            return Ok(Arc::new(DataValue::Utf8 {
                                value: None,
                                ty: Utf8Type::Variable(None),
                                unit: CharLengthUnits::Characters,
                            }));
                        }
                        string = string.split_off(from as usize);
                    }
                    if let Some(for_expr) = for_expr {
                        let for_i =
                            cmp::min(eval_to_num!(for_expr, tuple, schema) as usize, string.len());
                        let _ = string.split_off(for_i);
                    }

                    Ok(Arc::new(DataValue::Utf8 {
                        value: Some(string),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }))
                } else {
                    Ok(Arc::new(DataValue::Utf8 {
                        value: None,
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }))
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                let unpack = |expr: &ScalarExpression| -> Result<String, DatabaseError> {
                    Ok(DataValue::clone(expr.eval(tuple, schema)?.as_ref())
                        .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                        .utf8()
                        .unwrap_or("".to_owned()))
                };
                let pattern = unpack(expr)?;
                let str = unpack(in_expr)?;
                Ok(Arc::new(DataValue::Int32(Some(
                    str.find(&pattern).map(|pos| pos as i32 + 1).unwrap_or(0),
                ))))
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                let mut value = None;
                if let Some(string) = DataValue::clone(expr.eval(tuple, schema)?.as_ref())
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                {
                    let mut trim_what = String::from(" ");
                    if let Some(trim_what_expr) = trim_what_expr {
                        trim_what = DataValue::clone(trim_what_expr.eval(tuple, schema)?.as_ref())
                            .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                            .utf8()
                            .unwrap_or_default();
                    }
                    let trim_regex = match trim_where {
                        Some(TrimWhereField::Both) | None => Regex::new(&format!(
                            r"^(?:{0})*([\w\W]*?)(?:{0})*$",
                            regex::escape(&trim_what)
                        ))
                        .unwrap(),
                        Some(TrimWhereField::Leading) => {
                            Regex::new(&format!(r"^(?:{0})*([\w\W]*?)", regex::escape(&trim_what)))
                                .unwrap()
                        }
                        Some(TrimWhereField::Trailing) => {
                            Regex::new(&format!(r"([\w\W]*?)(?:{0})*$", regex::escape(&trim_what)))
                                .unwrap()
                        }
                    };
                    let string_trimmed = trim_regex.replace_all(&string, "$1").to_string();

                    value = Some(string_trimmed)
                }
                Ok(Arc::new(DataValue::Utf8 {
                    value,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }))
            }
            ScalarExpression::Reference { pos, .. } => Ok(tuple
                .values
                .get(*pos)
                .unwrap_or_else(|| &NULL_VALUE)
                .clone()),
            ScalarExpression::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    values.push(expr.eval(tuple, schema)?);
                }
                Ok(Arc::new(DataValue::Tuple(
                    (!values.is_empty()).then_some(values),
                )))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, args, .. }) => Ok(Arc::new(
                inner.eval(args, tuple, schema)?.cast(inner.return_type())?,
            )),
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ty,
            } => {
                if condition.eval(tuple, schema)?.is_true()? {
                    check_cast(left_expr.eval(tuple, schema)?, ty)
                } else {
                    check_cast(right_expr.eval(tuple, schema)?, ty)
                }
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = left_expr.eval(tuple, schema)?;

                if value.is_null() {
                    value = right_expr.eval(tuple, schema)?;
                }
                check_cast(value, ty)
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = left_expr.eval(tuple, schema)?;

                if right_expr.eval(tuple, schema)? == value {
                    value = NULL_VALUE.clone();
                }
                check_cast(value, ty)
            }
            ScalarExpression::Coalesce { exprs, ty } => {
                let mut value = None;

                for expr in exprs {
                    let temp = expr.eval(tuple, schema)?;

                    if !temp.is_null() {
                        value = Some(temp);
                        break;
                    }
                }
                check_cast(value.unwrap_or_else(|| NULL_VALUE.clone()), ty)
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ty,
            } => {
                let mut operand_value = None;
                let mut result = None;

                if let Some(expr) = operand_expr {
                    operand_value = Some(expr.eval(tuple, schema)?);
                }
                for (when_expr, result_expr) in expr_pairs {
                    let mut when_value = when_expr.eval(tuple, schema)?;
                    let is_true = if let Some(operand_value) = &operand_value {
                        let ty = operand_value.logical_type();
                        let evaluator = EvaluatorFactory::binary_create(ty, BinaryOperator::Eq)?;

                        if when_value.logical_type() != ty {
                            when_value = Arc::new(DataValue::clone(&when_value).cast(&ty)?);
                        }
                        evaluator
                            .0
                            .binary_eval(operand_value, &when_value)
                            .is_true()?
                    } else {
                        when_value.is_true()?
                    };
                    if is_true {
                        result = Some(result_expr.eval(tuple, schema)?);
                        break;
                    }
                }
                if result.is_none() {
                    if let Some(expr) = else_expr {
                        result = Some(expr.eval(tuple, schema)?);
                    }
                }
                check_cast(result.unwrap_or_else(|| NULL_VALUE.clone()), ty)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
        }
    }
}
