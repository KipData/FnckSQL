use crate::errors::DatabaseError;
use crate::expression;
use crate::expression::agg::AggKind;
use itertools::Itertools;
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident, UnaryOperator,
};
use std::slice;
use std::sync::Arc;

use super::{lower_ident, Binder};
use crate::expression::ScalarExpression;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

macro_rules! try_alias {
    ($context:expr, $column_name:expr) => {
        if let Some(expr) = $context.expr_aliases.get(&$column_name) {
            return Ok(ScalarExpression::Alias {
                expr: Box::new(expr.clone()),
                alias: $column_name,
            });
        }
    };
}

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<ScalarExpression, DatabaseError> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident), None)
            }
            Expr::CompoundIdentifier(idents) => self.bind_column_ref_from_identifiers(idents, None),
            Expr::BinaryOp { left, right, op } => self.bind_binary_op_internal(left, right, op),
            Expr::Value(v) => Ok(ScalarExpression::Constant(Arc::new(v.into()))),
            Expr::Function(func) => self.bind_function(func),
            Expr::Nested(expr) => self.bind_expr(expr),
            Expr::UnaryOp { expr, op } => self.bind_unary_op_internal(expr, op),
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => self.bind_like(*negated, expr, pattern),
            Expr::IsNull(expr) => self.bind_is_null(expr, false),
            Expr::IsNotNull(expr) => self.bind_is_null(expr, true),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.bind_is_in(expr, list, *negated),
            Expr::Cast { expr, data_type } => self.bind_cast(expr, data_type),
            Expr::TypedString { data_type, value } => {
                let logical_type = LogicalType::try_from(data_type.clone())?;
                let value = DataValue::Utf8(Some(value.to_string())).cast(&logical_type)?;

                Ok(ScalarExpression::Constant(Arc::new(value)))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(ScalarExpression::Between {
                negated: *negated,
                expr: Box::new(self.bind_expr(expr)?),
                left_expr: Box::new(self.bind_expr(low)?),
                right_expr: Box::new(self.bind_expr(high)?),
            }),
            Expr::Substring {
                expr,
                substring_for,
                substring_from,
            } => {
                let mut for_expr = None;
                let mut from_expr = None;

                if let Some(expr) = substring_for {
                    for_expr = Some(Box::new(self.bind_expr(expr)?))
                }
                if let Some(expr) = substring_from {
                    from_expr = Some(Box::new(self.bind_expr(expr)?))
                }

                Ok(ScalarExpression::SubString {
                    expr: Box::new(self.bind_expr(expr)?),
                    for_expr,
                    from_expr,
                })
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn bind_like(
        &mut self,
        negated: bool,
        expr: &Expr,
        pattern: &Expr,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(self.bind_expr(expr)?);
        let right_expr = Box::new(self.bind_expr(pattern)?);
        let op = if negated {
            expression::BinaryOperator::NotLike
        } else {
            expression::BinaryOperator::Like
        };
        Ok(ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            ty: LogicalType::Boolean,
        })
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
        bind_table_name: Option<String>,
    ) -> Result<ScalarExpression, DatabaseError> {
        let (table_name, column_name) = match idents {
            [column] => (None, lower_ident(column)),
            [table, column] => (Some(lower_ident(table)), lower_ident(column)),
            _ => {
                return Err(DatabaseError::InvalidColumn(
                    idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .join(".")
                        .to_string(),
                ))
            }
        };
        if let Some(table) = table_name.or(bind_table_name) {
            try_alias!(self.context, column_name);
            let table_catalog = self
                .context
                .table(Arc::new(table.clone()))
                .ok_or_else(|| DatabaseError::TableNotFound)?;

            let column_catalog = table_catalog
                .get_column_by_name(&column_name)
                .ok_or_else(|| DatabaseError::NotFound("column", column_name))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        } else {
            try_alias!(self.context, column_name);
            // handle col syntax
            let mut got_column = None;
            for (table_catalog, _) in self.context.bind_table.values() {
                if let Some(column_catalog) = table_catalog.get_column_by_name(&column_name) {
                    got_column = Some(column_catalog);
                }
                if got_column.is_some() {
                    break;
                }
            }
            let column_catalog =
                got_column.ok_or_else(|| DatabaseError::NotFound("column", column_name))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        }
    }

    fn bind_binary_op_internal(
        &mut self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(self.bind_expr(left)?);
        let right_expr = Box::new(self.bind_expr(right)?);

        let ty = match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                LogicalType::max_logical_type(&left_expr.return_type(), &right_expr.return_type())?
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Xor => LogicalType::Boolean,
            BinaryOperator::StringConcat => LogicalType::Varchar(None),
            _ => todo!(),
        };

        Ok(ScalarExpression::Binary {
            op: (op.clone()).into(),
            left_expr,
            right_expr,
            ty,
        })
    }

    fn bind_unary_op_internal(
        &mut self,
        expr: &Expr,
        op: &UnaryOperator,
    ) -> Result<ScalarExpression, DatabaseError> {
        let expr = Box::new(self.bind_expr(expr)?);
        let ty = if let UnaryOperator::Not = op {
            LogicalType::Boolean
        } else {
            expr.return_type()
        };

        Ok(ScalarExpression::Unary {
            op: (*op).into(),
            expr,
            ty,
        })
    }

    fn bind_function(&mut self, func: &Function) -> Result<ScalarExpression, DatabaseError> {
        let mut args = Vec::with_capacity(func.args.len());

        for arg in func.args.iter() {
            let arg_expr = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg_expr {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr)?),
                FunctionArgExpr::Wildcard => args.push(Self::wildcard_expr()),
                _ => todo!(),
            }
        }

        Ok(match func.name.to_string().to_lowercase().as_str() {
            "count" => ScalarExpression::AggCall {
                distinct: func.distinct,
                kind: AggKind::Count,
                args,
                ty: LogicalType::Integer,
            },
            "sum" => {
                let ty = args[0].return_type();

                ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Sum,
                    args,
                    ty,
                }
            }
            "min" => {
                let ty = args[0].return_type();

                ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Min,
                    args,
                    ty,
                }
            }
            "max" => {
                let ty = args[0].return_type();

                ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Max,
                    args,
                    ty,
                }
            }
            "avg" => {
                let ty = args[0].return_type();

                ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Avg,
                    args,
                    ty,
                }
            }
            _ => todo!(),
        })
    }

    fn bind_is_null(
        &mut self,
        expr: &Expr,
        negated: bool,
    ) -> Result<ScalarExpression, DatabaseError> {
        Ok(ScalarExpression::IsNull {
            negated,
            expr: Box::new(self.bind_expr(expr)?),
        })
    }

    fn bind_is_in(
        &mut self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
    ) -> Result<ScalarExpression, DatabaseError> {
        let args = list.iter().map(|expr| self.bind_expr(expr)).try_collect()?;

        Ok(ScalarExpression::In {
            negated,
            expr: Box::new(self.bind_expr(expr)?),
            args,
        })
    }

    fn bind_cast(&mut self, expr: &Expr, ty: &DataType) -> Result<ScalarExpression, DatabaseError> {
        Ok(ScalarExpression::TypeCast {
            expr: Box::new(self.bind_expr(expr)?),
            ty: LogicalType::try_from(ty.clone())?,
        })
    }

    fn wildcard_expr() -> ScalarExpression {
        ScalarExpression::Constant(Arc::new(DataValue::Utf8(Some("*".to_string()))))
    }
}
