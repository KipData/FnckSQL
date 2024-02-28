use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression;
use crate::expression::agg::AggKind;
use itertools::Itertools;
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident, Query,
    UnaryOperator,
};
use std::slice;
use std::sync::Arc;

use super::{lower_ident, Binder, QueryBindStep, SubQueryType};
use crate::expression::function::{FunctionSummary, ScalarFunction};
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

macro_rules! try_alias {
    ($context:expr, $column_name:expr) => {
        if let Some(expr) = $context.expr_aliases.get(&$column_name) {
            return Ok(ScalarExpression::Alias {
                expr: Box::new(expr.clone()),
                alias: AliasType::Name($column_name),
            });
        }
    };
}

macro_rules! try_default {
    ($table_name:expr, $column_name:expr) => {
        if let (None, "default") = ($table_name, $column_name.as_str()) {
            return Ok(ScalarExpression::Empty);
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
                escape_char,
            } => self.bind_like(*negated, expr, pattern, escape_char),
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
            Expr::Subquery(subquery) => {
                let (sub_query, column) = self.bind_subquery(subquery)?;
                self.context.sub_query(SubQueryType::SubQuery(sub_query));

                if self.context.is_step(&QueryBindStep::Where) {
                    Ok(self.bind_temp_column(column))
                } else {
                    Ok(ScalarExpression::ColumnRef(column))
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let (sub_query, column) = self.bind_subquery(subquery)?;
                self.context
                    .sub_query(SubQueryType::InSubQuery(*negated, sub_query));

                if !self.context.is_step(&QueryBindStep::Where) {
                    return Err(DatabaseError::UnsupportedStmt(
                        "`in subquery` can only appear in `Where`".to_string(),
                    ));
                }

                let alias_expr = self.bind_temp_column(column);

                Ok(ScalarExpression::Binary {
                    op: expression::BinaryOperator::Eq,
                    left_expr: Box::new(self.bind_expr(expr)?),
                    right_expr: Box::new(alias_expr),
                    ty: LogicalType::Boolean,
                })
            }
            Expr::Tuple(exprs) => {
                let mut bond_exprs = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    bond_exprs.push(self.bind_expr(expr)?);
                }
                Ok(ScalarExpression::Tuple(bond_exprs))
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let fn_check_ty = |ty: &mut LogicalType, result_ty| {
                    if result_ty != LogicalType::SqlNull {
                        if ty == &LogicalType::SqlNull {
                            *ty = result_ty;
                        } else if ty != &result_ty {
                            return Err(DatabaseError::Incomparable(*ty, result_ty));
                        }
                    }

                    Ok(())
                };
                let mut operand_expr = None;
                let mut ty = LogicalType::SqlNull;
                if let Some(expr) = operand {
                    operand_expr = Some(Box::new(self.bind_expr(expr)?));
                }
                let mut expr_pairs = Vec::with_capacity(conditions.len());
                for i in 0..conditions.len() {
                    let result = self.bind_expr(&results[i])?;
                    let result_ty = result.return_type();

                    fn_check_ty(&mut ty, result_ty)?;
                    expr_pairs.push((self.bind_expr(&conditions[i])?, result))
                }

                let mut else_expr = None;
                if let Some(expr) = else_result {
                    let temp_expr = Box::new(self.bind_expr(expr)?);
                    let else_ty = temp_expr.return_type();

                    fn_check_ty(&mut ty, else_ty)?;
                    else_expr = Some(temp_expr);
                }

                Ok(ScalarExpression::CaseWhen {
                    operand_expr,
                    expr_pairs,
                    else_expr,
                    ty,
                })
            }
            expr => {
                todo!("{}", expr)
            }
        }
    }

    fn bind_temp_column(&mut self, column: ColumnRef) -> ScalarExpression {
        let mut alias_column = ColumnCatalog::clone(&column);
        alias_column.set_table_name(self.context.temp_table());

        ScalarExpression::Alias {
            expr: Box::new(ScalarExpression::ColumnRef(column)),
            alias: AliasType::Expr(Box::new(ScalarExpression::ColumnRef(Arc::new(
                alias_column,
            )))),
        }
    }

    fn bind_subquery(
        &mut self,
        subquery: &Query,
    ) -> Result<(LogicalPlan, Arc<ColumnCatalog>), DatabaseError> {
        let mut sub_query = self.bind_query(subquery)?;
        let sub_query_schema = sub_query.output_schema();

        if sub_query_schema.len() != 1 {
            return Err(DatabaseError::MisMatch(
                "expects only one expression to be returned",
                "the expression returned by the subquery",
            ));
        }
        let column = sub_query_schema[0].clone();
        Ok((sub_query, column))
    }

    pub fn bind_like(
        &mut self,
        negated: bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(self.bind_expr(expr)?);
        let right_expr = Box::new(self.bind_expr(pattern)?);
        let op = if negated {
            expression::BinaryOperator::NotLike(*escape_char)
        } else {
            expression::BinaryOperator::Like(*escape_char)
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
        try_alias!(self.context, column_name);
        if self.context.allow_default {
            try_default!(&table_name, column_name);
        }
        if let Some(table) = table_name.or(bind_table_name) {
            let table_catalog = self
                .context
                .table(Arc::new(table.clone()))
                .ok_or_else(|| DatabaseError::TableNotFound)?;

            let column_catalog = table_catalog
                .get_column_by_name(&column_name)
                .ok_or_else(|| DatabaseError::NotFound("column", column_name))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        } else {
            // handle col syntax
            let mut got_column = None;
            for table_catalog in self.context.bind_table.values() {
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
        let function_name = func.name.to_string().to_lowercase();

        match function_name.as_str() {
            "count" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of count() parameters", "1"));
                }
                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Count,
                    args,
                    ty: LogicalType::Integer,
                });
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of sum() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Sum,
                    args,
                    ty,
                });
            }
            "min" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of min() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Min,
                    args,
                    ty,
                });
            }
            "max" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of max() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Max,
                    args,
                    ty,
                });
            }
            "avg" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of avg() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Avg,
                    args,
                    ty,
                });
            }
            "if" => {
                if args.len() != 3 {
                    return Err(DatabaseError::MisMatch("number of if() parameters", "3"));
                }
                let ty = Self::return_type(&args[1], &args[2])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());
                let condition = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::If {
                    condition,
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "nullif" => {
                if args.len() != 2 {
                    return Err(DatabaseError::MisMatch(
                        "number of nullif() parameters",
                        "3",
                    ));
                }
                let ty = Self::return_type(&args[0], &args[1])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::NullIf {
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "ifnull" => {
                if args.len() != 2 {
                    return Err(DatabaseError::MisMatch(
                        "number of ifnull() parameters",
                        "3",
                    ));
                }
                let ty = Self::return_type(&args[0], &args[1])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::IfNull {
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "coalesce" => {
                let mut ty = LogicalType::SqlNull;

                if !args.is_empty() {
                    ty = args[0].return_type();

                    for arg in args.iter() {
                        let temp_ty = arg.return_type();

                        if temp_ty == LogicalType::SqlNull {
                            continue;
                        }
                        if ty == LogicalType::SqlNull && temp_ty != LogicalType::SqlNull {
                            ty = temp_ty;
                        } else if ty != temp_ty {
                            return Err(DatabaseError::Incomparable(ty, temp_ty));
                        }
                    }
                }
                return Ok(ScalarExpression::Coalesce { exprs: args, ty });
            }
            _ => (),
        }
        let arg_types = args.iter().map(ScalarExpression::return_type).collect_vec();
        let summary = FunctionSummary {
            name: function_name,
            arg_types,
        };
        if let Some(function) = self.context.functions.get(&summary) {
            return Ok(ScalarExpression::Function(ScalarFunction {
                args,
                inner: function.clone(),
            }));
        }

        Err(DatabaseError::NotFound("function", summary.name))
    }

    fn return_type(
        expr_1: &ScalarExpression,
        expr_2: &ScalarExpression,
    ) -> Result<LogicalType, DatabaseError> {
        let temp_ty_1 = expr_1.return_type();
        let temp_ty_2 = expr_2.return_type();

        match (temp_ty_1, temp_ty_2) {
            (LogicalType::SqlNull, LogicalType::SqlNull) => Ok(LogicalType::SqlNull),
            (ty, LogicalType::SqlNull) | (LogicalType::SqlNull, ty) => Ok(ty),
            (ty_1, ty_2) => LogicalType::max_logical_type(&ty_1, &ty_2),
        }
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
