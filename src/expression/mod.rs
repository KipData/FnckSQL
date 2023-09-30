use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use itertools::Itertools;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator};
use crate::binder::BinderContext;

use self::agg::AggKind;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
use crate::storage::Storage;
use crate::types::value::ValueRef;
use crate::types::LogicalType;
use crate::types::tuple::Tuple;

pub mod agg;
mod evaluator;
pub mod value_compute;
pub mod simplify;

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ScalarExpression {
    Constant(ValueRef),
    ColumnRef(ColumnRef),
    InputRef {
        index: usize,
        ty: LogicalType,
    },
    Alias {
        expr: Box<ScalarExpression>,
        alias: String,
    },
    TypeCast {
        expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    IsNull {
        expr: Box<ScalarExpression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    Binary {
        op: BinaryOperator,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    AggCall {
        distinct: bool,
        kind: AggKind,
        args: Vec<ScalarExpression>,
        ty: LogicalType,
    },
}

impl ScalarExpression {
    pub fn unpack_alias(&self) -> &ScalarExpression {
        if let ScalarExpression::Alias { expr, .. } = self {
            expr.unpack_alias()
        } else {
            self
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(col) => col.nullable,
            ScalarExpression::InputRef { .. } => unreachable!(),
            ScalarExpression::Alias { expr, .. } => expr.nullable(),
            ScalarExpression::TypeCast { expr, .. } => expr.nullable(),
            ScalarExpression::IsNull { expr } => expr.nullable(),
            ScalarExpression::Unary { expr, .. } => expr.nullable(),
            ScalarExpression::Binary { left_expr, right_expr, .. } =>
                left_expr.nullable() && right_expr.nullable(),
            ScalarExpression::AggCall { args, .. } => args[0].nullable()
        }
    }

    pub fn return_type(&self) -> LogicalType {
        match self {
            Self::Constant(v) => v.logical_type(),
            Self::ColumnRef(col) => col.datatype().clone(),
            Self::Binary {
                ty: return_type, ..
            } => return_type.clone(),
            Self::Unary {
                ty: return_type, ..
            } => return_type.clone(),
            Self::TypeCast {
                ty: return_type, ..
            } => return_type.clone(),
            Self::AggCall {
                ty: return_type, ..
            } => return_type.clone(),
            Self::InputRef {
                ty: return_type, ..
            } => return_type.clone(),
            Self::IsNull { .. } => LogicalType::Boolean,
            Self::Alias { expr, .. } => expr.return_type(),
        }
    }

    pub fn referenced_columns(&self) -> Vec<ColumnRef> {
        fn columns_collect(expr: &ScalarExpression, vec: &mut Vec<ColumnRef>) {
            match expr {
                ScalarExpression::ColumnRef(col) => {
                    vec.push(col.clone());
                }
                ScalarExpression::Alias { expr, .. } => {
                    columns_collect(&expr, vec)
                }
                ScalarExpression::TypeCast { expr, .. } => {
                    columns_collect(&expr, vec)
                }
                ScalarExpression::IsNull { expr, .. } => {
                    columns_collect(&expr, vec)
                }
                ScalarExpression::Unary { expr, .. } => {
                    columns_collect(&expr, vec)
                }
                ScalarExpression::Binary { left_expr, right_expr, .. } => {
                    columns_collect(left_expr, vec);
                    columns_collect(right_expr, vec);
                }
                ScalarExpression::AggCall { args, .. } => {
                    for expr in args {
                        columns_collect(expr, vec)
                    }
                }
                _ => (),
            }
        }

        let mut exprs = Vec::new();

        columns_collect(self, &mut exprs);

        exprs
    }

    pub fn has_agg_call<S: Storage>(&self, context: &BinderContext<S>) -> bool {
        match self {
            ScalarExpression::InputRef { index, .. } => {
                context.agg_calls.get(*index).is_some()
            },
            ScalarExpression::AggCall { .. } => unreachable!(),
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(_) => false,
            ScalarExpression::Alias { expr, .. } => expr.has_agg_call(context),
            ScalarExpression::TypeCast { expr, .. } => expr.has_agg_call(context),
            ScalarExpression::IsNull { expr, .. } => expr.has_agg_call(context),
            ScalarExpression::Unary { expr, .. } => expr.has_agg_call(context),
            ScalarExpression::Binary { left_expr, right_expr, .. } => {
                left_expr.has_agg_call(context) || right_expr.has_agg_call(context)
            }
        }
    }

    pub fn output_columns(&self, tuple: &Tuple) -> ColumnRef {
        match self {
            ScalarExpression::ColumnRef(col) => {
                col.clone()
            }
            ScalarExpression::Constant(value) => {
                Arc::new(ColumnCatalog::new(
                    format!("{}", value),
                    true,
                    ColumnDesc::new(value.logical_type(), false, false)
                ))
            }
            ScalarExpression::Alias { expr, alias } => {
                Arc::new(ColumnCatalog::new(
                    alias.to_string(),
                    true,
                    ColumnDesc::new(expr.return_type(), false, false)
                ))
            }
            ScalarExpression::AggCall { kind, args, ty, distinct } => {
                let args_str = args.iter()
                    .map(|expr| expr.output_columns(tuple).name.clone())
                    .join(", ");
                let op = |allow_distinct, distinct| {
                    if allow_distinct && distinct {
                        "DISTINCT "
                    } else {
                        ""
                    }
                };
                let column_name = format!(
                    "{:?}({}{})",
                    kind,
                    op(kind.allow_distinct(), *distinct),
                    args_str
                );

                Arc::new(ColumnCatalog::new(
                    column_name,
                    true,
                    ColumnDesc::new(ty.clone(), false, false)
                ))
            }
            ScalarExpression::InputRef { index, .. } => {
                tuple.columns[*index].clone()
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty
            } => {
                let column_name = format!(
                    "({} {} {})",
                    left_expr.output_columns(tuple).name,
                    op,
                    right_expr.output_columns(tuple).name,
                );

                Arc::new(ColumnCatalog::new(
                    column_name,
                    true,
                    ColumnDesc::new(ty.clone(), false, false)
                ))
            }
            _ => unreachable!()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl From<SqlUnaryOperator> for UnaryOperator {
    fn from(value: SqlUnaryOperator) -> Self {
        match value {
            SqlUnaryOperator::Plus =>  UnaryOperator::Plus,
            SqlUnaryOperator::Minus => UnaryOperator::Minus,
            SqlUnaryOperator::Not => UnaryOperator::Not,
            _ => unimplemented!("not support!")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,

    Modulo,
    StringConcat,

    Gt,
    Lt,
    GtEq,
    LtEq,
    Spaceship,
    Eq,
    NotEq,

    And,
    Or,
    Xor,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "mod"),
            BinaryOperator::StringConcat => write!(f, "&"),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::GtEq => write!(f, ">="),
            BinaryOperator::LtEq => write!(f, "<="),
            BinaryOperator::Spaceship => write!(f, "<=>"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::NotEq => write!(f, "!="),
            BinaryOperator::And => write!(f, "&&"),
            BinaryOperator::Or => write!(f, "||"),
            BinaryOperator::Xor => write!(f, "^"),
        }
    }
}

impl From<SqlBinaryOperator> for BinaryOperator {
    fn from(value: SqlBinaryOperator) -> Self {
        match value {
            SqlBinaryOperator::Plus => BinaryOperator::Plus,
            SqlBinaryOperator::Minus => BinaryOperator::Minus,
            SqlBinaryOperator::Multiply => BinaryOperator::Multiply,
            SqlBinaryOperator::Divide => BinaryOperator::Divide,
            SqlBinaryOperator::Modulo => BinaryOperator::Modulo,
            SqlBinaryOperator::StringConcat => BinaryOperator::StringConcat,
            SqlBinaryOperator::Gt => BinaryOperator::Gt,
            SqlBinaryOperator::Lt => BinaryOperator::Lt,
            SqlBinaryOperator::GtEq => BinaryOperator::GtEq,
            SqlBinaryOperator::LtEq => BinaryOperator::LtEq,
            SqlBinaryOperator::Spaceship => BinaryOperator::Spaceship,
            SqlBinaryOperator::Eq => BinaryOperator::Eq,
            SqlBinaryOperator::NotEq => BinaryOperator::NotEq,
            SqlBinaryOperator::And => BinaryOperator::And,
            SqlBinaryOperator::Or => BinaryOperator::Or,
            SqlBinaryOperator::Xor => BinaryOperator::Xor,
            _ => unimplemented!("not support!")
        }
    }
}
