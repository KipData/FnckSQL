use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator};

use self::agg::AggKind;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
use crate::types::value::ValueRef;
use crate::types::LogicalType;

pub mod agg;
mod evaluator;
pub mod simplify;
pub mod value_compute;

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum ScalarExpression {
    Constant(ValueRef),
    ColumnRef(ColumnRef),
    Alias {
        expr: Box<ScalarExpression>,
        alias: String,
    },
    TypeCast {
        expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    IsNull {
        negated: bool,
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
    In {
        negated: bool,
        expr: Box<ScalarExpression>,
        args: Vec<ScalarExpression>,
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

    pub fn has_count_star(&self) -> bool {
        match self {
            ScalarExpression::Alias { expr, .. } => expr.has_count_star(),
            ScalarExpression::TypeCast { expr, .. } => expr.has_count_star(),
            ScalarExpression::IsNull { expr, .. } => expr.has_count_star(),
            ScalarExpression::Unary { expr, .. } => expr.has_count_star(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_count_star() || right_expr.has_count_star(),
            ScalarExpression::AggCall { args, .. } => args.iter().any(Self::has_count_star),
            _ => false,
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(col) => col.nullable,
            ScalarExpression::Alias { expr, .. } => expr.nullable(),
            ScalarExpression::TypeCast { expr, .. } => expr.nullable(),
            ScalarExpression::IsNull { expr, .. } => expr.nullable(),
            ScalarExpression::Unary { expr, .. } => expr.nullable(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.nullable() && right_expr.nullable(),
            ScalarExpression::In { expr, args, .. } => {
                args.iter().all(ScalarExpression::nullable) && expr.nullable()
            }
            ScalarExpression::AggCall { args, .. } => args.iter().all(ScalarExpression::nullable),
        }
    }

    pub fn return_type(&self) -> LogicalType {
        match self {
            Self::Constant(v) => v.logical_type(),
            Self::ColumnRef(col) => *col.datatype(),
            Self::Binary {
                ty: return_type, ..
            } => *return_type,
            Self::Unary {
                ty: return_type, ..
            } => *return_type,
            Self::TypeCast {
                ty: return_type, ..
            } => *return_type,
            Self::AggCall {
                ty: return_type, ..
            } => *return_type,
            Self::IsNull { .. } | Self::In { .. } => LogicalType::Boolean,
            Self::Alias { expr, .. } => expr.return_type(),
        }
    }

    pub fn referenced_columns(&self, only_column_ref: bool) -> Vec<ColumnRef> {
        fn columns_collect(
            expr: &ScalarExpression,
            vec: &mut Vec<ColumnRef>,
            only_column_ref: bool,
        ) {
            // When `ScalarExpression` is a complex type, it itself is also a special Column
            if !only_column_ref {
                vec.push(expr.output_column());
            }
            match expr {
                ScalarExpression::ColumnRef(col) => {
                    vec.push(col.clone());
                }
                ScalarExpression::Alias { expr, .. } => columns_collect(expr, vec, only_column_ref),
                ScalarExpression::TypeCast { expr, .. } => {
                    columns_collect(expr, vec, only_column_ref)
                }
                ScalarExpression::IsNull { expr, .. } => {
                    columns_collect(expr, vec, only_column_ref)
                }
                ScalarExpression::Unary { expr, .. } => columns_collect(expr, vec, only_column_ref),
                ScalarExpression::Binary {
                    left_expr,
                    right_expr,
                    ..
                } => {
                    columns_collect(left_expr, vec, only_column_ref);
                    columns_collect(right_expr, vec, only_column_ref);
                }
                ScalarExpression::AggCall { args, .. } => {
                    for expr in args {
                        columns_collect(expr, vec, only_column_ref)
                    }
                }
                ScalarExpression::In { expr, args, .. } => {
                    columns_collect(expr, vec, only_column_ref);
                    for arg in args {
                        columns_collect(arg, vec, only_column_ref)
                    }
                }
                _ => (),
            }
        }
        let mut exprs = Vec::new();

        columns_collect(self, &mut exprs, only_column_ref);

        exprs
    }

    pub fn has_agg_call(&self) -> bool {
        match self {
            ScalarExpression::AggCall { .. } => true,
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(_) => false,
            ScalarExpression::Alias { expr, .. } => expr.has_agg_call(),
            ScalarExpression::TypeCast { expr, .. } => expr.has_agg_call(),
            ScalarExpression::IsNull { expr, .. } => expr.has_agg_call(),
            ScalarExpression::Unary { expr, .. } => expr.has_agg_call(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_agg_call() || right_expr.has_agg_call(),
            ScalarExpression::In { expr, args, .. } => {
                expr.has_agg_call() || args.iter().any(|arg| arg.has_agg_call())
            }
        }
    }

    pub fn output_column(&self) -> ColumnRef {
        match self {
            ScalarExpression::ColumnRef(col) => col.clone(),
            ScalarExpression::Constant(value) => Arc::new(ColumnCatalog::new(
                format!("{}", value),
                true,
                ColumnDesc::new(value.logical_type(), false, false, None),
                Some(self.clone()),
            )),
            ScalarExpression::Alias { expr, alias } => Arc::new(ColumnCatalog::new(
                alias.to_string(),
                true,
                ColumnDesc::new(expr.return_type(), false, false, None),
                Some(self.clone()),
            )),
            ScalarExpression::AggCall {
                kind,
                args,
                ty,
                distinct,
            } => {
                let args_str = args
                    .iter()
                    .map(|expr| expr.output_column().name().to_string())
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
                    ColumnDesc::new(*ty, false, false, None),
                    Some(self.clone()),
                ))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
            } => {
                let column_name = format!(
                    "({} {} {})",
                    left_expr.output_column().name(),
                    op,
                    right_expr.output_column().name(),
                );

                Arc::new(ColumnCatalog::new(
                    column_name,
                    true,
                    ColumnDesc::new(*ty, false, false, None),
                    Some(self.clone()),
                ))
            }
            ScalarExpression::Unary { expr, op, ty } => {
                let column_name = format!("{}{}", op, expr.output_column().name());
                Arc::new(ColumnCatalog::new(
                    column_name,
                    true,
                    ColumnDesc::new(*ty, false, false, None),
                    Some(self.clone()),
                ))
            }
            ScalarExpression::IsNull { negated, expr } => {
                let suffix = if *negated { "is not null" } else { "is null" };
                Arc::new(ColumnCatalog::new(
                    format!("{} {}", expr.output_column().name(), suffix),
                    true,
                    ColumnDesc::new(LogicalType::Boolean, false, false, None),
                    Some(self.clone()),
                ))
            }
            ScalarExpression::In {
                negated,
                expr,
                args,
            } => {
                let args_string = args
                    .iter()
                    .map(|arg| arg.output_column().name().to_string())
                    .join(", ");
                let op_string = if *negated { "not in" } else { "in" };
                Arc::new(ColumnCatalog::new(
                    format!(
                        "{} {} ({})",
                        expr.output_column().name(),
                        op_string,
                        args_string
                    ),
                    true,
                    ColumnDesc::new(LogicalType::Boolean, false, false, None),
                    Some(self.clone()),
                ))
            }
            ScalarExpression::TypeCast { expr, ty } => Arc::new(ColumnCatalog::new(
                format!("CAST({} as {})", expr.output_column().name(), ty),
                true,
                ColumnDesc::new(*ty, false, false, None),
                Some(self.clone()),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl From<SqlUnaryOperator> for UnaryOperator {
    fn from(value: SqlUnaryOperator) -> Self {
        match value {
            SqlUnaryOperator::Plus => UnaryOperator::Plus,
            SqlUnaryOperator::Minus => UnaryOperator::Minus,
            SqlUnaryOperator::Not => UnaryOperator::Not,
            _ => unimplemented!("not support!"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Like,
    NotLike,

    And,
    Or,
    Xor,
}

impl fmt::Display for ScalarExpression {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.output_column().name())
    }
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
            BinaryOperator::Like => write!(f, "like"),
            BinaryOperator::NotLike => write!(f, "not like"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            UnaryOperator::Plus => write!(f, "+"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Not => write!(f, "!"),
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
            _ => unimplemented!("not support!"),
        }
    }
}
