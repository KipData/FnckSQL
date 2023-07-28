use std::fmt::Display;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator};

use self::agg::AggKind;
use crate::catalog::ColumnCatalog;
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub mod agg;
mod evaluator;
mod array_compute;

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Clone)]
pub enum ScalarExpression {
    Constant(DataValue),
    ColumnRef(ColumnCatalog),
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
        is_try: bool,
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
        kind: AggKind,
        args: Vec<ScalarExpression>,
        ty: LogicalType,
    },
}

impl ScalarExpression {
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
            Self::Constant(v) => v.logic_type().clone(),
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

    pub fn has_agg_call(&self) -> bool {
        todo!()
    }
}

impl Display for ScalarExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    True,
    False,
}

impl From<SqlUnaryOperator> for UnaryOperator {
    fn from(value: SqlUnaryOperator) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
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
            SqlBinaryOperator::BitwiseOr => BinaryOperator::BitwiseOr,
            SqlBinaryOperator::BitwiseAnd => BinaryOperator::BitwiseAnd,
            SqlBinaryOperator::BitwiseXor => BinaryOperator::BitwiseXor,
            _ => todo!()
        }
    }
}
