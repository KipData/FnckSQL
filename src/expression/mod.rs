use std::fmt::Display;

use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator};

use self::agg::AggKind;
use crate::catalog::{ColumnDesc, ColumnRefId};
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub mod agg;
mod evaluator;

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Clone)]
pub enum ScalarExpression {
    Constant(DataValue),
    ColumnRef {
        column_ref_id: ColumnRefId,
        primary_key: bool,
        desc: ColumnDesc,
    },
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
    pub fn return_type(&self) -> Option<LogicalType> {
        match self {
            Self::Constant(v) => Some(v.get_logic_type().clone()),
            Self::ColumnRef { desc, .. } => Some(desc.get_datatype().clone()),
            Self::Binary {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::Unary {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::TypeCast {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::AggCall {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::InputRef {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::IsNull { .. } => Some(LogicalType::Boolean),
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
        todo!()
    }
}
