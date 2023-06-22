use std::fmt::Display;

use crate::{
    catalog::{ColumnDesc, ColumnRefId},
    types::{value::DataValue, DataType},
};
use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator};

use self::agg::AggKind;
pub use sqlparser::ast::DataType as DataTypeKind;

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
        ty: DataType,
    },
    Alias {
        expr: Box<ScalarExpression>,
        alias: String,
    },
    TypeCast {
        expr: Box<ScalarExpression>,
        ty: DataType,
        is_try: bool,
    },
    IsNull {
        expr: Box<ScalarExpression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ScalarExpression>,
        ty: Option<DataType>,
    },
    Binary {
        op: BinaryOperator,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: Option<DataType>,
    },
    AggCall {
        kind: AggKind,
        args: Vec<ScalarExpression>,
        ty: DataType,
    },
}

impl ScalarExpression {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            Self::Constant(v) => v.data_type(),
            Self::ColumnRef { desc, .. } => Some(desc.get_datatype().clone()),
            Self::Binary {
                ty: return_type, ..
            } => return_type.clone(),
            Self::Unary {
                ty: return_type, ..
            } => return_type.clone(),
            Self::TypeCast {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::AggCall {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::InputRef {
                ty: return_type, ..
            } => Some(return_type.clone()),
            Self::IsNull { .. } => Some(DataType::new(DataTypeKind::Boolean, false)),
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
