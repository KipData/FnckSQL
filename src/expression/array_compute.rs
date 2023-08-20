use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::compute::{eq_dyn, gt_dyn, gt_eq_dyn, lt_dyn, lt_eq_dyn, neq_dyn};
use arrow::datatypes::DataType;
use arrow::array::*;
use arrow::compute::*;
use crate::execution::ExecutorError;
use crate::expression::BinaryOperator;
/// Copied from datafusion binary.rs
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! arithmetic_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => todo!("unsupported data type"),
        }
    }};
}

macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        if *$LEFT.data_type() != DataType::Boolean || *$RIGHT.data_type() != DataType::Boolean {
            return Err(ExecutorError::InternalError(format!(
                "Cannot evaluate binary expression with types {:?} and {:?}, only Boolean supported",
                $LEFT.data_type(),
                $RIGHT.data_type()
            )));
        }

        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

pub fn binary_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    match op {
        BinaryOperator::Plus => arithmetic_op!(left, right, add),
        BinaryOperator::Minus => arithmetic_op!(left, right, subtract),
        BinaryOperator::Multiply => arithmetic_op!(left, right, multiply),
        BinaryOperator::Divide => arithmetic_op!(left, right, divide),
        BinaryOperator::Gt => Ok(Arc::new(gt_dyn(left, right)?)),
        BinaryOperator::Lt => Ok(Arc::new(lt_dyn(left, right)?)),
        BinaryOperator::GtEq => Ok(Arc::new(gt_eq_dyn(left, right)?)),
        BinaryOperator::LtEq => Ok(Arc::new(lt_eq_dyn(left, right)?)),
        BinaryOperator::Eq => Ok(Arc::new(eq_dyn(left, right)?)),
        BinaryOperator::NotEq => Ok(Arc::new(neq_dyn(left, right)?)),
        BinaryOperator::And => boolean_op!(left, right, and_kleene),
        BinaryOperator::Or => boolean_op!(left, right, or_kleene),
        _ => todo!(),
    }
}
