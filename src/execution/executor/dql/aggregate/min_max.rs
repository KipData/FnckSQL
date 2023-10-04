use crate::execution::executor::dql::aggregate::Accumulator;
use crate::execution::ExecutorError;
use crate::expression::value_compute::binary_op;
use crate::expression::BinaryOperator;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use std::sync::Arc;

pub struct MinMaxAccumulator {
    inner: Option<ValueRef>,
    op: BinaryOperator,
    ty: LogicalType,
}

impl MinMaxAccumulator {
    pub fn new(ty: &LogicalType, is_max: bool) -> Self {
        let op = if is_max {
            BinaryOperator::Lt
        } else {
            BinaryOperator::Gt
        };

        Self {
            inner: None,
            op,
            ty: *ty,
        }
    }
}

impl Accumulator for MinMaxAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), ExecutorError> {
        if !value.is_null() {
            if let Some(inner_value) = &self.inner {
                if let DataValue::Boolean(Some(result)) = binary_op(&inner_value, value, &self.op)?
                {
                    result
                } else {
                    unreachable!()
                }
            } else {
                true
            }
            .then(|| self.inner = Some(value.clone()));
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, ExecutorError> {
        Ok(self
            .inner
            .clone()
            .unwrap_or_else(|| Arc::new(DataValue::none(&self.ty))))
    }
}
