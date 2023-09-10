use std::sync::Arc;
use crate::execution::executor::dql::aggregate::Accumulator;
use crate::execution::executor::dql::aggregate::sum::SumAccumulator;
use crate::execution::ExecutorError;
use crate::expression::BinaryOperator;
use crate::expression::value_compute::binary_op;
use crate::types::LogicalType;
use crate::types::value::{DataValue, ValueRef};

pub struct AvgAccumulator {
    inner: SumAccumulator,
    count: usize
}

impl AvgAccumulator {
    pub fn new(ty: &LogicalType) -> Self {
        Self {
            inner: SumAccumulator::new(ty),
            count: 0,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), ExecutorError> {
        if !value.is_null() {
            self.inner.update_value(value)?;
            self.count += 1;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, ExecutorError> {
        let value = self.inner
            .evaluate()?;

        let quantity = if value.logical_type().is_signed_numeric() {
            DataValue::Int64(Some(self.count as i64))
        } else {
            DataValue::UInt32(Some(self.count as u32))
        };

        Ok(Arc::new(
            binary_op(
                &value,
                &quantity,
                &BinaryOperator::Divide
            )
        ))
    }
}