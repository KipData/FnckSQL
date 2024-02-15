use crate::errors::DatabaseError;
use crate::execution::volcano::dql::aggregate::sum::SumAccumulator;
use crate::execution::volcano::dql::aggregate::Accumulator;
use crate::expression::BinaryOperator;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use std::sync::Arc;

pub struct AvgAccumulator {
    inner: SumAccumulator,
    count: usize,
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
    fn update_value(&mut self, value: &ValueRef) -> Result<(), DatabaseError> {
        if !value.is_null() {
            self.inner.update_value(value)?;
            self.count += 1;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, DatabaseError> {
        let value = self.inner.evaluate()?;

        let quantity = if value.logical_type().is_signed_numeric() {
            DataValue::Int64(Some(self.count as i64))
        } else {
            DataValue::UInt32(Some(self.count as u32))
        };

        Ok(Arc::new(DataValue::binary_op(
            &value,
            &quantity,
            &BinaryOperator::Divide,
        )?))
    }
}
