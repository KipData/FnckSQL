use crate::errors::DatabaseError;
use crate::execution::volcano::dql::aggregate::sum::SumAccumulator;
use crate::execution::volcano::dql::aggregate::Accumulator;
use crate::expression::BinaryOperator;
use crate::types::evaluator::EvaluatorFactory;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use std::sync::Arc;

pub struct AvgAccumulator {
    inner: SumAccumulator,
    count: usize,
}

impl AvgAccumulator {
    pub fn new(ty: &LogicalType) -> Result<Self, DatabaseError> {
        Ok(Self {
            inner: SumAccumulator::new(ty)?,
            count: 0,
        })
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
        let mut value = self.inner.evaluate()?;
        let value_ty = value.logical_type();

        if self.count == 0 {
            return Ok(Arc::new(DataValue::init(&value_ty)));
        }
        let quantity = if value_ty.is_signed_numeric() {
            DataValue::Int64(Some(self.count as i64))
        } else {
            DataValue::UInt32(Some(self.count as u32))
        };
        let quantity_ty = quantity.logical_type();

        if value_ty != quantity_ty {
            value = Arc::new(DataValue::clone(&value).cast(&quantity_ty)?)
        }
        let evaluator = EvaluatorFactory::binary_create(quantity_ty, BinaryOperator::Divide)?;
        Ok(Arc::new(evaluator.0.binary_eval(&value, &quantity)))
    }
}
