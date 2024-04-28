use crate::errors::DatabaseError;
use crate::execution::volcano::dql::aggregate::Accumulator;
use crate::expression::BinaryOperator;
use crate::types::evaluator::{BinaryEvaluatorBox, EvaluatorFactory};
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use ahash::RandomState;
use std::collections::HashSet;
use std::sync::Arc;

pub struct SumAccumulator {
    result: DataValue,
    evaluator: BinaryEvaluatorBox,
}

impl SumAccumulator {
    pub fn new(ty: &LogicalType) -> Result<Self, DatabaseError> {
        assert!(ty.is_numeric());

        Ok(Self {
            result: DataValue::none(ty),
            evaluator: EvaluatorFactory::binary_create(*ty, BinaryOperator::Plus)?,
        })
    }
}

impl Accumulator for SumAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), DatabaseError> {
        if !value.is_null() {
            if self.result.is_null() {
                self.result = DataValue::clone(value);
            } else {
                self.result = self.evaluator.0.binary_eval(&self.result, value);
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, DatabaseError> {
        Ok(Arc::new(self.result.clone()))
    }
}

pub struct DistinctSumAccumulator {
    distinct_values: HashSet<ValueRef, RandomState>,
    inner: SumAccumulator,
}

impl DistinctSumAccumulator {
    pub fn new(ty: &LogicalType) -> Result<Self, DatabaseError> {
        Ok(Self {
            distinct_values: HashSet::default(),
            inner: SumAccumulator::new(ty)?,
        })
    }
}

impl Accumulator for DistinctSumAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), DatabaseError> {
        if !self.distinct_values.contains(value) {
            self.distinct_values.insert(value.clone());
            self.inner.update_value(value)?;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, DatabaseError> {
        self.inner.evaluate()
    }
}
