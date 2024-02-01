use crate::errors::DatabaseError;
use crate::execution::volcano::dql::aggregate::Accumulator;
use crate::types::value::{DataValue, ValueRef};
use ahash::RandomState;
use std::collections::HashSet;
use std::sync::Arc;

pub struct CountAccumulator {
    result: i32,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { result: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), DatabaseError> {
        if !value.is_null() {
            self.result += 1;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, DatabaseError> {
        Ok(Arc::new(DataValue::Int32(Some(self.result))))
    }
}

pub struct DistinctCountAccumulator {
    distinct_values: HashSet<ValueRef, RandomState>,
}

impl DistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            distinct_values: HashSet::default(),
        }
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn update_value(&mut self, value: &ValueRef) -> Result<(), DatabaseError> {
        if !value.is_null() {
            self.distinct_values.insert(value.clone());
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ValueRef, DatabaseError> {
        Ok(Arc::new(DataValue::Int32(Some(
            self.distinct_values.len() as i32
        ))))
    }
}
