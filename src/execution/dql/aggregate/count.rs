use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::Accumulator;
use crate::types::value::DataValue;
use ahash::RandomState;
use std::collections::HashSet;

pub struct CountAccumulator {
    result: i32,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { result: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            self.result += 1;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Int32(Some(self.result)))
    }
}

pub struct DistinctCountAccumulator {
    distinct_values: HashSet<DataValue, RandomState>,
}

impl DistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            distinct_values: HashSet::default(),
        }
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            self.distinct_values.insert(value.clone());
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Int32(Some(self.distinct_values.len() as i32)))
    }
}
