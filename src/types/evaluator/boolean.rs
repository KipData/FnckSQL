use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanAndBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanOrBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanNotEqBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for BooleanAndBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(*v1 && *v2),
            (Some(false), _) | (_, Some(false)) => Some(false),
            _ => None,
        };
        DataValue::Boolean(value)
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanOrBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(*v1 || *v2),
            (Some(true), _) | (_, Some(true)) => Some(true),
            _ => None,
        };
        DataValue::Boolean(value)
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(v1 == v2),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Boolean(value) => value,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(v1 != v2),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
