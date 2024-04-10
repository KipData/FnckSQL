use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalPlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalDivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalNotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for DecimalPlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 + v2)
        } else {
            None
        };
        DataValue::Decimal(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalMinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 - v2)
        } else {
            None
        };
        DataValue::Decimal(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalMultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 * v2)
        } else {
            None
        };
        DataValue::Decimal(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalDivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 / v2)
        } else {
            None
        };
        DataValue::Decimal(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 > v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 >= v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 < v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 <= v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 == v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 != v2)
        } else {
            None
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Decimal(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = if let (Some(v1), Some(v2)) = (left, right) {
            Some(v1 % v2)
        } else {
            None
        };
        DataValue::Decimal(value)
    }
}
