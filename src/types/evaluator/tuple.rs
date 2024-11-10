use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleNotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleLtEqBinaryEvaluator;

fn tuple_cmp(v1: &[DataValue], v2: &[DataValue]) -> Option<Ordering> {
    let mut order = Ordering::Equal;
    let mut v1_iter = v1.iter();
    let mut v2_iter = v2.iter();

    while order == Ordering::Equal {
        order = match (v1_iter.next(), v2_iter.next()) {
            (Some(v1), Some(v2)) => v1.partial_cmp(v2)?,
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => break,
        }
    }
    Some(order)
}

#[typetag::serde]
impl BinaryEvaluator for TupleEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
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
impl BinaryEvaluator for TupleNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(v1 != v2),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => tuple_cmp(v1, v2).map(|order| order.is_gt()),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => tuple_cmp(v1, v2).map(|order| order.is_ge()),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => tuple_cmp(v1, v2).map(|order| order.is_lt()),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Tuple(value) => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => tuple_cmp(v1, v2).map(|order| order.is_le()),
            (_, _) => None,
        };
        DataValue::Boolean(value)
    }
}
