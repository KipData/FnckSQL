use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use crate::types::value::Utf8Type;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8StringConcatBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotLikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}

#[typetag::serde]
impl BinaryEvaluator for Utf8GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
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
impl BinaryEvaluator for Utf8StringConcatBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let left = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let right = match right {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let value = match (left, right) {
            (Some(v1), Some(v2)) => Some(v1.clone() + v2),
            _ => None,
        };
        DataValue::Utf8 {
            value,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let value = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let pattern = match right {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let is_match = if let (Some(value), Some(pattern)) = (value, pattern) {
            string_like(value, pattern, self.escape_char)
        } else {
            return DataValue::Boolean(None);
        };

        DataValue::Boolean(Some(is_match))
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8NotLikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        let value = match left {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let pattern = match right {
            DataValue::Utf8 { value, .. } => value,
            DataValue::Null => &None,
            _ => unsafe { hint::unreachable_unchecked() },
        };
        let is_match = if let (Some(value), Some(pattern)) = (value, pattern) {
            string_like(value, pattern, self.escape_char)
        } else {
            return DataValue::Boolean(None);
        };

        DataValue::Boolean(Some(!is_match))
    }
}

fn string_like(value: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let mut regex_pattern = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(escape_char.map(|escape_c| escape_c == c), Some(true)) {
            if let Some(next_char) = chars.next() {
                regex_pattern.push(next_char);
            }
        } else if c == '%' {
            regex_pattern.push_str(".*");
        } else if c == '_' {
            regex_pattern.push('.');
        } else {
            regex_pattern.push(c);
        }
    }
    Regex::new(&regex_pattern).unwrap().is_match(value)
}
