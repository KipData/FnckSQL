use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, UnaryOperator};
use crate::types::value::{DataValue, Utf8Type, ValueRef};
use crate::types::LogicalType;
use regex::Regex;
use std::cmp::Ordering;
use sqlparser::ast::CharLengthUnits;

fn unpack_bool(value: DataValue) -> Option<bool> {
    match value {
        DataValue::Boolean(inner) => inner,
        _ => None,
    }
}

fn unpack_utf8(value: DataValue) -> Option<String> {
    match value {
        DataValue::Utf8 { value: inner, .. } => inner,
        _ => None,
    }
}

fn unpack_tuple(value: DataValue) -> Option<Vec<ValueRef>> {
    match value {
        DataValue::Tuple(inner) => inner,
        _ => None,
    }
}

macro_rules! numeric_binary_compute {
    ($compute_type:path, $left:expr, $right:expr, $op:expr, $unified_type:expr) => {
        match $op {
            BinaryOperator::Plus => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 + v2)
                } else {
                    None
                };

                $compute_type(value)
            }
            BinaryOperator::Minus => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 - v2)
                } else {
                    None
                };

                $compute_type(value)
            }
            BinaryOperator::Multiply => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 * v2)
                } else {
                    None
                };

                $compute_type(value)
            }
            BinaryOperator::Divide => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 as f64 / v2 as f64)
                } else {
                    None
                };

                DataValue::Float64(value)
            }

            BinaryOperator::Gt => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 > v2)
                } else {
                    None
                };

                DataValue::Boolean(value)
            }
            BinaryOperator::Lt => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 < v2)
                } else {
                    None
                };

                DataValue::Boolean(value)
            }
            BinaryOperator::GtEq => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 >= v2)
                } else {
                    None
                };

                DataValue::Boolean(value)
            }
            BinaryOperator::LtEq => {
                let value = if let ($compute_type(Some(v1)), $compute_type(Some(v2))) =
                    ($left.cast($unified_type)?, $right.cast($unified_type)?)
                {
                    Some(v1 <= v2)
                } else {
                    None
                };

                DataValue::Boolean(value)
            }
            BinaryOperator::Eq => {
                let value = match ($left.cast($unified_type)?, $right.cast($unified_type)?) {
                    ($compute_type(Some(v1)), $compute_type(Some(v2))) => Some(v1 == v2),
                    (_, _) => None,
                };

                DataValue::Boolean(value)
            }
            BinaryOperator::NotEq => {
                let value = match ($left.cast($unified_type)?, $right.cast($unified_type)?) {
                    ($compute_type(Some(v1)), $compute_type(Some(v2))) => Some(v1 != v2),
                    (_, _) => None,
                };

                DataValue::Boolean(value)
            }
            _ => {
                return Err(DatabaseError::UnsupportedBinaryOperator(
                    *$unified_type,
                    *$op,
                ))
            }
        }
    };
}

impl DataValue {
    pub fn unary_op(&self, op: &UnaryOperator) -> Result<DataValue, DatabaseError> {
        let mut value_type = self.logical_type();
        let mut value = self.clone();

        if value_type.is_numeric() && matches!(op, UnaryOperator::Plus | UnaryOperator::Minus) {
            if value_type.is_unsigned_numeric() {
                match value_type {
                    LogicalType::UTinyint => value_type = LogicalType::Tinyint,
                    LogicalType::USmallint => value_type = LogicalType::Smallint,
                    LogicalType::UInteger => value_type = LogicalType::Integer,
                    LogicalType::UBigint => value_type = LogicalType::Bigint,
                    _ => unreachable!(),
                };
                value = value.cast(&value_type)?;
            }

            let result = match op {
                UnaryOperator::Plus => value,
                UnaryOperator::Minus => match value {
                    DataValue::Float32(option) => DataValue::Float32(option.map(|v| -v)),
                    DataValue::Float64(option) => DataValue::Float64(option.map(|v| -v)),
                    DataValue::Int8(option) => DataValue::Int8(option.map(|v| -v)),
                    DataValue::Int16(option) => DataValue::Int16(option.map(|v| -v)),
                    DataValue::Int32(option) => DataValue::Int32(option.map(|v| -v)),
                    DataValue::Int64(option) => DataValue::Int64(option.map(|v| -v)),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };

            Ok(result)
        } else if matches!((value_type, op), (LogicalType::Boolean, UnaryOperator::Not)) {
            match value {
                DataValue::Boolean(option) => Ok(DataValue::Boolean(option.map(|v| !v))),
                _ => unreachable!(),
            }
        } else {
            Err(DatabaseError::InvalidType)
        }
    }
    /// Tips:
    /// - Null values operate as null values
    pub fn binary_op(
        &self,
        right: &DataValue,
        op: &BinaryOperator,
    ) -> Result<DataValue, DatabaseError> {
        if let BinaryOperator::Like(escape_char) | BinaryOperator::NotLike(escape_char) = op {
            let value_option = unpack_utf8(self.clone().cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?);
            let pattern_option = unpack_utf8(right.clone().cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?);

            let mut is_match = if let (Some(value), Some(pattern)) = (value_option, pattern_option)
            {
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

                Regex::new(&regex_pattern).unwrap().is_match(&value)
            } else {
                return Ok(DataValue::Boolean(None));
            };
            if matches!(op, BinaryOperator::NotLike(_)) {
                is_match = !is_match;
            }
            return Ok(DataValue::Boolean(Some(is_match)));
        }
        let unified_type =
            LogicalType::max_logical_type(&self.logical_type(), &right.logical_type())?;

        let value = match &unified_type {
            LogicalType::Tinyint => {
                numeric_binary_compute!(
                    DataValue::Int8,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Smallint => {
                numeric_binary_compute!(
                    DataValue::Int16,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Integer => {
                numeric_binary_compute!(
                    DataValue::Int32,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Bigint => {
                numeric_binary_compute!(
                    DataValue::Int64,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::UTinyint => {
                numeric_binary_compute!(
                    DataValue::UInt8,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::USmallint => {
                numeric_binary_compute!(
                    DataValue::UInt16,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::UInteger => {
                numeric_binary_compute!(
                    DataValue::UInt32,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::UBigint => {
                numeric_binary_compute!(
                    DataValue::UInt64,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Float => {
                numeric_binary_compute!(
                    DataValue::Float32,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Double => {
                numeric_binary_compute!(
                    DataValue::Float64,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Date => {
                numeric_binary_compute!(
                    DataValue::Date32,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::DateTime => {
                numeric_binary_compute!(
                    DataValue::Date64,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Time => {
                numeric_binary_compute!(
                    DataValue::Time,
                    self.clone(),
                    right.clone(),
                    op,
                    &unified_type
                )
            }
            LogicalType::Decimal(_, _) => {
                let left_value = self.clone().cast(&unified_type)?;
                let right_value = right.clone().cast(&unified_type)?;

                match op {
                    BinaryOperator::Plus => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 + v2)
                            } else {
                                None
                            };

                        DataValue::Decimal(value)
                    }
                    BinaryOperator::Minus => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 - v2)
                            } else {
                                None
                            };

                        DataValue::Decimal(value)
                    }
                    BinaryOperator::Multiply => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 * v2)
                            } else {
                                None
                            };

                        DataValue::Decimal(value)
                    }
                    BinaryOperator::Divide => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 / v2)
                            } else {
                                None
                            };

                        DataValue::Decimal(value)
                    }

                    BinaryOperator::Gt => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 > v2)
                            } else {
                                None
                            };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Lt => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 < v2)
                            } else {
                                None
                            };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::GtEq => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 >= v2)
                            } else {
                                None
                            };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::LtEq => {
                        let value =
                            if let (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) =
                                (left_value, right_value)
                            {
                                Some(v1 <= v2)
                            } else {
                                None
                            };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Eq => {
                        let value = match (left_value, right_value) {
                            (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) => {
                                Some(v1 == v2)
                            }
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::NotEq => {
                        let value = match (left_value, right_value) {
                            (DataValue::Decimal(Some(v1)), DataValue::Decimal(Some(v2))) => {
                                Some(v1 != v2)
                            }
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    _ => return Err(DatabaseError::UnsupportedBinaryOperator(unified_type, *op)),
                }
            }
            LogicalType::Boolean => {
                let left_value = unpack_bool(self.clone().cast(&unified_type)?);
                let right_value = unpack_bool(right.clone().cast(&unified_type)?);

                match op {
                    BinaryOperator::And => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 && v2),
                            (Some(false), _) | (_, Some(false)) => Some(false),
                            _ => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Or => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 || v2),
                            (Some(true), _) | (_, Some(true)) => Some(true),
                            _ => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Eq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 == v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::NotEq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 != v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    _ => return Err(DatabaseError::UnsupportedBinaryOperator(unified_type, *op)),
                }
            }
            LogicalType::Varchar(_, _) | LogicalType::Char(_, _) => {
                let left_value = unpack_utf8(self.clone().cast(&unified_type)?);
                let right_value = unpack_utf8(right.clone().cast(&unified_type)?);

                match op {
                    BinaryOperator::Gt => {
                        let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                            Some(v1 > v2)
                        } else {
                            None
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Lt => {
                        let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                            Some(v1 < v2)
                        } else {
                            None
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::GtEq => {
                        let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                            Some(v1 >= v2)
                        } else {
                            None
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::LtEq => {
                        let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                            Some(v1 <= v2)
                        } else {
                            None
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Eq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 == v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::NotEq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 != v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::StringConcat => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 + &v2),
                            _ => None,
                        };

                        DataValue::Utf8 {
                            value,
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        }
                    }
                    _ => return Err(DatabaseError::UnsupportedBinaryOperator(unified_type, *op)),
                }
            }
            LogicalType::SqlNull => return Ok(DataValue::Null),
            LogicalType::Invalid => return Err(DatabaseError::InvalidType),
            LogicalType::Tuple => {
                let left_value = unpack_tuple(self.clone().cast(&unified_type)?);
                let right_value = unpack_tuple(right.clone().cast(&unified_type)?);

                match op {
                    BinaryOperator::Eq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 == v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::NotEq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Some(v1 != v2),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq => {
                        let value = match (left_value, right_value) {
                            (Some(v1), Some(v2)) => Self::tuple_cmp(v1, v2).map(|order| match op {
                                BinaryOperator::Gt => order.is_gt(),
                                BinaryOperator::Lt => order.is_lt(),
                                BinaryOperator::GtEq => order.is_ge(),
                                BinaryOperator::LtEq => order.is_le(),
                                _ => unreachable!(),
                            }),
                            (_, _) => None,
                        };

                        DataValue::Boolean(value)
                    }
                    _ => return Err(DatabaseError::UnsupportedBinaryOperator(unified_type, *op)),
                }
            }
        };

        Ok(value)
    }

    fn tuple_cmp(v1: Vec<ValueRef>, v2: Vec<ValueRef>) -> Option<Ordering> {
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
}

#[cfg(test)]
mod test {
    use sqlparser::ast::CharLengthUnits;
    use crate::errors::DatabaseError;
    use crate::expression::BinaryOperator;
    use crate::types::value::{DataValue, Utf8Type};

    #[test]
    fn test_binary_op_arithmetic_plus() -> Result<(), DatabaseError> {
        let plus_i32_1 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(None),
            &BinaryOperator::Plus,
        )?;
        let plus_i32_2 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(None),
            &BinaryOperator::Plus,
        )?;
        let plus_i32_3 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Plus,
        )?;
        let plus_i32_4 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Plus,
        )?;

        assert_eq!(plus_i32_1, plus_i32_2);
        assert_eq!(plus_i32_2, plus_i32_3);
        assert_eq!(plus_i32_4, DataValue::Int32(Some(2)));

        let plus_i64_1 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(None),
            &BinaryOperator::Plus,
        )?;
        let plus_i64_2 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(None),
            &BinaryOperator::Plus,
        )?;
        let plus_i64_3 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Plus,
        )?;
        let plus_i64_4 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Plus,
        )?;

        assert_eq!(plus_i64_1, plus_i64_2);
        assert_eq!(plus_i64_2, plus_i64_3);
        assert_eq!(plus_i64_4, DataValue::Int64(Some(2)));

        let plus_f64_1 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(None),
            &BinaryOperator::Plus,
        )?;
        let plus_f64_2 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(None),
            &BinaryOperator::Plus,
        )?;
        let plus_f64_3 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Plus,
        )?;
        let plus_f64_4 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Plus,
        )?;

        assert_eq!(plus_f64_1, plus_f64_2);
        assert_eq!(plus_f64_2, plus_f64_3);
        assert_eq!(plus_f64_4, DataValue::Float64(Some(2.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_minus() -> Result<(), DatabaseError> {
        let minus_i32_1 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(None),
            &BinaryOperator::Minus,
        )?;
        let minus_i32_2 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(None),
            &BinaryOperator::Minus,
        )?;
        let minus_i32_3 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Minus,
        )?;
        let minus_i32_4 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Minus,
        )?;

        assert_eq!(minus_i32_1, minus_i32_2);
        assert_eq!(minus_i32_2, minus_i32_3);
        assert_eq!(minus_i32_4, DataValue::Int32(Some(0)));

        let minus_i64_1 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(None),
            &BinaryOperator::Minus,
        )?;
        let minus_i64_2 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(None),
            &BinaryOperator::Minus,
        )?;
        let minus_i64_3 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Minus,
        )?;
        let minus_i64_4 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Minus,
        )?;

        assert_eq!(minus_i64_1, minus_i64_2);
        assert_eq!(minus_i64_2, minus_i64_3);
        assert_eq!(minus_i64_4, DataValue::Int64(Some(0)));

        let minus_f64_1 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(None),
            &BinaryOperator::Minus,
        )?;
        let minus_f64_2 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(None),
            &BinaryOperator::Minus,
        )?;
        let minus_f64_3 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Minus,
        )?;
        let minus_f64_4 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Minus,
        )?;

        assert_eq!(minus_f64_1, minus_f64_2);
        assert_eq!(minus_f64_2, minus_f64_3);
        assert_eq!(minus_f64_4, DataValue::Float64(Some(0.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_multiply() -> Result<(), DatabaseError> {
        let multiply_i32_1 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i32_2 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i32_3 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i32_4 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Multiply,
        )?;

        assert_eq!(multiply_i32_1, multiply_i32_2);
        assert_eq!(multiply_i32_2, multiply_i32_3);
        assert_eq!(multiply_i32_4, DataValue::Int32(Some(1)));

        let multiply_i64_1 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i64_2 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i64_3 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Multiply,
        )?;
        let multiply_i64_4 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Multiply,
        )?;

        assert_eq!(multiply_i64_1, multiply_i64_2);
        assert_eq!(multiply_i64_2, multiply_i64_3);
        assert_eq!(multiply_i64_4, DataValue::Int64(Some(1)));

        let multiply_f64_1 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_f64_2 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(None),
            &BinaryOperator::Multiply,
        )?;
        let multiply_f64_3 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Multiply,
        )?;
        let multiply_f64_4 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Multiply,
        )?;

        assert_eq!(multiply_f64_1, multiply_f64_2);
        assert_eq!(multiply_f64_2, multiply_f64_3);
        assert_eq!(multiply_f64_4, DataValue::Float64(Some(1.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_divide() -> Result<(), DatabaseError> {
        let divide_i32_1 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(None),
            &BinaryOperator::Divide,
        )?;
        let divide_i32_2 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(None),
            &BinaryOperator::Divide,
        )?;
        let divide_i32_3 = DataValue::binary_op(
            &DataValue::Int32(None),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Divide,
        )?;
        let divide_i32_4 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Divide,
        )?;

        assert_eq!(divide_i32_1, divide_i32_2);
        assert_eq!(divide_i32_2, divide_i32_3);
        assert_eq!(divide_i32_4, DataValue::Float64(Some(1.0)));

        let divide_i64_1 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(None),
            &BinaryOperator::Divide,
        )?;
        let divide_i64_2 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(None),
            &BinaryOperator::Divide,
        )?;
        let divide_i64_3 = DataValue::binary_op(
            &DataValue::Int64(None),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Divide,
        )?;
        let divide_i64_4 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int64(Some(1)),
            &BinaryOperator::Divide,
        )?;

        assert_eq!(divide_i64_1, divide_i64_2);
        assert_eq!(divide_i64_2, divide_i64_3);
        assert_eq!(divide_i64_4, DataValue::Float64(Some(1.0)));

        let divide_f64_1 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(None),
            &BinaryOperator::Divide,
        )?;
        let divide_f64_2 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(None),
            &BinaryOperator::Divide,
        )?;
        let divide_f64_3 = DataValue::binary_op(
            &DataValue::Float64(None),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Divide,
        )?;
        let divide_f64_4 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float64(Some(1.0)),
            &BinaryOperator::Divide,
        )?;

        assert_eq!(divide_f64_1, divide_f64_2);
        assert_eq!(divide_f64_2, divide_f64_3);
        assert_eq!(divide_f64_4, DataValue::Float64(Some(1.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_cast() -> Result<(), DatabaseError> {
        let i32_cast_1 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int8(Some(1)),
            &BinaryOperator::Plus,
        )?;
        let i32_cast_2 = DataValue::binary_op(
            &DataValue::Int32(Some(1)),
            &DataValue::Int16(Some(1)),
            &BinaryOperator::Plus,
        )?;

        assert_eq!(i32_cast_1, i32_cast_2);

        let i64_cast_1 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int8(Some(1)),
            &BinaryOperator::Plus,
        )?;
        let i64_cast_2 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int16(Some(1)),
            &BinaryOperator::Plus,
        )?;
        let i64_cast_3 = DataValue::binary_op(
            &DataValue::Int64(Some(1)),
            &DataValue::Int32(Some(1)),
            &BinaryOperator::Plus,
        )?;

        assert_eq!(i64_cast_1, i64_cast_2);
        assert_eq!(i64_cast_2, i64_cast_3);

        let f64_cast_1 = DataValue::binary_op(
            &DataValue::Float64(Some(1.0)),
            &DataValue::Float32(Some(1.0)),
            &BinaryOperator::Plus,
        )?;
        assert_eq!(f64_cast_1, DataValue::Float64(Some(2.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_i32_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(Some(1)),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(Some(true))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(None)
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(Some(1)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int32(None),
                &DataValue::Int32(None),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_i64_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(Some(1)),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(Some(true))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(None)
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(Some(1)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Int64(None),
                &DataValue::Int64(None),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_f64_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(0.0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(0.0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(Some(1.0)),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(Some(true))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(0.0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(0.0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(None)
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(Some(1.0)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float64(None),
                &DataValue::Float64(None),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_f32_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(0.0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(0.0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(Some(1.0)),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(Some(true))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(0.0)),
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(0.0)),
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(None)
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(Some(1.0)),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Float32(None),
                &DataValue::Float32(None),
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(None)
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_bool_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(true)),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::And
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(false)),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::And
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(false)),
                &DataValue::Boolean(Some(false)),
                &BinaryOperator::And
            )?,
            DataValue::Boolean(Some(false))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(None),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::And
            )?,
            DataValue::Boolean(None)
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(true)),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::Or
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(false)),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::Or
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(Some(false)),
                &DataValue::Boolean(Some(false)),
                &BinaryOperator::Or
            )?,
            DataValue::Boolean(Some(false))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Boolean(None),
                &DataValue::Boolean(Some(true)),
                &BinaryOperator::Or
            )?,
            DataValue::Boolean(Some(true))
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_utf8_compare() -> Result<(), DatabaseError> {
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("b".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("b".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(Some(true))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(Some(false))
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::Eq
            )?,
            DataValue::Boolean(Some(true))
        );

        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::Gt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::Lt
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::GtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::LtEq
            )?,
            DataValue::Boolean(None)
        );
        assert_eq!(
            DataValue::binary_op(
                &DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: Some("a".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &BinaryOperator::NotEq
            )?,
            DataValue::Boolean(None)
        );

        Ok(())
    }
}
