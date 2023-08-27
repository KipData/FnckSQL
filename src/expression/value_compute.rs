use crate::expression::BinaryOperator;
use crate::types::LogicalType;
use crate::types::value::DataValue;

fn unpack_i32(value: DataValue) -> Option<i32> {
    match value {
        DataValue::Int32(inner) => inner,
        _ => None
    }
}

fn unpack_i64(value: DataValue) -> Option<i64> {
    match value {
        DataValue::Int64(inner) => inner,
        _ => None
    }
}

fn unpack_f64(value: DataValue) -> Option<f64> {
    match value {
        DataValue::Float64(inner) => inner,
        _ => None
    }
}

fn unpack_f32(value: DataValue) -> Option<f32> {
    match value {
        DataValue::Float32(inner) => inner,
        _ => None
    }
}

fn unpack_bool(value: DataValue) -> Option<bool> {
    match value {
        DataValue::Boolean(inner) => inner,
        _ => None
    }
}

/// Tips:
/// - Null values operate as null values
pub fn binary_op(
    left: &DataValue,
    right: &DataValue,
    op: &BinaryOperator,
) -> DataValue {
    let unified_type = LogicalType::max_logical_type(&left.logical_type(), &right.logical_type()).unwrap();

    match &unified_type {
        LogicalType::Integer => {
            let left_value = unpack_i32(left.clone().cast(&unified_type));
            let right_value = unpack_i32(right.clone().cast(&unified_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }

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
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        }
        LogicalType::Bigint => {
            let left_value = unpack_i64(left.clone().cast(&unified_type));
            let right_value = unpack_i64(right.clone().cast(&unified_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }

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
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        }
        LogicalType::Double => {
            let left_value = unpack_f64(left.clone().cast(&unified_type));
            let right_value = unpack_f64(right.clone().cast(&unified_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }

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
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        }
        LogicalType::Boolean => {
            let left_value = unpack_bool(left.clone().cast(&unified_type));
            let right_value = unpack_bool(right.clone().cast(&unified_type));

            match op {
                BinaryOperator::And => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 && v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Or => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 || v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        }
        LogicalType::Float => {
            let left_value = unpack_f32(left.clone().cast(&unified_type));
            let right_value = unpack_f32(right.clone().cast(&unified_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Float32(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Float32(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Float32(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Float32(value)
                }
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
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        }
        LogicalType::SqlNull => {
            DataValue::Boolean(None)
        }
        _ => todo!("unsupported data type"),
    }
}

#[cfg(test)]
mod test {
    use crate::expression::value_compute::binary_op;
    use crate::expression::BinaryOperator;
    use crate::types::value::DataValue;

    #[test]
    fn test_binary_op_arithmetic_plus() {
        let plus_i32_1 = binary_op(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Plus);
        let plus_i32_2 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Plus);
        let plus_i32_3 = binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);
        let plus_i32_4 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);

        assert_eq!(plus_i32_1, plus_i32_2);
        assert_eq!(plus_i32_2, plus_i32_3);
        assert_eq!(plus_i32_4, DataValue::Int32(Some(2)));

        let plus_i64_1 = binary_op(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Plus);
        let plus_i64_2 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Plus);
        let plus_i64_3 = binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Plus);
        let plus_i64_4 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Plus);

        assert_eq!(plus_i64_1, plus_i64_2);
        assert_eq!(plus_i64_2, plus_i64_3);
        assert_eq!(plus_i64_4, DataValue::Int64(Some(2)));

        let plus_f64_1 = binary_op(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Plus);
        let plus_f64_2 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Plus);
        let plus_f64_3 = binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Plus);
        let plus_f64_4 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Plus);

        assert_eq!(plus_f64_1, plus_f64_2);
        assert_eq!(plus_f64_2, plus_f64_3);
        assert_eq!(plus_f64_4, DataValue::Float64(Some(2.0)));
    }

    #[test]
    fn test_binary_op_arithmetic_minus() {
        let minus_i32_1 = binary_op(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Minus);
        let minus_i32_2 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Minus);
        let minus_i32_3 = binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Minus);
        let minus_i32_4 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Minus);

        assert_eq!(minus_i32_1, minus_i32_2);
        assert_eq!(minus_i32_2, minus_i32_3);
        assert_eq!(minus_i32_4, DataValue::Int32(Some(0)));

        let minus_i64_1 = binary_op(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Minus);
        let minus_i64_2 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Minus);
        let minus_i64_3 = binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Minus);
        let minus_i64_4 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Minus);

        assert_eq!(minus_i64_1, minus_i64_2);
        assert_eq!(minus_i64_2, minus_i64_3);
        assert_eq!(minus_i64_4, DataValue::Int64(Some(0)));

        let minus_f64_1 = binary_op(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Minus);
        let minus_f64_2 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Minus);
        let minus_f64_3 = binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Minus);
        let minus_f64_4 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Minus);

        assert_eq!(minus_f64_1, minus_f64_2);
        assert_eq!(minus_f64_2, minus_f64_3);
        assert_eq!(minus_f64_4, DataValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_binary_op_arithmetic_multiply() {
        let multiply_i32_1 = binary_op(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Multiply);
        let multiply_i32_2 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Multiply);
        let multiply_i32_3 = binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Multiply);
        let multiply_i32_4 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Multiply);

        assert_eq!(multiply_i32_1, multiply_i32_2);
        assert_eq!(multiply_i32_2, multiply_i32_3);
        assert_eq!(multiply_i32_4, DataValue::Int32(Some(1)));

        let multiply_i64_1 = binary_op(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Multiply);
        let multiply_i64_2 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Multiply);
        let multiply_i64_3 = binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Multiply);
        let multiply_i64_4 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Multiply);

        assert_eq!(multiply_i64_1, multiply_i64_2);
        assert_eq!(multiply_i64_2, multiply_i64_3);
        assert_eq!(multiply_i64_4, DataValue::Int64(Some(1)));

        let multiply_f64_1 = binary_op(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Multiply);
        let multiply_f64_2 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Multiply);
        let multiply_f64_3 = binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Multiply);
        let multiply_f64_4 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Multiply);

        assert_eq!(multiply_f64_1, multiply_f64_2);
        assert_eq!(multiply_f64_2, multiply_f64_3);
        assert_eq!(multiply_f64_4, DataValue::Float64(Some(1.0)));
    }

    #[test]
    fn test_binary_op_arithmetic_divide() {
        let divide_i32_1 = binary_op(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Divide);
        let divide_i32_2 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Divide);
        let divide_i32_3 = binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Divide);
        let divide_i32_4 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Divide);

        assert_eq!(divide_i32_1, divide_i32_2);
        assert_eq!(divide_i32_2, divide_i32_3);
        assert_eq!(divide_i32_4, DataValue::Int32(Some(1)));

        let divide_i64_1 = binary_op(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Divide);
        let divide_i64_2 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Divide);
        let divide_i64_3 = binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Divide);
        let divide_i64_4 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Divide);

        assert_eq!(divide_i64_1, divide_i64_2);
        assert_eq!(divide_i64_2, divide_i64_3);
        assert_eq!(divide_i64_4, DataValue::Int64(Some(1)));

        let divide_f64_1 = binary_op(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Divide);
        let divide_f64_2 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Divide);
        let divide_f64_3 = binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Divide);
        let divide_f64_4 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Divide);

        assert_eq!(divide_f64_1, divide_f64_2);
        assert_eq!(divide_f64_2, divide_f64_3);
        assert_eq!(divide_f64_4, DataValue::Float64(Some(1.0)));
    }

    #[test]
    fn test_binary_op_cast() {
        let i32_cast_1 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int8(Some(1)), &BinaryOperator::Plus);
        let i32_cast_2 = binary_op(&DataValue::Int32(Some(1)), &DataValue::Int16(Some(1)), &BinaryOperator::Plus);

        assert_eq!(i32_cast_1, i32_cast_2);

        let i32_cast_2 = binary_op(&DataValue::Int16(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);

        let i64_cast_1 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int8(Some(1)), &BinaryOperator::Plus);
        let i64_cast_2 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int16(Some(1)), &BinaryOperator::Plus);
        let i64_cast_3 = binary_op(&DataValue::Int64(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);

        assert_eq!(i64_cast_1, i64_cast_2);
        assert_eq!(i64_cast_2, i64_cast_3);

        let f64_cast_1 = binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::Plus);
        assert_eq!(f64_cast_1, DataValue::Float64(Some(2.0)));
    }

    #[test]
    fn test_binary_op_i32_compare() {
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_i64_compare() {
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_f64_compare() {
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_f32_compare() {
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Float32(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op(&DataValue::Float32(None), &DataValue::Float32(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_bool_compare() {
        assert_eq!(binary_op(&DataValue::Boolean(Some(true)), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(false)), &BinaryOperator::And), DataValue::Boolean(Some(false)));

        assert_eq!(binary_op(&DataValue::Boolean(None), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(None));

        assert_eq!(binary_op(&DataValue::Boolean(Some(true)), &DataValue::Boolean(Some(true)), &BinaryOperator::Or), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(true)), &BinaryOperator::Or), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(false)), &BinaryOperator::Or), DataValue::Boolean(Some(false)));

        assert_eq!(binary_op(&DataValue::Boolean(None), &DataValue::Boolean(Some(true)), &BinaryOperator::Or), DataValue::Boolean(None));
    }
}
