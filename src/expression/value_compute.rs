use crate::errors::DatabaseError;
use crate::expression::UnaryOperator;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl DataValue {
    // FIXME: like BinaryEvaluator
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
}
