pub mod index;
pub mod tuple;
pub mod tuple_builder;
pub mod value;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::cmp;

use crate::errors::DatabaseError;
use sqlparser::ast::{CharLengthUnits, ExactNumberInfo, TimezoneInfo};
use strum_macros::AsRefStr;

pub type ColumnId = u32;

/// Sqlrs type conversion:
/// sqlparser::ast::DataType -> LogicalType -> arrow::datatypes::DataType
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, AsRefStr, Serialize, Deserialize,
)]
pub enum LogicalType {
    Invalid,
    SqlNull,
    Boolean,
    Tinyint,
    UTinyint,
    Smallint,
    USmallint,
    Integer,
    UInteger,
    Bigint,
    UBigint,
    Float,
    Double,
    Char(u32, CharLengthUnits),
    Varchar(Option<u32>, CharLengthUnits),
    Date,
    DateTime,
    Time,
    // decimal (precision, scale)
    Decimal(Option<u8>, Option<u8>),
    Tuple,
}

impl LogicalType {
    pub fn type_trans<T: 'static>() -> Option<LogicalType> {
        let type_id = TypeId::of::<T>();

        if type_id == TypeId::of::<i8>() {
            Some(LogicalType::Tinyint)
        } else if type_id == TypeId::of::<i16>() {
            Some(LogicalType::Smallint)
        } else if type_id == TypeId::of::<i32>() {
            Some(LogicalType::Integer)
        } else if type_id == TypeId::of::<i64>() {
            Some(LogicalType::Bigint)
        } else if type_id == TypeId::of::<u8>() {
            Some(LogicalType::UTinyint)
        } else if type_id == TypeId::of::<u16>() {
            Some(LogicalType::USmallint)
        } else if type_id == TypeId::of::<u32>() {
            Some(LogicalType::UInteger)
        } else if type_id == TypeId::of::<u64>() {
            Some(LogicalType::UBigint)
        } else if type_id == TypeId::of::<f32>() {
            Some(LogicalType::Float)
        } else if type_id == TypeId::of::<f64>() {
            Some(LogicalType::Double)
        } else if type_id == TypeId::of::<NaiveDate>() {
            Some(LogicalType::Date)
        } else if type_id == TypeId::of::<NaiveDateTime>() {
            Some(LogicalType::DateTime)
        } else if type_id == TypeId::of::<NaiveTime>() {
            Some(LogicalType::Time)
        } else if type_id == TypeId::of::<Decimal>() {
            Some(LogicalType::Decimal(None, None))
        } else if type_id == TypeId::of::<String>() {
            Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
        } else {
            None
        }
    }

    pub fn raw_len(&self) -> Option<usize> {
        match self {
            LogicalType::SqlNull => Some(0),
            LogicalType::Boolean => Some(1),
            LogicalType::Tinyint => Some(1),
            LogicalType::UTinyint => Some(1),
            LogicalType::Smallint => Some(2),
            LogicalType::USmallint => Some(2),
            LogicalType::Integer => Some(4),
            LogicalType::UInteger => Some(4),
            LogicalType::Bigint => Some(8),
            LogicalType::UBigint => Some(8),
            LogicalType::Float => Some(4),
            LogicalType::Double => Some(8),
            /// Note: The non-fixed length type's raw_len is None e.g. Varchar
            LogicalType::Varchar(_, _) => None,
            LogicalType::Char(len, unit) => match unit {
                CharLengthUnits::Characters => None,
                CharLengthUnits::Octets => Some(*len as usize),
            },
            LogicalType::Decimal(_, _) => Some(16),
            LogicalType::Date => Some(4),
            LogicalType::DateTime => Some(8),
            LogicalType::Time => Some(4),
            LogicalType::Invalid | LogicalType::Tuple => unreachable!(),
        }
    }

    pub fn numeric() -> Vec<LogicalType> {
        vec![
            LogicalType::Tinyint,
            LogicalType::UTinyint,
            LogicalType::Smallint,
            LogicalType::USmallint,
            LogicalType::Integer,
            LogicalType::UInteger,
            LogicalType::Bigint,
            LogicalType::UBigint,
            LogicalType::Float,
            LogicalType::Double,
        ]
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::UTinyint
                | LogicalType::Smallint
                | LogicalType::USmallint
                | LogicalType::Integer
                | LogicalType::UInteger
                | LogicalType::Bigint
                | LogicalType::UBigint
                | LogicalType::Float
                | LogicalType::Double
        )
    }

    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::Smallint
                | LogicalType::Integer
                | LogicalType::Bigint
        )
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::UTinyint
                | LogicalType::USmallint
                | LogicalType::UInteger
                | LogicalType::UBigint
        )
    }

    pub fn is_floating_point_numeric(&self) -> bool {
        matches!(self, LogicalType::Float | LogicalType::Double)
    }

    pub fn max_logical_type(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, DatabaseError> {
        if left == right {
            return Ok(*left);
        }
        match (left, right) {
            // SqlNull type can be cast to anything
            (LogicalType::SqlNull, _) => return Ok(*right),
            (_, LogicalType::SqlNull) => return Ok(*left),
            _ => {}
        }
        if left.is_numeric() && right.is_numeric() {
            return LogicalType::combine_numeric_types(left, right);
        }
        if matches!(
            (left, right),
            (LogicalType::Date, LogicalType::Varchar(..))
                | (LogicalType::Varchar(..), LogicalType::Date)
        ) {
            return Ok(LogicalType::Date);
        }
        if matches!(
            (left, right),
            (LogicalType::Date, LogicalType::DateTime) | (LogicalType::DateTime, LogicalType::Date)
        ) {
            return Ok(LogicalType::DateTime);
        }
        if matches!(
            (left, right),
            (LogicalType::DateTime, LogicalType::Varchar(..))
                | (LogicalType::Varchar(..), LogicalType::DateTime)
        ) {
            return Ok(LogicalType::DateTime);
        }
        if let (LogicalType::Char(..), LogicalType::Varchar(len, ..))
        | (LogicalType::Varchar(len, ..), LogicalType::Char(..)) = (left, right)
        {
            return Ok(LogicalType::Varchar(*len, CharLengthUnits::Characters));
        }
        Err(DatabaseError::Incomparable(*left, *right))
    }

    fn combine_numeric_types(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, DatabaseError> {
        if left == right {
            return Ok(*left);
        }
        if left.is_signed_numeric() && right.is_unsigned_numeric() {
            // this method is symmetric
            // arrange it so the left type is smaller
            // to limit the number of options we need to check
            return LogicalType::combine_numeric_types(right, left);
        }

        if LogicalType::can_implicit_cast(left, right) {
            return Ok(*right);
        }
        if LogicalType::can_implicit_cast(right, left) {
            return Ok(*left);
        }
        // we can't cast implicitly either way and types are not equal
        // this happens when left is signed and right is unsigned
        // e.g. INTEGER and UINTEGER
        // in this case we need to upcast to make sure the types fit
        match (left, right) {
            (LogicalType::Bigint, _) | (_, LogicalType::UBigint) => Ok(LogicalType::Double),
            (LogicalType::Integer, _) | (_, LogicalType::UInteger) => Ok(LogicalType::Bigint),
            (LogicalType::Smallint, _) | (_, LogicalType::USmallint) => Ok(LogicalType::Integer),
            (LogicalType::Tinyint, _) | (_, LogicalType::UTinyint) => Ok(LogicalType::Smallint),
            _ => Err(DatabaseError::Incomparable(*left, *right)),
        }
    }

    pub fn can_implicit_cast(from: &LogicalType, to: &LogicalType) -> bool {
        if from == to {
            return true;
        }
        match from {
            LogicalType::Invalid => false,
            LogicalType::SqlNull => true,
            LogicalType::Boolean => false,
            LogicalType::Tinyint => matches!(
                to,
                LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::UTinyint => matches!(
                to,
                LogicalType::USmallint
                    | LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Smallint => matches!(
                to,
                LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::USmallint => matches!(
                to,
                LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Integer => matches!(
                to,
                LogicalType::Bigint | LogicalType::Float | LogicalType::Double
            ),
            LogicalType::UInteger => matches!(
                to,
                LogicalType::UBigint
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Bigint => matches!(to, LogicalType::Float | LogicalType::Double),
            LogicalType::UBigint => matches!(to, LogicalType::Float | LogicalType::Double),
            LogicalType::Float => matches!(to, LogicalType::Double),
            LogicalType::Double => false,
            LogicalType::Char(..) => false,
            LogicalType::Varchar(..) => false,
            LogicalType::Date => matches!(
                to,
                LogicalType::DateTime | LogicalType::Varchar(..) | LogicalType::Char(..)
            ),
            LogicalType::DateTime => matches!(
                to,
                LogicalType::Date
                    | LogicalType::Time
                    | LogicalType::Varchar(..)
                    | LogicalType::Char(..)
            ),
            LogicalType::Time => {
                matches!(to, LogicalType::Varchar(..) | LogicalType::Char(..))
            }
            LogicalType::Decimal(_, _) | LogicalType::Tuple => false,
        }
    }
}

/// sqlparser datatype to logical type
impl TryFrom<sqlparser::ast::DataType> for LogicalType {
    type Error = DatabaseError;

    fn try_from(value: sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Char(char_len)
            | sqlparser::ast::DataType::Character(char_len) => {
                let mut len = 1;
                let mut char_unit = None;
                if let Some(sqlparser::ast::CharacterLength { length, unit }) = char_len {
                    len = cmp::max(len, length);
                    char_unit = unit;
                }
                Ok(LogicalType::Char(
                    len as u32,
                    char_unit.unwrap_or(CharLengthUnits::Characters),
                ))
            }
            sqlparser::ast::DataType::CharVarying(varchar_len)
            | sqlparser::ast::DataType::CharacterVarying(varchar_len)
            | sqlparser::ast::DataType::Varchar(varchar_len) => {
                let mut len = None;
                let mut char_unit = None;
                if let Some(sqlparser::ast::CharacterLength { length, unit }) = varchar_len {
                    len = Some(length as u32);
                    char_unit = unit;
                }
                Ok(LogicalType::Varchar(
                    len,
                    char_unit.unwrap_or(CharLengthUnits::Characters),
                ))
            }
            sqlparser::ast::DataType::Float(_) => Ok(LogicalType::Float),
            sqlparser::ast::DataType::Double | sqlparser::ast::DataType::DoublePrecision => {
                Ok(LogicalType::Double)
            }
            sqlparser::ast::DataType::TinyInt(_) => Ok(LogicalType::Tinyint),
            sqlparser::ast::DataType::UnsignedTinyInt(_) => Ok(LogicalType::UTinyint),
            sqlparser::ast::DataType::SmallInt(_) => Ok(LogicalType::Smallint),
            sqlparser::ast::DataType::UnsignedSmallInt(_) => Ok(LogicalType::USmallint),
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                Ok(LogicalType::Integer)
            }
            sqlparser::ast::DataType::UnsignedInt(_)
            | sqlparser::ast::DataType::UnsignedInteger(_) => Ok(LogicalType::UInteger),
            sqlparser::ast::DataType::BigInt(_) => Ok(LogicalType::Bigint),
            sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(LogicalType::UBigint),
            sqlparser::ast::DataType::Boolean => Ok(LogicalType::Boolean),
            sqlparser::ast::DataType::Date => Ok(LogicalType::Date),
            sqlparser::ast::DataType::Datetime(precision) => {
                if precision.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "time's precision".to_string(),
                    ));
                }
                Ok(LogicalType::DateTime)
            }
            sqlparser::ast::DataType::Time(precision, info) => {
                if precision.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "time's precision".to_string(),
                    ));
                }
                if !matches!(info, TimezoneInfo::None) {
                    return Err(DatabaseError::UnsupportedStmt(
                        "time's time zone".to_string(),
                    ));
                }
                Ok(LogicalType::Time)
            }
            sqlparser::ast::DataType::Decimal(info) | sqlparser::ast::DataType::Dec(info) => {
                match info {
                    ExactNumberInfo::None => Ok(Self::Decimal(None, None)),
                    ExactNumberInfo::Precision(p) => Ok(Self::Decimal(Some(p as u8), None)),
                    ExactNumberInfo::PrecisionAndScale(p, s) => {
                        Ok(Self::Decimal(Some(p as u8), Some(s as u8)))
                    }
                }
            }
            other => Err(DatabaseError::UnsupportedStmt(other.to_string())),
        }
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref().to_uppercase())
    }
}
