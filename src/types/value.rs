use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;
use chrono::{NaiveDateTime, Datelike, NaiveDate};
use chrono::format::{DelayedFormat, StrftimeItems};
use integer_encoding::FixedInt;
use lazy_static::lazy_static;

use ordered_float::OrderedFloat;
use crate::types::errors::TypeError;

use super::LogicalType;

lazy_static! {
    static ref UNIX_DATETIME: NaiveDateTime = {
        NaiveDateTime::from_timestamp_opt(0, 0).unwrap()
    };
}

pub const DATE_FMT: &str = "%Y-%m-%d";
pub const DATE_TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

pub type ValueRef = Arc<DataValue>;

#[derive(Clone)]
pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int timestamp since UNIX epoch 1970-01-01
    Date64(Option<i64>),
}

impl PartialEq for DataValue {
    fn eq(&self, other: &Self) -> bool {
        use DataValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1.eq(v2),
            (Utf8(_), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
        }
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use DataValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
            (Utf8(_), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
        }
    }
}

impl Eq for DataValue {}

impl Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use DataValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Float32(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Float64(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            Null => 1.hash(state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
        }
    }
}

impl DataValue {
    pub fn is_variable(&self) -> bool {
        match self {
            DataValue::Utf8(_) => true,
            _ => false
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            DataValue::Null => true,
            DataValue::Boolean(value) => value.is_none(),
            DataValue::Float32(value) => value.is_none(),
            DataValue::Float64(value) => value.is_none(),
            DataValue::Int8(value) => value.is_none(),
            DataValue::Int16(value) => value.is_none(),
            DataValue::Int32(value) => value.is_none(),
            DataValue::Int64(value) => value.is_none(),
            DataValue::UInt8(value) => value.is_none(),
            DataValue::UInt16(value) => value.is_none(),
            DataValue::UInt32(value) => value.is_none(),
            DataValue::UInt64(value) => value.is_none(),
            DataValue::Utf8(value) => value.is_none(),
            DataValue::Date32(value) => value.is_none(),
            DataValue::Date64(value) => value.is_none(),
        }
    }

    pub fn none(logic_type: &LogicalType) -> DataValue {
        match logic_type {
            LogicalType::Invalid => panic!("invalid logical type"),
            LogicalType::SqlNull => DataValue::Null,
            LogicalType::Boolean => DataValue::Boolean(None),
            LogicalType::Tinyint => DataValue::Int8(None),
            LogicalType::UTinyint => DataValue::UInt8(None),
            LogicalType::Smallint => DataValue::Int16(None),
            LogicalType::USmallint => DataValue::UInt16(None),
            LogicalType::Integer => DataValue::Int32(None),
            LogicalType::UInteger => DataValue::UInt32(None),
            LogicalType::Bigint => DataValue::Int64(None),
            LogicalType::UBigint => DataValue::UInt64(None),
            LogicalType::Float => DataValue::Float32(None),
            LogicalType::Double => DataValue::Float64(None),
            LogicalType::Varchar => DataValue::Utf8(None),
            LogicalType::Date => DataValue::Date32(None),
            LogicalType::DateTime => DataValue::Date64(None)
        }
    }

    pub fn init(logic_type: &LogicalType) -> DataValue {
        match logic_type {
            LogicalType::Invalid => panic!("invalid logical type"),
            LogicalType::SqlNull => DataValue::Null,
            LogicalType::Boolean => DataValue::Boolean(Some(false)),
            LogicalType::Tinyint => DataValue::Int8(Some(0)),
            LogicalType::UTinyint => DataValue::UInt8(Some(0)),
            LogicalType::Smallint => DataValue::Int16(Some(0)),
            LogicalType::USmallint => DataValue::UInt16(Some(0)),
            LogicalType::Integer => DataValue::Int32(Some(0)),
            LogicalType::UInteger => DataValue::UInt32(Some(0)),
            LogicalType::Bigint => DataValue::Int64(Some(0)),
            LogicalType::UBigint => DataValue::UInt64(Some(0)),
            LogicalType::Float => DataValue::Float32(Some(0.0)),
            LogicalType::Double => DataValue::Float64(Some(0.0)),
            LogicalType::Varchar => DataValue::Utf8(Some("".to_string())),
            LogicalType::Date => DataValue::Date32(Some(UNIX_DATETIME.num_days_from_ce())),
            LogicalType::DateTime => DataValue::Date64(Some(UNIX_DATETIME.timestamp()))
        }
    }

    pub fn to_raw(&self) -> Vec<u8> {
        match self {
            DataValue::Null => None,
            DataValue::Boolean(v) => v.map(|v| vec![v as u8]),
            DataValue::Float32(v) => v.map(|v| v.to_ne_bytes().to_vec()),
            DataValue::Float64(v) => v.map(|v| v.to_ne_bytes().to_vec()),
            DataValue::Int8(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::Int16(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::Int32(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::Int64(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::UInt8(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::UInt16(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::UInt32(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::UInt64(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::Utf8(v) => v.clone().map(|v| v.into_bytes()),
            DataValue::Date32(v) => v.map(|v| v.encode_fixed_vec()),
            DataValue::Date64(v) => v.map(|v| v.encode_fixed_vec()),
        }.unwrap_or(vec![])
    }

    pub fn from_raw(bytes: &[u8], ty: &LogicalType) -> Self {
        match ty {
            LogicalType::Invalid => panic!("invalid logical type"),
            LogicalType::SqlNull => DataValue::Null,
            LogicalType::Boolean => DataValue::Boolean(bytes.get(0).map(|v| *v != 0)),
            LogicalType::Tinyint => DataValue::Int8((!bytes.is_empty()).then(|| i8::decode_fixed(bytes))),
            LogicalType::UTinyint => DataValue::UInt8((!bytes.is_empty()).then(|| u8::decode_fixed(bytes))),
            LogicalType::Smallint => DataValue::Int16((!bytes.is_empty()).then(|| i16::decode_fixed(bytes))),
            LogicalType::USmallint => DataValue::UInt16((!bytes.is_empty()).then(|| u16::decode_fixed(bytes))),
            LogicalType::Integer => DataValue::Int32((!bytes.is_empty()).then(|| i32::decode_fixed(bytes))),
            LogicalType::UInteger => DataValue::UInt32((!bytes.is_empty()).then(|| u32::decode_fixed(bytes))),
            LogicalType::Bigint => DataValue::Int64((!bytes.is_empty()).then(|| i64::decode_fixed(bytes))),
            LogicalType::UBigint => DataValue::UInt64((!bytes.is_empty()).then(|| u64::decode_fixed(bytes))),
            LogicalType::Float => DataValue::Float32((!bytes.is_empty()).then(|| {
                let mut buf = [0; 4];
                buf.copy_from_slice(bytes);
                f32::from_ne_bytes(buf)
            })),
            LogicalType::Double => DataValue::Float64((!bytes.is_empty()).then(|| {
                let mut buf = [0; 8];
                buf.copy_from_slice(bytes);
                f64::from_ne_bytes(buf)
            })),
            LogicalType::Varchar => DataValue::Utf8((!bytes.is_empty()).then(|| String::from_utf8(bytes.to_owned()).unwrap())),
            LogicalType::Date => DataValue::Date32((!bytes.is_empty()).then(|| i32::decode_fixed(bytes))),
            LogicalType::DateTime => DataValue::Date64((!bytes.is_empty()).then(|| i64::decode_fixed(bytes))),
        }
    }

    pub fn logical_type(&self) -> LogicalType {
        match self {
            DataValue::Null => LogicalType::SqlNull,
            DataValue::Boolean(_) => LogicalType::Boolean,
            DataValue::Float32(_) => LogicalType::Float,
            DataValue::Float64(_) => LogicalType::Double,
            DataValue::Int8(_) => LogicalType::Tinyint,
            DataValue::Int16(_) => LogicalType::Smallint,
            DataValue::Int32(_) => LogicalType::Integer,
            DataValue::Int64(_) => LogicalType::Bigint,
            DataValue::UInt8(_) => LogicalType::UTinyint,
            DataValue::UInt16(_) => LogicalType::USmallint,
            DataValue::UInt32(_) => LogicalType::UInteger,
            DataValue::UInt64(_) => LogicalType::UBigint,
            DataValue::Utf8(_) => LogicalType::Varchar,
            DataValue::Date32(_) => LogicalType::Date,
            DataValue::Date64(_) => LogicalType::DateTime,
        }
    }
    
    pub fn cast(self, to: &LogicalType) -> Result<DataValue, TypeError> {
        match self {
            DataValue::Null => {
                match to {
                    LogicalType::Invalid => Err(TypeError::CastFail),
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Boolean => Ok(DataValue::Boolean(None)),
                    LogicalType::Tinyint => Ok(DataValue::Int8(None)),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(None)),
                    LogicalType::Smallint => Ok(DataValue::Int16(None)),
                    LogicalType::USmallint => Ok(DataValue::UInt16(None)),
                    LogicalType::Integer => Ok(DataValue::Int32(None)),
                    LogicalType::UInteger => Ok(DataValue::UInt32(None)),
                    LogicalType::Bigint => Ok(DataValue::Int64(None)),
                    LogicalType::UBigint => Ok(DataValue::UInt64(None)),
                    LogicalType::Float => Ok(DataValue::Float32(None)),
                    LogicalType::Double => Ok(DataValue::Float64(None)),
                    LogicalType::Varchar => Ok(DataValue::Utf8(None)),
                    LogicalType::Date => Ok(DataValue::Date32(None)),
                    LogicalType::DateTime => Ok(DataValue::Date64(None)),
                }
            }
            DataValue::Boolean(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Boolean => Ok(DataValue::Boolean(value)),
                    LogicalType::Tinyint => Ok(DataValue::Int8(value.map(|v| v.into()))),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(|v| v.into()))),
                    LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.map(|v| v.into()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Float32(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Float => Ok(DataValue::Float32(value)),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Float64(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Double => Ok(DataValue::Float64(value)),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Int8(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Tinyint => Ok(DataValue::Int8(value)),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.and_then(|v| u8::try_from(v).ok()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.and_then(|v| u16::try_from(v).ok()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.and_then(|v| u32::try_from(v).ok()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.and_then(|v| u64::try_from(v).ok()))),
                    LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Int16(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.and_then(|v| u8::try_from(v).ok()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.and_then(|v| u16::try_from(v).ok()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.and_then(|v| u32::try_from(v).ok()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.and_then(|v| u64::try_from(v).ok()))),
                    LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Int32(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.and_then(|v| u8::try_from(v).ok()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.and_then(|v| u16::try_from(v).ok()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.and_then(|v| u32::try_from(v).ok()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.and_then(|v| u64::try_from(v).ok()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Int64(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.and_then(|v| u8::try_from(v).ok()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.and_then(|v| u16::try_from(v).ok()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.and_then(|v| u32::try_from(v).ok()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.and_then(|v| u64::try_from(v).ok()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::UInt8(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value)),
                    LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.map(|v| v.into()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::UInt16(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.map(|v| v.into()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::UInt32(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::UInt64(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.map(|v| format!("{}", v)))),
                    _ => Err(TypeError::CastFail),
                }
            }
            DataValue::Utf8(value) => {
                match to {
                    LogicalType::Invalid => Err(TypeError::CastFail),
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Boolean => Ok(DataValue::Boolean(value.and_then(|v| bool::from_str(&v).ok()))),
                    LogicalType::Tinyint => Ok(DataValue::Int8(value.and_then(|v| i8::from_str(&v).ok()))),
                    LogicalType::UTinyint => Ok(DataValue::UInt8(value.and_then(|v| u8::from_str(&v).ok()))),
                    LogicalType::Smallint => Ok(DataValue::Int16(value.and_then(|v| i16::from_str(&v).ok()))),
                    LogicalType::USmallint => Ok(DataValue::UInt16(value.and_then(|v| u16::from_str(&v).ok()))),
                    LogicalType::Integer => Ok(DataValue::Int32(value.and_then(|v| i32::from_str(&v).ok()))),
                    LogicalType::UInteger => Ok(DataValue::UInt32(value.and_then(|v| u32::from_str(&v).ok()))),
                    LogicalType::Bigint => Ok(DataValue::Int64(value.and_then(|v| i64::from_str(&v).ok()))),
                    LogicalType::UBigint => Ok(DataValue::UInt64(value.and_then(|v| u64::from_str(&v).ok()))),
                    LogicalType::Float => Ok(DataValue::Float32(value.and_then(|v| f32::from_str(&v).ok()))),
                    LogicalType::Double => Ok(DataValue::Float64(value.and_then(|v| f64::from_str(&v).ok()))),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value)),
                    LogicalType::Date => {
                        let option = value.and_then(|v| {
                            NaiveDate::parse_from_str(&v, DATE_FMT)
                                .map(|date| date.num_days_from_ce())
                                .ok()
                        });

                        Ok(DataValue::Date32(option))
                    }
                    LogicalType::DateTime => {
                        let option = value.and_then(|v| {
                            NaiveDateTime::parse_from_str(&v, DATE_TIME_FMT)
                                .ok()
                                .or_else(|| {
                                    NaiveDate::parse_from_str(&v, DATE_FMT)
                                        .ok()
                                        .and_then(|date| date.and_hms_opt(0, 0 ,0))
                                })
                                .map(|date_time| date_time.timestamp())
                        });

                        Ok(DataValue::Date64(option))
                    }
                }
            }
            DataValue::Date32(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.and_then(|v| {
                        Self::date_format(v).map(|fmt| format!("{}", fmt))
                    }))),
                    LogicalType::Date => Ok(DataValue::Date32(value)),
                    LogicalType::DateTime => {
                        let option = value.and_then(|v| {
                            NaiveDate::from_num_days_from_ce_opt(v)
                                .and_then(|date| date.and_hms_opt(0, 0, 0))
                                .map(|date_time| date_time.timestamp())
                        });

                        Ok(DataValue::Date64(option))
                    },
                    _ => Err(TypeError::CastFail)
                }
            }
            DataValue::Date64(value) => {
                match to {
                    LogicalType::SqlNull => Ok(DataValue::Null),
                    LogicalType::Varchar => Ok(DataValue::Utf8(value.and_then(|v| {
                        Self::date_time_format(v).map(|fmt| format!("{}", fmt))
                    }))),
                    LogicalType::Date => {
                        let option = value.and_then(|v| {
                            NaiveDateTime::from_timestamp_opt(v, 0)
                                .map(|date_time| date_time.date().num_days_from_ce())
                        });

                        Ok(DataValue::Date32(option))
                    }
                    LogicalType::DateTime => Ok(DataValue::Date64(value)),
                    _ => Err(TypeError::CastFail),
                }
            }
        }
    }

    fn date_format<'a>(v: i32) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        NaiveDate::from_num_days_from_ce_opt(v)
            .map(|date| date.format(DATE_FMT))
    }

    fn date_time_format<'a>(v: i64) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        NaiveDateTime::from_timestamp_opt(v, 0)
            .map(|date_time| date_time.format(DATE_TIME_FMT))
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for DataValue {
            fn from(value: $ty) -> Self {
                DataValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for DataValue {
            fn from(value: Option<$ty>) -> Self {
                DataValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);
impl_scalar!(String, Utf8);

impl From<&sqlparser::ast::Value> for DataValue {
    fn from(v: &sqlparser::ast::Value) -> Self {
        match v {
            sqlparser::ast::Value::Number(n, _) => {
                // use i32 to handle most cases
                if let Ok(v) = n.parse::<i32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<i64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f64>() {
                    v.into()
                } else {
                    panic!("unsupported number {:?}", n)
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::Boolean(b) => (*b).into(),
            sqlparser::ast::Value::Null => Self::Null,
            _ => todo!("unsupported parsed scalar value {:?}", v),
        }
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "null"),
        }
    }};
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataValue::Boolean(e) => format_option!(f, e)?,
            DataValue::Float32(e) => format_option!(f, e)?,
            DataValue::Float64(e) => format_option!(f, e)?,
            DataValue::Int8(e) => format_option!(f, e)?,
            DataValue::Int16(e) => format_option!(f, e)?,
            DataValue::Int32(e) => format_option!(f, e)?,
            DataValue::Int64(e) => format_option!(f, e)?,
            DataValue::UInt8(e) => format_option!(f, e)?,
            DataValue::UInt16(e) => format_option!(f, e)?,
            DataValue::UInt32(e) => format_option!(f, e)?,
            DataValue::UInt64(e) => format_option!(f, e)?,
            DataValue::Utf8(e) => format_option!(f, e)?,
            DataValue::Null => write!(f, "null")?,
            DataValue::Date32(e) => {
                format_option!(f, e.and_then(|s| DataValue::date_format(s)))?
            }
            DataValue::Date64(e) => {
                format_option!(f, e.and_then(|s| DataValue::date_time_format(s)))?
            },
        };
        Ok(())
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataValue::Boolean(_) => write!(f, "Boolean({})", self),
            DataValue::Float32(_) => write!(f, "Float32({})", self),
            DataValue::Float64(_) => write!(f, "Float64({})", self),
            DataValue::Int8(_) => write!(f, "Int8({})", self),
            DataValue::Int16(_) => write!(f, "Int16({})", self),
            DataValue::Int32(_) => write!(f, "Int32({})", self),
            DataValue::Int64(_) => write!(f, "Int64({})", self),
            DataValue::UInt8(_) => write!(f, "UInt8({})", self),
            DataValue::UInt16(_) => write!(f, "UInt16({})", self),
            DataValue::UInt32(_) => write!(f, "UInt32({})", self),
            DataValue::UInt64(_) => write!(f, "UInt64({})", self),
            DataValue::Utf8(None) => write!(f, "Utf8({})", self),
            DataValue::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
            DataValue::Null => write!(f, "null"),
            DataValue::Date32(_) => write!(f, "Date32({})", self),
            DataValue::Date64(_) => write!(f, "Date64({})", self),
        }
    }
}
