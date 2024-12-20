use super::LogicalType;
use crate::errors::DatabaseError;
use crate::storage::table_codec::{BumpBytes, BOUND_MAX_TAG, BOUND_MIN_TAG};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::str::FromStr;
use std::sync::LazyLock;
use std::{cmp, fmt, mem};

pub static NULL_VALUE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Null);

static UNIX_DATETIME: LazyLock<NaiveDateTime> =
    LazyLock::new(|| DateTime::from_timestamp(0, 0).unwrap().naive_utc());

static UNIX_TIME: LazyLock<NaiveTime> = LazyLock::new(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap());

pub const DATE_FMT: &str = "%Y-%m-%d";
pub const DATE_TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
pub const TIME_FMT: &str = "%H:%M:%S";

const ENCODE_GROUP_SIZE: usize = 8;
const ENCODE_MARKER: u8 = 0xFF;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Utf8Type {
    Variable(Option<u32>),
    Fixed(u32),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    Utf8 {
        value: Option<String>,
        ty: Utf8Type,
        unit: CharLengthUnits,
    },
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int timestamp since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    Time(Option<u32>),
    Decimal(Option<Decimal>),
    /// (values, is_upper)
    Tuple(Option<(Vec<DataValue>, bool)>),
}

macro_rules! generate_get_option {
    ($data_value:ident, $($prefix:ident : $variant:ident($field:ty)),*) => {
        impl $data_value {
            $(
                pub fn $prefix(&self) -> $field {
                    if let $data_value::$variant(Some(val)) = self {
                        Some(val.clone())
                    } else {
                        None
                    }
                }
            )*
        }
    };
}

generate_get_option!(DataValue,
    bool : Boolean(Option<bool>),
    float : Float32(Option<f32>),
    double : Float64(Option<f64>),
    i8 : Int8(Option<i8>),
    i16 : Int16(Option<i16>),
    i32 : Int32(Option<i32>),
    i64 : Int64(Option<i64>),
    u8 : UInt8(Option<u8>),
    u16 : UInt16(Option<u16>),
    u32 : UInt32(Option<u32>),
    u64 : UInt64(Option<u64>),
    decimal : Decimal(Option<Decimal>)
);

impl PartialEq for DataValue {
    fn eq(&self, other: &Self) -> bool {
        use DataValue::*;

        if self.is_null() && other.is_null() {
            return true;
        }

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
            (Utf8 { value: v1, .. }, Utf8 { value: v2, .. }) => v1.eq(v2),
            (Utf8 { .. }, _) => false,
            (Null, Null) => true,
            (Null, _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
            (Time(v1), Time(v2)) => v1.eq(v2),
            (Time(_), _) => false,
            (Decimal(v1), Decimal(v2)) => v1.eq(v2),
            (Decimal(_), _) => false,
            (Tuple(values_1), Tuple(values_2)) => values_1.eq(values_2),
            (Tuple(_), _) => false,
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
            (Utf8 { value: v1, .. }, Utf8 { value: v2, .. }) => v1.partial_cmp(v2),
            (Utf8 { .. }, _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
            (Time(v1), Time(v2)) => v1.partial_cmp(v2),
            (Time(_), _) => None,
            (Decimal(v1), Decimal(v2)) => v1.partial_cmp(v2),
            (Decimal(_), _) => None,
            (Tuple(_), _) => None,
        }
    }
}

macro_rules! encode_u {
    ($writer:ident, $u:expr) => {
        $writer.write_all(&$u.to_be_bytes())?
    };
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
            Utf8 { value: v, .. } => v.hash(state),
            Null => 1.hash(state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Time(v) => v.hash(state),
            Decimal(v) => v.hash(state),
            Tuple(values) => values.hash(state),
        }
    }
}
macro_rules! varchar_cast {
    ($value:expr, $len:expr, $ty:expr, $unit:expr) => {
        $value
            .map(|v| {
                let string_value = format!("{}", v);
                if let Some(len) = $len {
                    if Self::check_string_len(&string_value, *len as usize, $unit) {
                        return Err(DatabaseError::TooLong);
                    }
                }
                Ok(DataValue::Utf8 {
                    value: Some(string_value),
                    ty: $ty,
                    unit: $unit,
                })
            })
            .unwrap_or(Ok(DataValue::Utf8 {
                value: None,
                ty: $ty,
                unit: $unit,
            }))
    };
}

macro_rules! numeric_to_boolean {
    ($value:expr) => {
        match $value {
            Some(0) => Ok(DataValue::Boolean(Some(false))),
            Some(1) => Ok(DataValue::Boolean(Some(true))),
            _ => Err(DatabaseError::CastFail),
        }
    };
}

impl DataValue {
    pub fn utf8(&self) -> Option<String> {
        if let DataValue::Utf8 {
            value: Some(val), ..
        } = self
        {
            Some(val.clone())
        } else {
            None
        }
    }

    pub fn date(&self) -> Option<NaiveDate> {
        if let DataValue::Date32(Some(val)) = self {
            NaiveDate::from_num_days_from_ce_opt(*val)
        } else {
            None
        }
    }

    pub fn datetime(&self) -> Option<NaiveDateTime> {
        if let DataValue::Date64(Some(val)) = self {
            DateTime::from_timestamp(*val, 0).map(|dt| dt.naive_utc())
        } else {
            None
        }
    }

    pub fn time(&self) -> Option<NaiveTime> {
        if let DataValue::Time(Some(val)) = self {
            NaiveTime::from_num_seconds_from_midnight_opt(*val, 0)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn check_string_len(string: &str, len: usize, unit: CharLengthUnits) -> bool {
        match unit {
            CharLengthUnits::Characters => string.chars().count() > len,
            CharLengthUnits::Octets => string.as_bytes().len() > len,
        }
    }

    #[inline]
    pub(crate) fn check_len(&self, logic_type: &LogicalType) -> Result<(), DatabaseError> {
        let is_over_len = match (logic_type, self) {
            (LogicalType::Varchar(None, _), _) => false,
            (
                LogicalType::Varchar(Some(len), CharLengthUnits::Characters),
                DataValue::Utf8 {
                    value: Some(val),
                    ty: Utf8Type::Variable(_),
                    unit: CharLengthUnits::Characters,
                },
            )
            | (
                LogicalType::Char(len, CharLengthUnits::Characters),
                DataValue::Utf8 {
                    value: Some(val),
                    ty: Utf8Type::Fixed(_),
                    unit: CharLengthUnits::Characters,
                },
            ) => Self::check_string_len(val, *len as usize, CharLengthUnits::Characters),
            (
                LogicalType::Varchar(Some(len), CharLengthUnits::Octets),
                DataValue::Utf8 {
                    value: Some(val),
                    ty: Utf8Type::Variable(_),
                    unit: CharLengthUnits::Octets,
                },
            )
            | (
                LogicalType::Char(len, CharLengthUnits::Octets),
                DataValue::Utf8 {
                    value: Some(val),
                    ty: Utf8Type::Fixed(_),
                    unit: CharLengthUnits::Octets,
                },
            ) => Self::check_string_len(val, *len as usize, CharLengthUnits::Octets),
            (LogicalType::Decimal(full_len, scale_len), DataValue::Decimal(Some(val))) => {
                if let Some(len) = full_len {
                    let mantissa = val.mantissa().abs();
                    if mantissa != 0 && mantissa.ilog10() + 1 > *len as u32 {
                        return Err(DatabaseError::TooLong);
                    }
                }
                if let Some(len) = scale_len {
                    if val.scale() > *len as u32 {
                        return Err(DatabaseError::TooLong);
                    }
                }
                false
            }
            _ => false,
        };

        if is_over_len {
            return Err(DatabaseError::TooLong);
        }

        Ok(())
    }

    fn format_date(value: Option<i32>) -> Option<String> {
        value.and_then(|v| Self::date_format(v).map(|fmt| format!("{}", fmt)))
    }

    fn format_datetime(value: Option<i64>) -> Option<String> {
        value.and_then(|v| Self::date_time_format(v).map(|fmt| format!("{}", fmt)))
    }

    fn format_time(value: Option<u32>) -> Option<String> {
        value.and_then(|v| Self::time_format(v).map(|fmt| format!("{}", fmt)))
    }

    #[inline]
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
            DataValue::Utf8 { value, .. } => value.is_none(),
            DataValue::Date32(value) => value.is_none(),
            DataValue::Date64(value) => value.is_none(),
            DataValue::Time(value) => value.is_none(),
            DataValue::Decimal(value) => value.is_none(),
            DataValue::Tuple(value) => value.is_none(),
        }
    }

    #[inline]
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
            LogicalType::Char(len, unit) => DataValue::Utf8 {
                value: None,
                ty: Utf8Type::Fixed(*len),
                unit: *unit,
            },
            LogicalType::Varchar(len, unit) => DataValue::Utf8 {
                value: None,
                ty: Utf8Type::Variable(*len),
                unit: *unit,
            },
            LogicalType::Date => DataValue::Date32(None),
            LogicalType::DateTime => DataValue::Date64(None),
            LogicalType::Time => DataValue::Time(None),
            LogicalType::Decimal(_, _) => DataValue::Decimal(None),
            LogicalType::Tuple(_) => DataValue::Tuple(None),
        }
    }

    #[inline]
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
            LogicalType::Char(len, unit) => DataValue::Utf8 {
                value: Some(String::new()),
                ty: Utf8Type::Fixed(*len),
                unit: *unit,
            },
            LogicalType::Varchar(len, unit) => DataValue::Utf8 {
                value: Some(String::new()),
                ty: Utf8Type::Variable(*len),
                unit: *unit,
            },
            LogicalType::Date => DataValue::Date32(Some(UNIX_DATETIME.num_days_from_ce())),
            LogicalType::DateTime => DataValue::Date64(Some(UNIX_DATETIME.and_utc().timestamp())),
            LogicalType::Time => DataValue::Time(Some(UNIX_TIME.num_seconds_from_midnight())),
            LogicalType::Decimal(_, _) => DataValue::Decimal(Some(Decimal::new(0, 0))),
            LogicalType::Tuple(types) => {
                let values = types.iter().map(DataValue::init).collect_vec();

                DataValue::Tuple(Some((values, false)))
            }
        }
    }

    #[inline]
    pub fn to_raw<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        match self {
            DataValue::Null => (),
            DataValue::Boolean(v) => {
                if let Some(v) = v {
                    writer.write_u8(*v as u8)?;
                    return Ok(());
                }
            }
            DataValue::Float32(v) => {
                if let Some(v) = v {
                    writer.write_f32::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Float64(v) => {
                if let Some(v) = v {
                    writer.write_f64::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Int8(v) => {
                if let Some(v) = v {
                    writer.write_i8(*v)?;
                    return Ok(());
                }
            }
            DataValue::Int16(v) => {
                if let Some(v) = v {
                    writer.write_i16::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Int32(v) => {
                if let Some(v) = v {
                    writer.write_i32::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Int64(v) => {
                if let Some(v) = v {
                    writer.write_i64::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::UInt8(v) => {
                if let Some(v) = v {
                    writer.write_u8(*v)?;
                    return Ok(());
                }
            }
            DataValue::UInt16(v) => {
                if let Some(v) = v {
                    writer.write_u16::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::UInt32(v) => {
                if let Some(v) = v {
                    writer.write_u32::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::UInt64(v) => {
                if let Some(v) = v {
                    writer.write_u64::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Utf8 { value: v, ty, unit } => {
                if let Some(v) = v {
                    match ty {
                        Utf8Type::Variable(_) => {
                            let bytes = v.as_bytes();

                            writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                            writer.write_all(bytes)?;
                            return Ok(());
                        }
                        Utf8Type::Fixed(len) => match unit {
                            CharLengthUnits::Characters => {
                                let chars_len = *len as usize;
                                let v = format!("{:len$}", v, len = chars_len);
                                let bytes = v.as_bytes();

                                writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                                writer.write_all(bytes)?;
                                return Ok(());
                            }
                            CharLengthUnits::Octets => {
                                let octets_len = *len as usize;
                                let bytes = v.as_bytes();
                                debug_assert!(octets_len >= bytes.len());

                                writer.write_all(bytes)?;
                                for _ in 0..octets_len - bytes.len() {
                                    writer.write_u8(b' ')?;
                                }
                                return Ok(());
                            }
                        },
                    }
                }
            }
            DataValue::Date32(v) => {
                if let Some(v) = v {
                    writer.write_i32::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Date64(v) => {
                if let Some(v) = v {
                    writer.write_i64::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Time(v) => {
                if let Some(v) = v {
                    writer.write_u32::<LittleEndian>(*v)?;
                    return Ok(());
                }
            }
            DataValue::Decimal(v) => {
                if let Some(v) = v {
                    writer.write_all(&v.serialize())?;
                    return Ok(());
                }
            }
            DataValue::Tuple(_) => unreachable!(),
        }
        Ok(())
    }

    #[inline]
    pub fn from_raw<R: Read + Seek>(
        reader: &mut R,
        ty: &LogicalType,
        is_projection: bool,
    ) -> Result<Option<Self>, DatabaseError> {
        let value = match ty {
            LogicalType::Invalid => panic!("invalid logical type"),
            LogicalType::SqlNull => {
                if !is_projection {
                    return Ok(None);
                }
                DataValue::Null
            }
            LogicalType::Boolean => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::Boolean(Some(reader.read_u8()? != 0))
            }
            LogicalType::Tinyint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::Int8(Some(reader.read_i8()?))
            }
            LogicalType::UTinyint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::UInt8(Some(reader.read_u8()?))
            }
            LogicalType::Smallint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(2))?;
                    return Ok(None);
                }
                DataValue::Int16(Some(reader.read_i16::<LittleEndian>()?))
            }
            LogicalType::USmallint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(2))?;
                    return Ok(None);
                }
                DataValue::UInt16(Some(reader.read_u16::<LittleEndian>()?))
            }
            LogicalType::Integer => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Int32(Some(reader.read_i32::<LittleEndian>()?))
            }
            LogicalType::UInteger => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::UInt32(Some(reader.read_u32::<LittleEndian>()?))
            }
            LogicalType::Bigint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Int64(Some(reader.read_i64::<LittleEndian>()?))
            }
            LogicalType::UBigint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::UInt64(Some(reader.read_u64::<LittleEndian>()?))
            }
            LogicalType::Float => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Float32(Some(reader.read_f32::<LittleEndian>()?))
            }
            LogicalType::Double => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Float64(Some(reader.read_f64::<LittleEndian>()?))
            }
            LogicalType::Char(ty_len, unit) => {
                // https://dev.mysql.com/doc/refman/8.0/en/char.html#:~:text=If%20a%20given%20value%20is%20stored%20into%20the%20CHAR(4)%20and%20VARCHAR(4)%20columns%2C%20the%20values%20retrieved%20from%20the%20columns%20are%20not%20always%20the%20same%20because%20trailing%20spaces%20are%20removed%20from%20CHAR%20columns%20upon%20retrieval.%20The%20following%20example%20illustrates%20this%20difference%3A
                let len = match unit {
                    CharLengthUnits::Characters => reader.read_u32::<LittleEndian>()?,
                    CharLengthUnits::Octets => *ty_len,
                } as usize;
                if !is_projection {
                    reader.seek(SeekFrom::Current(len as i64))?;
                    return Ok(None);
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;
                let last_non_zero_index = match bytes.iter().rposition(|&x| x != b' ') {
                    Some(index) => index + 1,
                    None => 0,
                };
                bytes.truncate(last_non_zero_index);

                DataValue::Utf8 {
                    value: Some(String::from_utf8(bytes)?),
                    ty: Utf8Type::Fixed(*ty_len),
                    unit: *unit,
                }
            }
            LogicalType::Varchar(ty_len, unit) => {
                let len = reader.read_u32::<LittleEndian>()? as usize;
                if !is_projection {
                    reader.seek(SeekFrom::Current(len as i64))?;
                    return Ok(None);
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;

                DataValue::Utf8 {
                    value: Some(String::from_utf8(bytes)?),
                    ty: Utf8Type::Variable(*ty_len),
                    unit: *unit,
                }
            }
            LogicalType::Date => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Date32(Some(reader.read_i32::<LittleEndian>()?))
            }
            LogicalType::DateTime => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Date64(Some(reader.read_i64::<LittleEndian>()?))
            }
            LogicalType::Time => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Time(Some(reader.read_u32::<LittleEndian>()?))
            }
            LogicalType::Decimal(_, _) => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(16))?;
                    return Ok(None);
                }
                let mut bytes = [0u8; 16];
                reader.read_exact(&mut bytes)?;

                DataValue::Decimal(Some(Decimal::deserialize(bytes)))
            }
            LogicalType::Tuple(_) => unreachable!(),
        };
        Ok(Some(value))
    }

    #[inline]
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
            DataValue::Utf8 {
                ty: Utf8Type::Variable(len),
                unit,
                ..
            } => LogicalType::Varchar(*len, *unit),
            DataValue::Utf8 {
                ty: Utf8Type::Fixed(len),
                unit,
                ..
            } => LogicalType::Char(*len, *unit),
            DataValue::Date32(_) => LogicalType::Date,
            DataValue::Date64(_) => LogicalType::DateTime,
            DataValue::Time(_) => LogicalType::Time,
            DataValue::Decimal(_) => LogicalType::Decimal(None, None),
            DataValue::Tuple(values) => {
                if let Some((values, _)) = values {
                    let types = values.iter().map(|v| v.logical_type()).collect_vec();
                    LogicalType::Tuple(types)
                } else {
                    LogicalType::Tuple(vec![])
                }
            }
        }
    }

    // EncodeBytes guarantees the encoded value is in ascending order for comparison,
    // encoding with the following rule:
    //
    //	[group1][marker1]...[groupN][markerN]
    //	group is 8 bytes slice which is padding with 0.
    //	marker is `0xFF - padding 0 count`
    //
    // For example:
    //
    //	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
    //	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
    //	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
    //	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
    //
    // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    #[inline]
    fn encode_bytes(b: &mut BumpBytes, data: &[u8]) {
        let d_len = data.len();
        let realloc_size = (d_len / ENCODE_GROUP_SIZE + 1) * (ENCODE_GROUP_SIZE + 1);
        Self::realloc_bytes(b, realloc_size);

        let mut idx = 0;
        while idx <= d_len {
            let remain = d_len - idx;
            let pad_count: usize;

            if remain >= ENCODE_GROUP_SIZE {
                b.extend_from_slice(&data[idx..idx + ENCODE_GROUP_SIZE]);
                pad_count = 0;
            } else {
                pad_count = ENCODE_GROUP_SIZE - remain;
                b.extend_from_slice(&data[idx..]);
                b.extend_from_slice(&vec![0; pad_count]);
            }

            b.push(ENCODE_MARKER - pad_count as u8);
            idx += ENCODE_GROUP_SIZE;
        }
    }

    #[inline]
    fn realloc_bytes(b: &mut BumpBytes, size: usize) {
        let len = b.len();

        if size > len {
            b.reserve(size - len);
            b.resize(size, 0);
        }
    }

    #[inline]
    pub fn memcomparable_encode(&self, b: &mut BumpBytes) -> Result<(), DatabaseError> {
        match self {
            DataValue::Int8(Some(v)) => encode_u!(b, *v as u8 ^ 0x80_u8),
            DataValue::Int16(Some(v)) => encode_u!(b, *v as u16 ^ 0x8000_u16),
            DataValue::Int32(Some(v)) | DataValue::Date32(Some(v)) => {
                encode_u!(b, *v as u32 ^ 0x80000000_u32)
            }
            DataValue::Int64(Some(v)) | DataValue::Date64(Some(v)) => {
                encode_u!(b, *v as u64 ^ 0x8000000000000000_u64)
            }
            DataValue::UInt8(Some(v)) => encode_u!(b, v),
            DataValue::UInt16(Some(v)) => encode_u!(b, v),
            DataValue::UInt32(Some(v)) | DataValue::Time(Some(v)) => encode_u!(b, v),
            DataValue::UInt64(Some(v)) => encode_u!(b, v),
            DataValue::Utf8 { value: Some(v), .. } => Self::encode_bytes(b, v.as_bytes()),
            DataValue::Boolean(Some(v)) => b.push(if *v { b'1' } else { b'0' }),
            DataValue::Float32(Some(f)) => {
                let mut u = f.to_bits();

                if *f >= 0_f32 {
                    u |= 0x80000000_u32;
                } else {
                    u = !u;
                }

                encode_u!(b, u);
            }
            DataValue::Float64(Some(f)) => {
                let mut u = f.to_bits();

                if *f >= 0_f64 {
                    u |= 0x8000000000000000_u64;
                } else {
                    u = !u;
                }

                encode_u!(b, u);
            }
            DataValue::Null => (),
            DataValue::Decimal(Some(v)) => Self::serialize_decimal(*v, b)?,
            DataValue::Tuple(Some((values, is_upper))) => {
                let last = values.len() - 1;

                for (i, v) in values.iter().enumerate() {
                    v.memcomparable_encode(b)?;
                    if (v.is_null() || i == last) && *is_upper {
                        b.push(BOUND_MAX_TAG);
                    } else {
                        b.push(BOUND_MIN_TAG);
                    }
                }
            }
            value => {
                if !value.is_null() {
                    return Err(DatabaseError::InvalidType);
                }
            }
        }

        Ok(())
    }

    // https://github.com/risingwavelabs/memcomparable/blob/main/src/ser.rs#L468
    pub fn serialize_decimal(decimal: Decimal, bytes: &mut BumpBytes) -> Result<(), DatabaseError> {
        if decimal.is_zero() {
            bytes.push(0x15);
            return Ok(());
        }
        let (exponent, significand) = Self::decimal_e_m(decimal);
        if decimal.is_sign_positive() {
            match exponent {
                11.. => {
                    bytes.push(0x22);
                    bytes.push(exponent as u8);
                }
                0..=10 => {
                    bytes.push(0x17 + exponent as u8);
                }
                _ => {
                    bytes.push(0x16);
                    bytes.push(!(-exponent) as u8);
                }
            }
            bytes.extend_from_slice(&significand)
        } else {
            match exponent {
                11.. => {
                    bytes.push(0x8);
                    bytes.push(!exponent as u8);
                }
                0..=10 => {
                    bytes.push(0x13 - exponent as u8);
                }
                _ => {
                    bytes.push(0x14);
                    bytes.push(-exponent as u8);
                }
            }
            for b in significand {
                bytes.push(!b);
            }
        }
        Ok(())
    }

    fn decimal_e_m(decimal: Decimal) -> (i8, Vec<u8>) {
        if decimal.is_zero() {
            return (0, vec![]);
        }
        const POW10: [u128; 30] = [
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000,
            10000000000,
            100000000000,
            1000000000000,
            10000000000000,
            100000000000000,
            1000000000000000,
            10000000000000000,
            100000000000000000,
            1000000000000000000,
            10000000000000000000,
            100000000000000000000,
            1000000000000000000000,
            10000000000000000000000,
            100000000000000000000000,
            1000000000000000000000000,
            10000000000000000000000000,
            100000000000000000000000000,
            1000000000000000000000000000,
            10000000000000000000000000000,
            100000000000000000000000000000,
        ];
        let mut mantissa = decimal.mantissa().unsigned_abs();
        let prec = POW10.as_slice().partition_point(|&p| p <= mantissa);

        let e10 = prec as i32 - decimal.scale() as i32;
        let e100 = if e10 >= 0 { (e10 + 1) / 2 } else { e10 / 2 };
        // Maybe need to add a zero at the beginning.
        // e.g. 111.11 -> 2(exponent which is 100 based) + 0.011111(mantissa).
        // So, the `digit_num` of 111.11 will be 6.
        let mut digit_num = if e10 == 2 * e100 { prec } else { prec + 1 };

        let mut byte_array = Vec::with_capacity(16);
        // Remove trailing zero.
        while mantissa % 10 == 0 && mantissa != 0 {
            mantissa /= 10;
            digit_num -= 1;
        }

        // Cases like: 0.12345, not 0.01111.
        if digit_num % 2 == 1 {
            mantissa *= 10;
            // digit_num += 1;
        }
        while mantissa >> 64 != 0 {
            let byte = (mantissa % 100) as u8 * 2 + 1;
            byte_array.push(byte);
            mantissa /= 100;
        }
        // optimize for division
        let mut mantissa = mantissa as u64;
        while mantissa != 0 {
            let byte = (mantissa % 100) as u8 * 2 + 1;
            byte_array.push(byte);
            mantissa /= 100;
        }
        byte_array[0] -= 1;
        byte_array.reverse();

        (e100 as i8, byte_array)
    }

    #[inline]
    pub fn is_true(&self) -> Result<bool, DatabaseError> {
        if self.is_null() {
            return Ok(false);
        }
        if let DataValue::Boolean(option) = self {
            Ok(matches!(option, Some(true)))
        } else {
            Err(DatabaseError::InvalidType)
        }
    }

    pub fn cast(self, to: &LogicalType) -> Result<DataValue, DatabaseError> {
        let value = match self {
            DataValue::Null => match to {
                LogicalType::Invalid => Err(DatabaseError::CastFail),
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
                LogicalType::Char(len, unit) => Ok(DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Fixed(*len),
                    unit: *unit,
                }),
                LogicalType::Varchar(len, unit) => Ok(DataValue::Utf8 {
                    value: None,
                    ty: Utf8Type::Variable(*len),
                    unit: *unit,
                }),
                LogicalType::Date => Ok(DataValue::Date32(None)),
                LogicalType::DateTime => Ok(DataValue::Date64(None)),
                LogicalType::Time => Ok(DataValue::Time(None)),
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(None)),
                LogicalType::Tuple(_) => Ok(DataValue::Tuple(None)),
            },
            DataValue::Boolean(value) => match to {
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
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Float32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(value)),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(
                    value
                        .map(|v| {
                            let mut decimal =
                                Decimal::from_f32(v).ok_or(DatabaseError::CastFail)?;
                            Self::decimal_round_f(option, &mut decimal);

                            Ok::<Decimal, DatabaseError>(decimal)
                        })
                        .transpose()?,
                )),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Float64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value)),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(
                    value
                        .map(|v| {
                            let mut decimal =
                                Decimal::from_f64(v).ok_or(DatabaseError::CastFail)?;
                            Self::decimal_round_f(option, &mut decimal);

                            Ok::<Decimal, DatabaseError>(decimal)
                        })
                        .transpose()?,
                )),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Int8(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::UInteger => {
                    Ok(DataValue::UInt32(value.map(u32::try_from).transpose()?))
                }
                LogicalType::UBigint => {
                    Ok(DataValue::UInt64(value.map(u64::try_from).transpose()?))
                }
                LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Int16(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::UInteger => {
                    Ok(DataValue::UInt32(value.map(u32::try_from).transpose()?))
                }
                LogicalType::UBigint => {
                    Ok(DataValue::UInt64(value.map(u64::try_from).transpose()?))
                }
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::Smallint => Ok(DataValue::Int16(value)),
                LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Int32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::UInteger => {
                    Ok(DataValue::UInt32(value.map(u32::try_from).transpose()?))
                }
                LogicalType::UBigint => {
                    Ok(DataValue::UInt64(value.map(u64::try_from).transpose()?))
                }
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::Smallint => {
                    Ok(DataValue::Int16(value.map(i16::try_from).transpose()?))
                }
                LogicalType::Integer => Ok(DataValue::Int32(value)),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Int64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::UInteger => {
                    Ok(DataValue::UInt32(value.map(u32::try_from).transpose()?))
                }
                LogicalType::UBigint => {
                    Ok(DataValue::UInt64(value.map(u64::try_from).transpose()?))
                }
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::Smallint => {
                    Ok(DataValue::Int16(value.map(i16::try_from).transpose()?))
                }
                LogicalType::Integer => Ok(DataValue::Int32(value.map(i32::try_from).transpose()?)),
                LogicalType::Bigint => Ok(DataValue::Int64(value)),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v as f64))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::UInt8(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value)),
                LogicalType::Smallint => Ok(DataValue::Int16(value.map(|v| v.into()))),
                LogicalType::USmallint => Ok(DataValue::UInt16(value.map(|v| v.into()))),
                LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::UInt16(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::Smallint => {
                    Ok(DataValue::Int16(value.map(i16::try_from).transpose()?))
                }
                LogicalType::USmallint => Ok(DataValue::UInt16(value)),
                LogicalType::Integer => Ok(DataValue::Int32(value.map(|v| v.into()))),
                LogicalType::UInteger => Ok(DataValue::UInt32(value.map(|v| v.into()))),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v.into()))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::UInt32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::Smallint => {
                    Ok(DataValue::Int16(value.map(i16::try_from).transpose()?))
                }
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::Integer => Ok(DataValue::Int32(value.map(i32::try_from).transpose()?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(value)),
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(|v| v.into()))),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.map(|v| v.into()))),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::UInt64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value.map(i8::try_from).transpose()?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.map(u8::try_from).transpose()?)),
                LogicalType::Smallint => {
                    Ok(DataValue::Int16(value.map(i16::try_from).transpose()?))
                }
                LogicalType::USmallint => {
                    Ok(DataValue::UInt16(value.map(u16::try_from).transpose()?))
                }
                LogicalType::Integer => Ok(DataValue::Int32(value.map(i32::try_from).transpose()?)),
                LogicalType::UInteger => {
                    Ok(DataValue::UInt32(value.map(u32::try_from).transpose()?))
                }
                LogicalType::Bigint => Ok(DataValue::Int64(value.map(i64::try_from).transpose()?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(value)),
                LogicalType::Float => Ok(DataValue::Float32(value.map(|v| v as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value.map(|v| v as f64))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => Ok(DataValue::Decimal(value.map(|v| {
                    let mut decimal = Decimal::from(v);
                    Self::decimal_round_i(option, &mut decimal);

                    decimal
                }))),
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Utf8 { value, .. } => match to {
                LogicalType::Invalid => Err(DatabaseError::CastFail),
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Boolean => Ok(DataValue::Boolean(
                    value.map(|v| bool::from_str(&v)).transpose()?,
                )),
                LogicalType::Tinyint => Ok(DataValue::Int8(
                    value.map(|v| i8::from_str(&v)).transpose()?,
                )),
                LogicalType::UTinyint => Ok(DataValue::UInt8(
                    value.map(|v| u8::from_str(&v)).transpose()?,
                )),
                LogicalType::Smallint => Ok(DataValue::Int16(
                    value.map(|v| i16::from_str(&v)).transpose()?,
                )),
                LogicalType::USmallint => Ok(DataValue::UInt16(
                    value.map(|v| u16::from_str(&v)).transpose()?,
                )),
                LogicalType::Integer => Ok(DataValue::Int32(
                    value.map(|v| i32::from_str(&v)).transpose()?,
                )),
                LogicalType::UInteger => Ok(DataValue::UInt32(
                    value.map(|v| u32::from_str(&v)).transpose()?,
                )),
                LogicalType::Bigint => Ok(DataValue::Int64(
                    value.map(|v| i64::from_str(&v)).transpose()?,
                )),
                LogicalType::UBigint => Ok(DataValue::UInt64(
                    value.map(|v| u64::from_str(&v)).transpose()?,
                )),
                LogicalType::Float => Ok(DataValue::Float32(
                    value.map(|v| f32::from_str(&v)).transpose()?,
                )),
                LogicalType::Double => Ok(DataValue::Float64(
                    value.map(|v| f64::from_str(&v)).transpose()?,
                )),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Date => {
                    let option = value
                        .map(|v| {
                            NaiveDate::parse_from_str(&v, DATE_FMT)
                                .map(|date| date.num_days_from_ce())
                        })
                        .transpose()?;

                    Ok(DataValue::Date32(option))
                }
                LogicalType::DateTime => {
                    let option = value
                        .map(|v| {
                            NaiveDateTime::parse_from_str(&v, DATE_TIME_FMT)
                                .or_else(|_| {
                                    NaiveDate::parse_from_str(&v, DATE_FMT)
                                        .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                                })
                                .map(|date_time| date_time.and_utc().timestamp())
                        })
                        .transpose()?;

                    Ok(DataValue::Date64(option))
                }
                LogicalType::Time => {
                    let option = value
                        .map(|v| {
                            NaiveTime::parse_from_str(&v, TIME_FMT)
                                .map(|time| time.num_seconds_from_midnight())
                        })
                        .transpose()?;

                    Ok(DataValue::Time(option))
                }
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(
                    value.map(|v| Decimal::from_str(&v)).transpose()?,
                )),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Date32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value),
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value),
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => Ok(DataValue::Date32(value)),
                LogicalType::DateTime => {
                    let option = value.and_then(|v| {
                        NaiveDate::from_num_days_from_ce_opt(v)
                            .and_then(|date| date.and_hms_opt(0, 0, 0))
                            .map(|date_time| date_time.and_utc().timestamp())
                    });

                    Ok(DataValue::Date64(option))
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Date64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value),
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value),
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => {
                    let option = value.and_then(|v| {
                        DateTime::from_timestamp(v, 0)
                            .map(|dt| dt.naive_utc())
                            .map(|date_time| date_time.date().num_days_from_ce())
                    });

                    Ok(DataValue::Date32(option))
                }
                LogicalType::DateTime => Ok(DataValue::Date64(value)),
                LogicalType::Time => {
                    let option = value.and_then(|v| {
                        DateTime::from_timestamp(v, 0)
                            .map(|date_time| date_time.time().num_seconds_from_midnight())
                    });

                    Ok(DataValue::Time(option))
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Time(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value),
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value),
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Decimal(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(value.and_then(|v| v.to_f32()))),
                LogicalType::Double => Ok(DataValue::Float64(value.and_then(|v| v.to_f64()))),
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(value)),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Tuple(values) => match to {
                LogicalType::Tuple(types) => Ok(if let Some((mut values, is_upper)) = values {
                    for (i, value) in values.iter_mut().enumerate() {
                        if types[i] != value.logical_type() {
                            *value = mem::replace(value, DataValue::Null).cast(&types[i])?;
                        }
                    }
                    DataValue::Tuple(Some((values, is_upper)))
                } else {
                    DataValue::Tuple(None)
                }),
                _ => Err(DatabaseError::CastFail),
            },
        }?;
        value.check_len(to)?;
        Ok(value)
    }

    #[inline]
    pub fn common_prefix_length(&self, target: &DataValue) -> Option<usize> {
        if self.is_null() && target.is_null() {
            return Some(0);
        }
        if self.is_null() || target.is_null() {
            return None;
        }

        if let (
            DataValue::Utf8 {
                value: Some(v1), ..
            },
            DataValue::Utf8 {
                value: Some(v2), ..
            },
        ) = (self, target)
        {
            let min_len = cmp::min(v1.len(), v2.len());

            let mut v1_iter = v1.get(0..min_len).unwrap().chars();
            let mut v2_iter = v2.get(0..min_len).unwrap().chars();

            for i in 0..min_len {
                if v1_iter.next() != v2_iter.next() {
                    return Some(i);
                }
            }

            return Some(min_len);
        }
        Some(0)
    }

    #[inline]
    pub(crate) fn values_to_tuple(mut values: Vec<DataValue>) -> Option<DataValue> {
        if values.len() > 1 {
            Some(DataValue::Tuple(Some((values, false))))
        } else {
            values.pop()
        }
    }

    fn decimal_round_i(option: &Option<u8>, decimal: &mut Decimal) {
        if let Some(scale) = option {
            let new_decimal = decimal.trunc_with_scale(*scale as u32);
            let _ = mem::replace(decimal, new_decimal);
        }
    }

    fn decimal_round_f(option: &Option<u8>, decimal: &mut Decimal) {
        if let Some(scale) = option {
            let new_decimal = decimal.round_dp_with_strategy(
                *scale as u32,
                rust_decimal::RoundingStrategy::MidpointAwayFromZero,
            );
            let _ = mem::replace(decimal, new_decimal);
        }
    }

    fn date_format<'a>(v: i32) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        NaiveDate::from_num_days_from_ce_opt(v).map(|date| date.format(DATE_FMT))
    }

    fn date_time_format<'a>(v: i64) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        DateTime::from_timestamp(v, 0).map(|date_time| date_time.format(DATE_TIME_FMT))
    }

    fn time_format<'a>(v: u32) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        NaiveTime::from_num_seconds_from_midnight_opt(v, 0).map(|time| time.format(TIME_FMT))
    }

    fn decimal_format(v: &Decimal) -> String {
        v.to_string()
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
impl_scalar!(Decimal, Decimal);

impl From<String> for DataValue {
    fn from(value: String) -> Self {
        DataValue::Utf8 {
            value: Some(value),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }
}

impl From<Option<String>> for DataValue {
    fn from(value: Option<String>) -> Self {
        DataValue::Utf8 {
            value,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }
}

impl From<&NaiveDate> for DataValue {
    fn from(value: &NaiveDate) -> Self {
        DataValue::Date32(Some(value.num_days_from_ce()))
    }
}

impl From<Option<&NaiveDate>> for DataValue {
    fn from(value: Option<&NaiveDate>) -> Self {
        DataValue::Date32(value.map(|d| d.num_days_from_ce()))
    }
}

impl From<&NaiveDateTime> for DataValue {
    fn from(value: &NaiveDateTime) -> Self {
        DataValue::Date64(Some(value.and_utc().timestamp()))
    }
}

impl From<Option<&NaiveDateTime>> for DataValue {
    fn from(value: Option<&NaiveDateTime>) -> Self {
        DataValue::Date64(value.map(|d| d.and_utc().timestamp()))
    }
}

impl From<&NaiveTime> for DataValue {
    fn from(value: &NaiveTime) -> Self {
        DataValue::Time(Some(value.num_seconds_from_midnight()))
    }
}

impl From<Option<&NaiveTime>> for DataValue {
    fn from(value: Option<&NaiveTime>) -> Self {
        DataValue::Time(value.map(|d| d.num_seconds_from_midnight()))
    }
}

impl TryFrom<&sqlparser::ast::Value> for DataValue {
    type Error = DatabaseError;

    fn try_from(value: &sqlparser::ast::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            sqlparser::ast::Value::Number(n, _) => {
                // use i32 to handle most cases
                if let Ok(v) = n.parse::<i32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<i64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f32>() {
                    v.into()
                } else {
                    return Err(DatabaseError::InvalidValue(n.to_string()));
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::Boolean(b) => (*b).into(),
            sqlparser::ast::Value::Null => Self::Null,
            v => return Err(DatabaseError::UnsupportedStmt(format!("{:?}", v))),
        })
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "null"),
        }
    };
}
macro_rules! format_float_option {
    ($F:expr, $EXPR:expr) => {
        match $EXPR {
            Some(e) => {
                let formatted_string = format!("{:?}", e);
                let formatted_result = if let Some(i) = formatted_string.find('.') {
                    format!("{:.1$}", e, formatted_string.len() - i - 1)
                } else {
                    format!("{:.1}", e)
                };

                write!($F, "{}", formatted_result)
            }
            None => write!($F, "null"),
        }
    };
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataValue::Boolean(e) => format_option!(f, e)?,
            DataValue::Float32(e) => format_float_option!(f, e)?,
            DataValue::Float64(e) => format_float_option!(f, e)?,
            DataValue::Int8(e) => format_option!(f, e)?,
            DataValue::Int16(e) => format_option!(f, e)?,
            DataValue::Int32(e) => format_option!(f, e)?,
            DataValue::Int64(e) => format_option!(f, e)?,
            DataValue::UInt8(e) => format_option!(f, e)?,
            DataValue::UInt16(e) => format_option!(f, e)?,
            DataValue::UInt32(e) => format_option!(f, e)?,
            DataValue::UInt64(e) => format_option!(f, e)?,
            DataValue::Utf8 { value: e, .. } => format_option!(f, e)?,
            DataValue::Null => write!(f, "null")?,
            DataValue::Date32(e) => format_option!(f, e.and_then(DataValue::date_format))?,
            DataValue::Date64(e) => format_option!(f, e.and_then(DataValue::date_time_format))?,
            DataValue::Time(e) => format_option!(f, e.and_then(DataValue::time_format))?,
            DataValue::Decimal(e) => format_option!(f, e.as_ref().map(DataValue::decimal_format))?,
            DataValue::Tuple(e) => {
                write!(f, "(")?;
                if let Some((values, _)) = e {
                    let len = values.len();

                    for (i, value) in values.iter().enumerate() {
                        value.fmt(f)?;
                        if len != i + 1 {
                            write!(f, ", ")?;
                        }
                    }
                }
                write!(f, ")")?;
            }
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
            DataValue::Utf8 { value: None, .. } => write!(f, "Utf8({})", self),
            DataValue::Utf8 { value: Some(_), .. } => write!(f, "Utf8(\"{}\")", self),
            DataValue::Null => write!(f, "null"),
            DataValue::Date32(_) => write!(f, "Date32({})", self),
            DataValue::Date64(_) => write!(f, "Date64({})", self),
            DataValue::Time(_) => write!(f, "Time({})", self),
            DataValue::Decimal(_) => write!(f, "Decimal({})", self),
            DataValue::Tuple(_) => {
                write!(f, "Tuple({}", self)?;
                if matches!(self, DataValue::Tuple(Some((_, true)))) {
                    write!(f, " [is upper]")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::errors::DatabaseError;
    use crate::storage::table_codec::BumpBytes;
    use crate::types::value::DataValue;
    use bumpalo::Bump;
    use rust_decimal::Decimal;

    #[test]
    fn test_mem_comparable_int() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_i8_1 = BumpBytes::new_in(&arena);
        let mut key_i8_2 = BumpBytes::new_in(&arena);
        let mut key_i8_3 = BumpBytes::new_in(&arena);

        DataValue::Int8(Some(i8::MIN)).memcomparable_encode(&mut key_i8_1)?;
        DataValue::Int8(Some(-1_i8)).memcomparable_encode(&mut key_i8_2)?;
        DataValue::Int8(Some(i8::MAX)).memcomparable_encode(&mut key_i8_3)?;

        println!("{:?} < {:?}", key_i8_1, key_i8_2);
        println!("{:?} < {:?}", key_i8_2, key_i8_3);
        assert!(key_i8_1 < key_i8_2);
        assert!(key_i8_2 < key_i8_3);

        let mut key_i16_1 = BumpBytes::new_in(&arena);
        let mut key_i16_2 = BumpBytes::new_in(&arena);
        let mut key_i16_3 = BumpBytes::new_in(&arena);

        DataValue::Int16(Some(i16::MIN)).memcomparable_encode(&mut key_i16_1)?;
        DataValue::Int16(Some(-1_i16)).memcomparable_encode(&mut key_i16_2)?;
        DataValue::Int16(Some(i16::MAX)).memcomparable_encode(&mut key_i16_3)?;

        println!("{:?} < {:?}", key_i16_1, key_i16_2);
        println!("{:?} < {:?}", key_i16_2, key_i16_3);
        assert!(key_i16_1 < key_i16_2);
        assert!(key_i16_2 < key_i16_3);

        let mut key_i32_1 = BumpBytes::new_in(&arena);
        let mut key_i32_2 = BumpBytes::new_in(&arena);
        let mut key_i32_3 = BumpBytes::new_in(&arena);

        DataValue::Int32(Some(i32::MIN)).memcomparable_encode(&mut key_i32_1)?;
        DataValue::Int32(Some(-1_i32)).memcomparable_encode(&mut key_i32_2)?;
        DataValue::Int32(Some(i32::MAX)).memcomparable_encode(&mut key_i32_3)?;

        println!("{:?} < {:?}", key_i32_1, key_i32_2);
        println!("{:?} < {:?}", key_i32_2, key_i32_3);
        assert!(key_i32_1 < key_i32_2);
        assert!(key_i32_2 < key_i32_3);

        let mut key_i64_1 = BumpBytes::new_in(&arena);
        let mut key_i64_2 = BumpBytes::new_in(&arena);
        let mut key_i64_3 = BumpBytes::new_in(&arena);

        DataValue::Int64(Some(i64::MIN)).memcomparable_encode(&mut key_i64_1)?;
        DataValue::Int64(Some(-1_i64)).memcomparable_encode(&mut key_i64_2)?;
        DataValue::Int64(Some(i64::MAX)).memcomparable_encode(&mut key_i64_3)?;

        println!("{:?} < {:?}", key_i64_1, key_i64_2);
        println!("{:?} < {:?}", key_i64_2, key_i64_3);
        assert!(key_i64_1 < key_i64_2);
        assert!(key_i64_2 < key_i64_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_float() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_f32_1 = BumpBytes::new_in(&arena);
        let mut key_f32_2 = BumpBytes::new_in(&arena);
        let mut key_f32_3 = BumpBytes::new_in(&arena);

        DataValue::Float32(Some(f32::MIN)).memcomparable_encode(&mut key_f32_1)?;
        DataValue::Float32(Some(-1_f32)).memcomparable_encode(&mut key_f32_2)?;
        DataValue::Float32(Some(f32::MAX)).memcomparable_encode(&mut key_f32_3)?;

        println!("{:?} < {:?}", key_f32_1, key_f32_2);
        println!("{:?} < {:?}", key_f32_2, key_f32_3);
        assert!(key_f32_1 < key_f32_2);
        assert!(key_f32_2 < key_f32_3);

        let mut key_f64_1 = BumpBytes::new_in(&arena);
        let mut key_f64_2 = BumpBytes::new_in(&arena);
        let mut key_f64_3 = BumpBytes::new_in(&arena);

        DataValue::Float64(Some(f64::MIN)).memcomparable_encode(&mut key_f64_1)?;
        DataValue::Float64(Some(-1_f64)).memcomparable_encode(&mut key_f64_2)?;
        DataValue::Float64(Some(f64::MAX)).memcomparable_encode(&mut key_f64_3)?;

        println!("{:?} < {:?}", key_f64_1, key_f64_2);
        println!("{:?} < {:?}", key_f64_2, key_f64_3);
        assert!(key_f64_1 < key_f64_2);
        assert!(key_f64_2 < key_f64_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_decimal() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_deciaml_1 = BumpBytes::new_in(&arena);
        let mut key_deciaml_2 = BumpBytes::new_in(&arena);
        let mut key_deciaml_3 = BumpBytes::new_in(&arena);

        DataValue::Decimal(Some(Decimal::MIN)).memcomparable_encode(&mut key_deciaml_1)?;
        DataValue::Decimal(Some(Decimal::new(-1, 0))).memcomparable_encode(&mut key_deciaml_2)?;
        DataValue::Decimal(Some(Decimal::MAX)).memcomparable_encode(&mut key_deciaml_3)?;

        println!("{:?} < {:?}", key_deciaml_1, key_deciaml_2);
        println!("{:?} < {:?}", key_deciaml_2, key_deciaml_3);
        assert!(key_deciaml_1 < key_deciaml_2);
        assert!(key_deciaml_2 < key_deciaml_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_lower() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(None),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(1)),
            ],
            false,
        )))
        .memcomparable_encode(&mut key_tuple_1)?;
        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(1)),
            ],
            false,
        )))
        .memcomparable_encode(&mut key_tuple_2)?;
        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(2)),
            ],
            false,
        )))
        .memcomparable_encode(&mut key_tuple_3)?;

        println!("{:?} < {:?}", key_tuple_1, key_tuple_2);
        println!("{:?} < {:?}", key_tuple_2, key_tuple_3);
        assert!(key_tuple_1 < key_tuple_2);
        assert!(key_tuple_2 < key_tuple_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_upper() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(None),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(1)),
            ],
            true,
        )))
        .memcomparable_encode(&mut key_tuple_1)?;
        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(1)),
            ],
            true,
        )))
        .memcomparable_encode(&mut key_tuple_2)?;
        DataValue::Tuple(Some((
            vec![
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(0)),
                DataValue::Int8(Some(2)),
            ],
            true,
        )))
        .memcomparable_encode(&mut key_tuple_3)?;

        println!("{:?} < {:?}", key_tuple_2, key_tuple_3);
        println!("{:?} < {:?}", key_tuple_3, key_tuple_1);
        assert!(key_tuple_2 < key_tuple_3);
        assert!(key_tuple_3 < key_tuple_1);

        Ok(())
    }
}
