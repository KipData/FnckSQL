use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use ordered_float::OrderedFloat;

use super::LogicalType;

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
        }
    }
}

impl DataValue {
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
            LogicalType::Varchar => DataValue::Utf8(None)
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
        }
    }
    
    pub fn cast(self, to: &LogicalType) -> DataValue {
        match self {
            DataValue::Null => {
                match to {
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
                }
            }
            DataValue::Boolean(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Boolean => DataValue::Boolean(value),
                    LogicalType::Tinyint => DataValue::Int8(value.map(|v| v.into())),
                    LogicalType::UTinyint => DataValue::UInt8(value.map(|v| v.into())),
                    LogicalType::Smallint => DataValue::Int16(value.map(|v| v.into())),
                    LogicalType::USmallint => DataValue::UInt16(value.map(|v| v.into())),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| v.into())),
                    LogicalType::UInteger => DataValue::UInt32(value.map(|v| v.into())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::UBigint => DataValue::UInt64(value.map(|v| v.into())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                }
            }
            DataValue::Float32(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Float => DataValue::Float32(value),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Float64(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Double => DataValue::Float64(value),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Int8(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Tinyint => DataValue::Int8(value),
                    LogicalType::Smallint => DataValue::Int16(value.map(|v| v.into())),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| v.into())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Int16(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Smallint => DataValue::Int16(value),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| v.into())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Int32(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Integer => DataValue::Int32(value),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Int64(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Bigint => DataValue::Int64(value),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::UInt8(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::UTinyint => DataValue::UInt8(value),
                    LogicalType::Smallint => DataValue::Int16(value.map(|v| v.into())),
                    LogicalType::USmallint => DataValue::UInt16(value.map(|v| v.into())),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| v.into())),
                    LogicalType::UInteger => DataValue::UInt32(value.map(|v| v.into())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::UBigint => DataValue::UInt64(value.map(|v| v.into())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::UInt16(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::USmallint => DataValue::UInt16(value),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| v.into())),
                    LogicalType::UInteger => DataValue::UInt32(value.map(|v| v.into())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::UBigint => DataValue::UInt64(value.map(|v| v.into())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                   _ => panic!("not support"),
                }
            }
            DataValue::UInt32(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::UInteger => DataValue::UInt32(value),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| v.into())),
                    LogicalType::UBigint => DataValue::UInt64(value.map(|v| v.into())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| v.into())),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::UInt64(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::UBigint => DataValue::UInt64(value),
                    LogicalType::Varchar => DataValue::Utf8(value.map(|v| format!("{}", v))),
                    _ => panic!("not support"),
                }
            }
            DataValue::Utf8(value) => {
                match to {
                    LogicalType::Invalid => panic!("invalid logical type"),
                    LogicalType::SqlNull => DataValue::Null,
                    LogicalType::Boolean => DataValue::Boolean(value.map(|v| bool::from_str(&v).unwrap())),
                    LogicalType::Tinyint => DataValue::Int8(value.map(|v| i8::from_str(&v).unwrap())),
                    LogicalType::UTinyint => DataValue::UInt8(value.map(|v| u8::from_str(&v).unwrap())),
                    LogicalType::Smallint => DataValue::Int16(value.map(|v| i16::from_str(&v).unwrap())),
                    LogicalType::USmallint => DataValue::UInt16(value.map(|v| u16::from_str(&v).unwrap())),
                    LogicalType::Integer => DataValue::Int32(value.map(|v| i32::from_str(&v).unwrap())),
                    LogicalType::UInteger => DataValue::UInt32(value.map(|v| u32::from_str(&v).unwrap())),
                    LogicalType::Bigint => DataValue::Int64(value.map(|v| i64::from_str(&v).unwrap())),
                    LogicalType::UBigint => DataValue::UInt64(value.map(|v| u64::from_str(&v).unwrap())),
                    LogicalType::Float => DataValue::Float32(value.map(|v| f32::from_str(&v).unwrap())),
                    LogicalType::Double => DataValue::Float64(value.map(|v| f64::from_str(&v).unwrap())),
                    LogicalType::Varchar => DataValue::Utf8(value),
                }
            }
        }
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
        };
        Ok(())
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
        }
    }
}
