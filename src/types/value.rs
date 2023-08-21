use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::repeat;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder,
    Float32Array, Float32Builder, Float64Array, Float64Builder, Int16Array,
    Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder,
    StringArray, StringBuilder, UInt16Array,
    UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array,
    UInt8Builder,
};
use arrow::datatypes::DataType;
use ordered_float::OrderedFloat;

use super::{LogicalType, TypeError};

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

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        DataValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE($ENUM), $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ENUM2:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => {
                let array: ArrayRef = Arc::new($ARRAY_TYPE::from_value(*value, $SIZE));
                // Need to call cast to cast to final data type with timezone/extra param
                cast(&array, &DataType::$DATA_TYPE($ENUM, $ENUM2)).expect("cannot do temporal cast")
            }
            None => new_null_array(&DataType::$DATA_TYPE($ENUM, $ENUM2), $SIZE),
        }
    }};
}

impl DataValue {
    pub fn new_none_value(data_type: &DataType) -> Result<Self, TypeError> {
        match data_type {
            DataType::Null => Ok(DataValue::Null),
            DataType::Boolean => Ok(DataValue::Boolean(None)),
            DataType::Float32 => Ok(DataValue::Float32(None)),
            DataType::Float64 => Ok(DataValue::Float64(None)),
            DataType::Int8 => Ok(DataValue::Int8(None)),
            DataType::Int16 => Ok(DataValue::Int16(None)),
            DataType::Int32 => Ok(DataValue::Int32(None)),
            DataType::Int64 => Ok(DataValue::Int64(None)),
            DataType::UInt8 => Ok(DataValue::UInt8(None)),
            DataType::UInt16 => Ok(DataValue::UInt16(None)),
            DataType::UInt32 => Ok(DataValue::UInt32(None)),
            DataType::UInt64 => Ok(DataValue::UInt64(None)),
            DataType::Utf8 => Ok(DataValue::Utf8(None)),
            other => Err(TypeError::NotImplementedArrowDataType(other.to_string())),
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

    /// Converts a value in `array` at `index` into a DataValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self, TypeError> {
        if !array.is_valid(index) {
            return Self::new_none_value(array.data_type());
        }

        use arrow::array::*;

        Ok(match array.data_type() {
            DataType::Null => DataValue::Null,
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            other => {
                return Err(TypeError::NotImplementedArrowDataType(other.to_string()));
            }
        })
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

    /// Converts a scalar value into an 1-row array.
    pub fn to_array(&self) -> ArrayRef {
        self.to_array_of_size(1)
    }

    pub fn bool_array(size: usize, e: &Option<bool>) -> ArrayRef {
        Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            DataValue::Boolean(e) => Self::bool_array(size, e),
            DataValue::Float64(e) => {
                build_array_from_option!(Float64, Float64Array, e, size)
            }
            DataValue::Float32(e) => {
                build_array_from_option!(Float32, Float32Array, e, size)
            }
            DataValue::Int8(e) => build_array_from_option!(Int8, Int8Array, e, size),
            DataValue::Int16(e) => build_array_from_option!(Int16, Int16Array, e, size),
            DataValue::Int32(e) => build_array_from_option!(Int32, Int32Array, e, size),
            DataValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            DataValue::UInt8(e) => build_array_from_option!(UInt8, UInt8Array, e, size),
            DataValue::UInt16(e) => {
                build_array_from_option!(UInt16, UInt16Array, e, size)
            }
            DataValue::UInt32(e) => {
                build_array_from_option!(UInt32, UInt32Array, e, size)
            }
            DataValue::UInt64(e) => {
                build_array_from_option!(UInt64, UInt64Array, e, size)
            }

            DataValue::Utf8(e) => match e {
                Some(value) => Arc::new(StringArray::from_iter_values(repeat(value).take(size))),
                None => new_null_array(&DataType::Utf8, size),
            },
            DataValue::Null => new_null_array(&DataType::Null, size),
        }
    }

    pub fn new_builder(data_type: &LogicalType) -> Result<Box<dyn ArrayBuilder>, TypeError> {
        match data_type {
            LogicalType::Invalid | LogicalType::SqlNull => Err(TypeError::InternalError(format!(
                "Unsupported type {:?} for builder",
                data_type
            ))),
            LogicalType::Boolean => Ok(Box::new(BooleanBuilder::new())),
            LogicalType::Tinyint => Ok(Box::new(Int8Builder::new())),
            LogicalType::UTinyint => Ok(Box::new(UInt8Builder::new())),
            LogicalType::Smallint => Ok(Box::new(Int16Builder::new())),
            LogicalType::USmallint => Ok(Box::new(UInt16Builder::new())),
            LogicalType::Integer => Ok(Box::new(Int32Builder::new())),
            LogicalType::UInteger => Ok(Box::new(UInt32Builder::new())),
            LogicalType::Bigint => Ok(Box::new(Int64Builder::new())),
            LogicalType::UBigint => Ok(Box::new(UInt64Builder::new())),
            LogicalType::Float => Ok(Box::new(Float32Builder::new())),
            LogicalType::Double => Ok(Box::new(Float64Builder::new())),
            LogicalType::Varchar => Ok(Box::new(StringBuilder::new())),
        }
    }

    pub fn append_for_builder(
        value: &DataValue,
        builder: &mut Box<dyn ArrayBuilder>,
    ) -> Result<(), TypeError> {
        match value {
            DataValue::Null => {
                return Err(TypeError::InternalError(
                    "Unsupported type: Null for builder".to_string(),
                ))
            }
            DataValue::Boolean(v) => builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_option(*v),
            DataValue::Utf8(v) => builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_option(v.as_ref()),
            DataValue::Int8(v) => builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::Int16(v) => builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::Int32(v) => builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::Int64(v) => builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::UInt8(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::UInt16(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::UInt32(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::UInt64(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::Float32(v) => builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap()
                .append_option(*v),
            DataValue::Float64(v) => builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_option(*v),
        }
        Ok(())
    }

    pub fn datatype(&self) -> DataType {
        match self {
            DataValue::Boolean(_) => DataType::Boolean,
            DataValue::UInt8(_) => DataType::UInt8,
            DataValue::UInt16(_) => DataType::UInt16,
            DataValue::UInt32(_) => DataType::UInt32,
            DataValue::UInt64(_) => DataType::UInt64,
            DataValue::Int8(_) => DataType::Int8,
            DataValue::Int16(_) => DataType::Int16,
            DataValue::Int32(_) => DataType::Int32,
            DataValue::Int64(_) => DataType::Int64,
            DataValue::Float32(_) => DataType::Float32,
            DataValue::Float64(_) => DataType::Float64,
            DataValue::Utf8(_) => DataType::Utf8,
            DataValue::Null => DataType::Null,
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
                    _ => panic!("not support"),
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
                    _ => panic!("not support"),
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
                    _ => panic!("not support"),
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
