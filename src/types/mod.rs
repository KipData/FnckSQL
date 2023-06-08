use std::any::Any;
use std::sync::Arc;

/// PostgresSQL DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum PgSQLDataTypeEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

/// MySQL DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum MySQLDataTypeEnum {
    Integer,
    Boolean,
    Double,
    Char,
}

/// Inner data type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum DataTypeEnum {
    Int32,
    Bool,
    Float64,
    Char,
}

/// Trait for all data types
pub(crate) trait DataType {
    fn is_nullable(&self) -> bool;
    fn get_type(&self) -> DataTypeEnum;
    fn get_data_len(&self) -> u32;
    fn as_any(&self) -> &dyn Any;
}

/// Type alias for DataTypeRef
pub(crate) type DataTypeRef = Arc<dyn DataType>;
pub(crate) type DatabaseIdT = u32;
pub(crate) type SchemaIdT = u32;
pub(crate) type TableIdT = u32;
pub(crate) type ColumnIdT = u32;

/// Import all data types
mod bool_type;
mod numeric_types;
/// Export all data types
pub(crate) use bool_type::*;
pub(crate) use numeric_types::*;
