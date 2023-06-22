pub mod value;

use snowflake::ProcessUniqueId;
pub use sqlparser::ast::DataType as DataTypeKind;
/// Inner data type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    #[inline]
    pub const fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        DataType { kind, nullable }
    }
    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    #[inline]
    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }
}

pub trait DataTypeExt {
    fn nullable(self) -> DataType;
    fn not_null(self) -> DataType;
}

impl DataTypeExt for DataTypeKind {
    #[inline]
    fn nullable(self) -> DataType {
        DataType::new(self, true)
    }
    #[inline]
    fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}

// The ID generated using the snowflake algorithm is used to ensure the sequence
// and avoid the ID being regenerated from 0 after the restart, resulting in duplication
pub type TableId = ProcessUniqueId;
pub type ColumnId = ProcessUniqueId;
