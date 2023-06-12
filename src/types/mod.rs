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

pub(crate) type DatabaseIdT = u32;
pub(crate) type SchemaIdT = u32;
pub(crate) type TableIdT = u32;
pub(crate) type ColumnIdT = u32;
