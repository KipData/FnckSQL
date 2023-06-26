use crate::types::{ColumnId, DataType, IdGenerator};

#[derive(Clone)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub desc: ColumnDesc,
}

impl Column {
    pub(crate) fn new(
        column_name: String,
        column_desc: ColumnDesc,
    ) -> Column {
        Column {
            id: IdGenerator::build(),
            name: column_name,
            desc: column_desc,
        }
    }

    pub(crate) fn datatype(&self) -> &DataType {
        &self.desc.column_datatype
    }
}

impl DataType {
    #[inline]
    pub const fn to_column(self) -> ColumnDesc {
        ColumnDesc::new(self, false)
    }
    #[inline]
    pub const fn to_column_primary_key(self) -> ColumnDesc {
        ColumnDesc::new(self, true)
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    column_datatype: DataType,
    is_primary: bool,
}

impl ColumnDesc {
    pub(crate) const fn new(column_datatype: DataType, is_primary: bool) -> ColumnDesc {
        ColumnDesc {
            column_datatype,
            is_primary,
        }
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.column_datatype.is_nullable()
    }

    pub(crate) fn get_datatype(&self) -> DataType {
        self.column_datatype.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_column_catalog() {
        let mut col_catalog = Column::new(
            "test".to_string(),
            DataTypeKind::Int(None).not_null().to_column(),
        );

        assert_eq!(col_catalog.desc.is_primary(), false);
        assert_eq!(col_catalog.desc.is_nullable(), false);
        assert_eq!(col_catalog.name, "test");
    }
}
