use crate::types::{ColumnId, DataType};

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

/// Column catalog
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(
        column_id: ColumnId,
        column_name: String,
        column_desc: ColumnDesc,
    ) -> ColumnCatalog {
        ColumnCatalog {
            id: column_id,
            name: column_name,
            desc: column_desc,
        }
    }

    pub(crate) fn id(&self) -> ColumnId {
        self.id
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub(crate) fn datatype(&self) -> DataType {
        self.desc.column_datatype.clone()
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_column_catalog() {
        let mut col_catalog = ColumnCatalog::new(
            0,
            "test".to_string(),
            DataTypeKind::Int(None).not_null().to_column(),
        );

        assert_eq!(col_catalog.id(), 0);
        assert_eq!(col_catalog.is_primary(), false);
        assert_eq!(col_catalog.is_nullable(), false);
        assert_eq!(col_catalog.name(), "test");
    }
}
