use crate::types::{ColumnIdT, DataType, DataTypeRef};
use std::sync::Arc;

/// Column description for column
pub(crate) struct ColumnDesc {
    column_datatype: DataTypeRef,
    is_primary: bool,
}

impl ColumnDesc {
    pub(crate) fn new(column_datatype: impl DataType, is_primary: bool) -> ColumnDesc {
        ColumnDesc {
            column_datatype: Arc::new(column_datatype),
            is_primary,
        }
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.column_datatype.is_nullable()
    }

    pub(crate) fn get_datatype(&self) -> DataTypeRef {
        self.column_datatype.clone()
    }
}

/// Column catalog
pub(crate) struct ColumnCatalog {
    column_id: ColumnIdT,
    column_name: String,
    column_desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(
        column_id: ColumnIdT,
        column_name: String,
        column_desc: ColumnDesc,
    ) -> ColumnCatalog {
        ColumnCatalog {
            column_id,
            column_name,
            column_desc,
        }
    }

    pub(crate) fn column_id(&self) -> ColumnIdT {
        self.column_id
    }

    pub(crate) fn column_name(&self) -> &str {
        &self.column_name
    }

    pub(crate) fn column_datatype(&self) -> &DataTypeRef {
        &self.column_desc.column_datatype
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.column_desc.set_primary(is_primary)
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.column_desc.is_primary()
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.column_desc.is_nullable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Int32Type;

    #[test]
    fn test_column_catalog() {
        let mut col_catalog = ColumnCatalog::new(
            0,
            String::from("test"),
            ColumnDesc::new(Int32Type { nullable: false }, false),
        );
        assert_eq!(col_catalog.column_id(), 0);
        assert_eq!(col_catalog.is_primary(), false);
        assert_eq!(col_catalog.column_datatype().as_ref().get_data_len(), 4);
        assert_eq!(col_catalog.column_name(), String::from("test"));
        col_catalog.set_primary(true);
        assert_eq!(col_catalog.is_primary(), true);
        assert_eq!(col_catalog.column_datatype().as_ref().is_nullable(), false);
    }
}
