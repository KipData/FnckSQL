use arrow::datatypes::{DataType, Field};
use sqlparser::ast::ColumnDef;

use crate::types::{ColumnId, IdGenerator, LogicalType};

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub name: String,
    pub desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(column_name: String, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            id: IdGenerator::build(),
            name: column_name,
            desc: column_desc,
        }
    }

    pub(crate) fn datatype(&self) -> &LogicalType {
        &self.desc.column_datatype
    }

    pub(crate) fn id(&self) -> ColumnId {
        self.id
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn to_field(&self) -> Field {
        Field::new(
            &*self.name.clone(),
            DataType::from(self.desc.column_datatype.clone()),
            self.desc.is_primary(),
        )
    }
}

impl From<ColumnDef> for ColumnCatalog {
    fn from(column_def: ColumnDef) -> Self {
        let column_name = column_def.name.to_string();
        let column_datatype = LogicalType::try_from(column_def.data_type).unwrap();
        let is_primary = false;
        let column_desc = ColumnDesc::new(column_datatype, is_primary);
        ColumnCatalog::new(column_name, column_desc)
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub(crate) column_datatype: LogicalType,
    pub(crate) is_primary: bool,
}

impl ColumnDesc {
    pub(crate) const fn new(column_datatype: LogicalType, is_primary: bool) -> ColumnDesc {
        ColumnDesc {
            column_datatype,
            is_primary,
        }
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub(crate) fn get_datatype(&self) -> LogicalType {
        self.column_datatype.clone()
    }
}
