use arrow::datatypes::{DataType, Field};
use sqlparser::ast::{ColumnDef, ColumnOption};

use crate::types::{ColumnIdx, LogicalType};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnCatalog {
    pub id: Option<ColumnIdx>,
    pub name: String,
    pub nullable: bool,
    pub desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(column_name: String, nullable: bool, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            id: None,
            name: column_name,
            nullable,
            desc: column_desc,
        }
    }

    pub(crate) fn datatype(&self) -> &LogicalType {
        &self.desc.column_datatype
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn to_field(&self) -> Field {
        Field::new(
            self.name.as_str(),
            DataType::from(self.datatype().clone()),
            self.nullable,
        )
    }
}

impl From<ColumnDef> for ColumnCatalog {
    fn from(column_def: ColumnDef) -> Self {
        let column_name = column_def.name.to_string();
        let column_desc = ColumnDesc::new(
            LogicalType::try_from(column_def.data_type).unwrap(),
            false
        );
        let mut nullable = false;

        // TODO: 这里可以对更多字段可设置内容进行补充
        for option_def in column_def.options {
            match option_def.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => (),
                _ => todo!()
            }
        }

        ColumnCatalog::new(column_name, nullable, column_desc)
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
