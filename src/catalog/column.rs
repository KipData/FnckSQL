use std::sync::Arc;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption};
use crate::catalog::TableName;

use crate::types::{ColumnId, IdGenerator, LogicalType};

pub type ColumnRef = Arc<ColumnCatalog>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub name: String,
    pub table_name: Option<TableName>,
    pub nullable: bool,
    pub desc: ColumnDesc,
}

// Tips: When there is a Join, the on condition in the Join has a nullable condition,
// and the nullable in the Projection will change after being affected by the JoinType,
// so that Eq does not necessarily match, so only use the ID as the matching criterion
impl PartialEq<Self> for ColumnCatalog {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ColumnCatalog {

}

impl ColumnCatalog {
    pub(crate) fn new(column_name: String, nullable: bool, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            id: IdGenerator::build(),
            name: column_name,
            table_name: None,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}
