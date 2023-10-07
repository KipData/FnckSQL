use crate::catalog::TableName;
use crate::expression::ScalarExpression;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption};
use std::sync::Arc;

use crate::types::{ColumnId, LogicalType};

pub type ColumnRef = Arc<ColumnCatalog>;

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct ColumnCatalog {
    pub id: Option<ColumnId>,
    pub name: String,
    pub table_name: Option<TableName>,
    pub nullable: bool,
    pub desc: ColumnDesc,
    pub ref_expr: Option<ScalarExpression>,
}

impl ColumnCatalog {
    pub(crate) fn new(
        column_name: String,
        nullable: bool,
        column_desc: ColumnDesc,
        ref_expr: Option<ScalarExpression>,
    ) -> ColumnCatalog {
        ColumnCatalog {
            id: None,
            name: column_name,
            table_name: None,
            nullable,
            desc: column_desc,
            ref_expr,
        }
    }

    pub(crate) fn new_dummy(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            id: Some(0),
            name: column_name,
            table_name: None,
            nullable: false,
            desc: ColumnDesc::new(LogicalType::Varchar(None), false, false),
            ref_expr: None,
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
        let mut column_desc = ColumnDesc::new(
            LogicalType::try_from(column_def.data_type).unwrap(),
            false,
            false,
        );
        let mut nullable = false;

        // TODO: 这里可以对更多字段可设置内容进行补充
        for option_def in column_def.options {
            match option_def.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => (),
                ColumnOption::Unique { is_primary } => {
                    if is_primary {
                        column_desc.is_primary = true;
                        nullable = false;
                        // Skip other options when using primary key
                        break;
                    } else {
                        column_desc.is_unique = true;
                    }
                }
                _ => todo!(),
            }
        }

        ColumnCatalog::new(column_name, nullable, column_desc, None)
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ColumnDesc {
    pub(crate) column_datatype: LogicalType,
    pub(crate) is_primary: bool,
    pub(crate) is_unique: bool,
}

impl ColumnDesc {
    pub(crate) const fn new(
        column_datatype: LogicalType,
        is_primary: bool,
        is_unique: bool,
    ) -> ColumnDesc {
        ColumnDesc {
            column_datatype,
            is_primary,
            is_unique,
        }
    }
}
