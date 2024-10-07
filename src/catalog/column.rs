use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::types::tuple::EMPTY_TUPLE;
use crate::types::value::ValueRef;
use crate::types::{ColumnId, LogicalType};
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hash::Hash;
use std::sync::Arc;

pub type ColumnRef = Arc<ColumnCatalog>;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnCatalog {
    pub summary: ColumnSummary,
    pub nullable: bool,
    pub desc: ColumnDesc,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum ColumnRelation {
    None,
    Table {
        column_id: ColumnId,
        table_name: TableName,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct ColumnSummary {
    pub name: String,
    pub relation: ColumnRelation,
}

impl ColumnCatalog {
    pub fn new(column_name: String, nullable: bool, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable,
            desc: column_desc,
        }
    }

    pub(crate) fn new_dummy(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable: true,
            // SAFETY: default expr must not be [`ScalarExpression::ColumnRef`]
            desc: ColumnDesc::new(
                LogicalType::Varchar(None, CharLengthUnits::Characters),
                false,
                false,
                None,
            )
            .unwrap(),
        }
    }

    pub(crate) fn summary(&self) -> &ColumnSummary {
        &self.summary
    }

    pub(crate) fn id(&self) -> Option<ColumnId> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { column_id, .. } => Some(*column_id),
        }
    }

    pub fn name(&self) -> &str {
        &self.summary.name
    }

    pub fn full_name(&self) -> String {
        if let Some(table_name) = self.table_name() {
            return format!("{}.{}", table_name, self.name());
        }
        self.name().to_string()
    }

    pub fn table_name(&self) -> Option<&TableName> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { table_name, .. } => Some(table_name),
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.summary.name = name;
    }

    pub fn set_ref_table(&mut self, table_name: TableName, column_id: ColumnId) {
        self.summary.relation = ColumnRelation::Table {
            column_id,
            table_name,
        };
    }

    pub fn datatype(&self) -> &LogicalType {
        &self.desc.column_datatype
    }

    pub(crate) fn default_value(&self) -> Result<Option<ValueRef>, DatabaseError> {
        self.desc
            .default
            .as_ref()
            .map(|expr| expr.eval(&EMPTY_TUPLE, &[]))
            .transpose()
    }

    #[allow(dead_code)]
    pub(crate) fn desc(&self) -> &ColumnDesc {
        &self.desc
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub(crate) column_datatype: LogicalType,
    pub(crate) is_primary: bool,
    pub(crate) is_unique: bool,
    pub(crate) default: Option<ScalarExpression>,
}

impl ColumnDesc {
    pub fn new(
        column_datatype: LogicalType,
        is_primary: bool,
        is_unique: bool,
        default: Option<ScalarExpression>,
    ) -> Result<ColumnDesc, DatabaseError> {
        if let Some(expr) = &default {
            if expr.has_table_ref_column() {
                return Err(DatabaseError::DefaultNotColumnRef);
            }
        }

        Ok(ColumnDesc {
            column_datatype,
            is_primary,
            is_unique,
            default,
        })
    }
}
