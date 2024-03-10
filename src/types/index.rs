use crate::catalog::{TableCatalog, TableName};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::expression::ScalarExpression;
use crate::types::value::ValueRef;
use crate::types::{ColumnId, LogicalType};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub type IndexId = u32;
pub type IndexMetaRef = Arc<IndexMeta>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum IndexType {
    PrimaryKey,
    Unique,
    Normal,
    Composite,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IndexInfo {
    pub(crate) meta: IndexMetaRef,
    pub(crate) range: Option<Range>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct IndexMeta {
    pub id: IndexId,
    pub column_ids: Vec<ColumnId>,
    pub table_name: TableName,
    pub pk_ty: LogicalType,
    pub name: String,
    pub ty: IndexType,
}

impl IndexMeta {
    pub(crate) fn column_exprs(
        &self,
        table: &TableCatalog,
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
        let mut exprs = Vec::with_capacity(self.column_ids.len());

        for column_id in self.column_ids.iter() {
            if let Some(column) = table.get_column_by_id(column_id) {
                exprs.push(ScalarExpression::ColumnRef(column.clone()));
            } else {
                return Err(DatabaseError::NotFound(
                    "Column by id",
                    column_id.to_string(),
                ));
            }
        }
        Ok(exprs)
    }
}

pub struct Index<'a> {
    pub id: IndexId,
    pub column_values: &'a [ValueRef],
    pub ty: IndexType,
}

impl<'a> Index<'a> {
    pub fn new(id: IndexId, column_values: &'a [ValueRef], ty: IndexType) -> Self {
        Index {
            id,
            column_values,
            ty,
        }
    }
}

impl fmt::Display for IndexInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.meta)?;
        write!(f, " => ")?;

        if let Some(range) = &self.range {
            write!(f, "{}", range)?;
        } else {
            write!(f, "EMPTY")?;
        }

        Ok(())
    }
}

impl fmt::Display for IndexMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
