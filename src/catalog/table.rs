use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{slice, vec};

use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::SchemaRef;
use crate::types::{ColumnId, LogicalType};

pub type TableName = Arc<String>;

#[derive(Debug, Clone, PartialEq)]
pub struct TableCatalog {
    pub(crate) name: TableName,
    /// Mapping from column names to column ids
    column_idxs: BTreeMap<String, (ColumnId, usize)>,
    columns: BTreeMap<ColumnId, usize>,
    pub(crate) indexes: Vec<IndexMetaRef>,

    schema_ref: SchemaRef,
}

//TODO: can add some like Table description and other information as attributes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMeta {
    pub(crate) table_name: TableName,
}

impl TableCatalog {
    pub(crate) fn get_unique_index(&self, col_id: &ColumnId) -> Option<&IndexMetaRef> {
        self.indexes
            .iter()
            .find(|meta| matches!(meta.ty, IndexType::Unique) && &meta.column_ids[0] == col_id)
    }

    #[allow(dead_code)]
    pub(crate) fn get_column_by_id(&self, id: &ColumnId) -> Option<&ColumnRef> {
        self.columns.get(id).map(|i| &self.schema_ref[*i])
    }

    #[allow(dead_code)]
    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).map(|(id, _)| id).cloned()
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<&ColumnRef> {
        self.column_idxs
            .get(name)
            .map(|(_, i)| &self.schema_ref[*i])
    }

    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn columns(&self) -> slice::Iter<'_, ColumnRef> {
        self.schema_ref.iter()
    }

    pub(crate) fn indexes(&self) -> slice::Iter<'_, IndexMetaRef> {
        self.indexes.iter()
    }

    pub fn schema_ref(&self) -> &SchemaRef {
        &self.schema_ref
    }

    pub(crate) fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn primary_key(&self) -> Result<(usize, &ColumnRef), DatabaseError> {
        self.schema_ref
            .iter()
            .enumerate()
            .find(|(_, column)| column.desc.is_primary)
            .ok_or(DatabaseError::PrimaryKeyNotFound)
    }

    pub(crate) fn types(&self) -> Vec<LogicalType> {
        self.columns()
            .map(|column| *column.datatype())
            .collect_vec()
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(&mut self, mut col: ColumnCatalog) -> Result<ColumnId, DatabaseError> {
        if self.column_idxs.contains_key(col.name()) {
            return Err(DatabaseError::DuplicateColumn(col.name().to_string()));
        }

        let col_id = self
            .columns
            .iter()
            .last()
            .map(|(column_id, _)| column_id + 1)
            .unwrap_or(0);

        col.summary.table_name = Some(self.name.clone());
        col.summary.id = Some(col_id);

        self.column_idxs
            .insert(col.name().to_string(), (col_id, self.schema_ref.len()));
        self.columns.insert(col_id, self.schema_ref.len());

        let mut schema = Vec::clone(&self.schema_ref);
        schema.push(Arc::new(col));
        self.schema_ref = Arc::new(schema);

        Ok(col_id)
    }

    pub(crate) fn add_index_meta(
        &mut self,
        name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<&IndexMeta, DatabaseError> {
        for index in self.indexes.iter() {
            if index.name == name {
                return Err(DatabaseError::DuplicateIndex(name));
            }
        }

        let index_id = self.indexes.last().map(|index| index.id + 1).unwrap_or(0);
        let pk_ty = *self.primary_key()?.1.datatype();
        let index = IndexMeta {
            id: index_id,
            column_ids,
            table_name: self.name.clone(),
            pk_ty,
            name,
            ty,
        };
        self.indexes.push(Arc::new(index));
        Ok(self.indexes.last().unwrap())
    }

    pub fn new(
        name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableCatalog, DatabaseError> {
        if columns.is_empty() {
            return Err(DatabaseError::ColumnsEmpty);
        }
        let mut table_catalog = TableCatalog {
            name,
            column_idxs: BTreeMap::new(),
            columns: BTreeMap::new(),
            indexes: vec![],
            schema_ref: Arc::new(vec![]),
        };
        for col_catalog in columns.into_iter() {
            let _ = table_catalog.add_column(col_catalog)?;
        }

        Ok(table_catalog)
    }

    pub(crate) fn reload(
        name: TableName,
        columns: Vec<ColumnCatalog>,
        indexes: Vec<IndexMetaRef>,
    ) -> Result<TableCatalog, DatabaseError> {
        let mut catalog = TableCatalog::new(name, columns)?;
        catalog.indexes = indexes;

        Ok(catalog)
    }
}

impl TableMeta {
    pub(crate) fn empty(table_name: TableName) -> Self {
        TableMeta { table_name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDesc;
    use crate::types::LogicalType;

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(
            "a".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, false, false, None),
        );
        let col1 = ColumnCatalog::new(
            "b".into(),
            false,
            ColumnDesc::new(LogicalType::Boolean, false, false, None),
        );
        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(Arc::new("test".to_string()), col_catalogs).unwrap();

        debug_assert_eq!(table_catalog.contains_column(&"a".to_string()), true);
        debug_assert_eq!(table_catalog.contains_column(&"b".to_string()), true);
        debug_assert_eq!(table_catalog.contains_column(&"c".to_string()), false);

        let col_a_id = table_catalog
            .get_column_id_by_name(&"a".to_string())
            .unwrap();
        let col_b_id = table_catalog
            .get_column_id_by_name(&"b".to_string())
            .unwrap();
        debug_assert!(col_a_id < col_b_id);

        let column_catalog = table_catalog.get_column_by_id(&col_a_id).unwrap();
        debug_assert_eq!(column_catalog.name(), "a");
        debug_assert_eq!(*column_catalog.datatype(), LogicalType::Integer,);

        let column_catalog = table_catalog.get_column_by_id(&col_b_id).unwrap();
        debug_assert_eq!(column_catalog.name(), "b");
        debug_assert_eq!(*column_catalog.datatype(), LogicalType::Boolean,);
    }
}
