use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::{CatalogError, ColumnCatalog, ColumnRef};
use crate::types::index::{IndexMeta, IndexMetaRef};
use crate::types::ColumnId;

pub type TableName = Arc<String>;

#[derive(Debug, Clone, PartialEq)]
pub struct TableCatalog {
    pub(crate) name: TableName,
    /// Mapping from column names to column ids
    column_idxs: BTreeMap<String, ColumnId>,
    pub(crate) columns: BTreeMap<ColumnId, ColumnRef>,
    pub(crate) indexes: Vec<IndexMetaRef>,
}

impl TableCatalog {
    pub(crate) fn get_unique_index(&self, col_id: &ColumnId) -> Option<&IndexMetaRef> {
        self.indexes
            .iter()
            .find(|meta| meta.is_unique && &meta.column_ids[0] == col_id)
    }

    #[allow(dead_code)]
    pub(crate) fn get_column_by_id(&self, id: &ColumnId) -> Option<&ColumnRef> {
        self.columns.get(id)
    }

    #[allow(dead_code)]
    pub(crate) fn get_column_id_by_name(&self, name: &String) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub(crate) fn get_column_by_name(&self, name: &String) -> Option<&ColumnRef> {
        let id = self.column_idxs.get(name)?;
        self.columns.get(id)
    }

    pub(crate) fn contains_column(&self, name: &String) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn all_columns_with_id(&self) -> Vec<(&ColumnId, &ColumnRef)> {
        self.columns.iter().collect()
    }

    pub(crate) fn all_columns(&self) -> Vec<ColumnRef> {
        self.columns
            .iter()
            .map(|(_, col)| Arc::clone(col))
            .collect()
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(&mut self, mut col: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(&col.name) {
            return Err(CatalogError::Duplicated("column", col.name.clone()));
        }

        let col_id = self.columns.len() as u32;

        col.id = Some(col_id);
        col.table_name = Some(self.name.clone());
        self.column_idxs.insert(col.name.clone(), col_id);
        self.columns.insert(col_id, Arc::new(col));

        Ok(col_id)
    }

    pub(crate) fn add_index_meta(&mut self, mut index: IndexMeta) -> &IndexMeta {
        let index_id = self.indexes.len();

        index.id = index_id as u32;
        self.indexes.push(Arc::new(index));

        &self.indexes[index_id]
    }

    pub(crate) fn new(
        name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableCatalog, CatalogError> {
        let mut table_catalog = TableCatalog {
            name,
            column_idxs: BTreeMap::new(),
            columns: BTreeMap::new(),
            indexes: vec![],
        };

        for col_catalog in columns.into_iter() {
            let _ = table_catalog.add_column(col_catalog)?;
        }

        Ok(table_catalog)
    }

    pub(crate) fn new_with_indexes(
        name: TableName,
        columns: Vec<ColumnCatalog>,
        indexes: Vec<IndexMetaRef>,
    ) -> Result<TableCatalog, CatalogError> {
        let mut catalog = TableCatalog::new(name, columns)?;
        catalog.indexes = indexes;

        Ok(catalog)
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
            ColumnDesc::new(LogicalType::Integer, false, false),
            None,
        );
        let col1 = ColumnCatalog::new(
            "b".into(),
            false,
            ColumnDesc::new(LogicalType::Boolean, false, false),
            None,
        );
        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(Arc::new("test".to_string()), col_catalogs).unwrap();

        assert_eq!(table_catalog.contains_column(&"a".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"b".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"c".to_string()), false);

        let col_a_id = table_catalog
            .get_column_id_by_name(&"a".to_string())
            .unwrap();
        let col_b_id = table_catalog
            .get_column_id_by_name(&"b".to_string())
            .unwrap();
        assert!(col_a_id < col_b_id);

        let column_catalog = table_catalog.get_column_by_id(&col_a_id).unwrap();
        assert_eq!(column_catalog.name, "a");
        assert_eq!(*column_catalog.datatype(), LogicalType::Integer,);

        let column_catalog = table_catalog.get_column_by_id(&col_b_id).unwrap();
        assert_eq!(column_catalog.name, "b");
        assert_eq!(*column_catalog.datatype(), LogicalType::Boolean,);
    }
}
