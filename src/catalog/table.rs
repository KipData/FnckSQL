use std::collections::BTreeMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, SchemaRef};

use itertools::Itertools;

use crate::catalog::{CatalogError, ColumnCatalog};
use crate::types::{ColumnId, IdGenerator, TableId};
#[derive(Debug, Clone, PartialEq)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    /// Mapping from column names to column ids
    column_idxs: BTreeMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
}

impl TableCatalog {
    pub(crate) fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn get_column_by_id(&self, id: &ColumnId) -> Option<&ColumnCatalog> {
        self.columns.get(id)
    }

    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<&ColumnCatalog> {
        let id = self.column_idxs.get(name)?;
        self.columns.get(id)
    }

    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn all_columns(&self) -> Vec<(&ColumnId, &ColumnCatalog)> {
        self.columns
            .iter()
            .collect()
    }

    // TODO: 缓存schema
    pub(crate) fn schema(&self) -> SchemaRef {
        let fields = self.columns.iter()
            .map(|(_, col)| col.to_field())
            .collect_vec();
        Arc::new(Schema::new(fields))
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(
        &mut self,
        mut col_catalog: ColumnCatalog,
    ) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(&col_catalog.name) {
            return Err(CatalogError::Duplicated("column", col_catalog.name.into()));
        }

        let col_id = col_catalog.id.unwrap();

        col_catalog.table_id = Some(self.id);
        self.column_idxs.insert(col_catalog.name.to_owned(), col_id);
        self.columns.insert(col_id, col_catalog);

        Ok(col_id)
    }

    pub(crate) fn new(
        table_name: String,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableCatalog, CatalogError> {
        let mut table_catalog = TableCatalog {
            id: IdGenerator::build(),
            name: table_name,
            column_idxs: BTreeMap::new(),
            columns: BTreeMap::new(),
        };

        for col_catalog in columns.into_iter() {
            let _ = table_catalog.add_column(col_catalog)?;
        }

        Ok(table_catalog)
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
        let col0 = ColumnCatalog::new(true, "a".into(), false, ColumnDesc::new(LogicalType::Integer, false));
        let col1 = ColumnCatalog::new(true, "b".into(), false, ColumnDesc::new(LogicalType::Boolean, false));
        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new("test".to_string(), col_catalogs).unwrap();

        assert_eq!(table_catalog.contains_column("a"), true);
        assert_eq!(table_catalog.contains_column("b"), true);
        assert_eq!(table_catalog.contains_column("c"), false);

        let col_a_id = table_catalog.get_column_id_by_name("a").unwrap();
        let col_b_id = table_catalog.get_column_id_by_name("b").unwrap();
        assert!(col_a_id < col_b_id);

        let column_catalog = table_catalog.get_column_by_id(&col_a_id).unwrap();
        assert_eq!(column_catalog.name, "a");
        assert_eq!(*column_catalog.datatype(), LogicalType::Integer,);

        let column_catalog = table_catalog.get_column_by_id(&col_b_id).unwrap();
        assert_eq!(column_catalog.name, "b");
        assert_eq!(*column_catalog.datatype(), LogicalType::Boolean,);
    }
}
