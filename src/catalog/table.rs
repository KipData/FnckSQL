use crate::catalog::{Catalog, CatalogError, CatalogTemp, ColumnCatalog};
use std::sync::Arc;
use crate::types::ColumnId;

pub(crate) struct TableCatalog {
    inner: CatalogTemp<ColumnCatalog>,
}

impl TableCatalog {
    pub(crate) fn new(name: String, columns: Vec<ColumnCatalog>) -> Result<Self, CatalogError> {
        let catalog = TableCatalog {
            inner: CatalogTemp::new(name, "column"),
        };
        for column in columns {
            catalog.add_column(column)?;
        }
        Ok(catalog)
    }

    pub(crate) fn add_column(&self, col_catalog: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        self.inner.add(col_catalog.name().to_string(), col_catalog)
    }

    /// Check if the table catalog contains a column with the given name.
    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.inner.inner
            .lock()
            .idxs
            .contains_key(name)
    }
}

impl Catalog<ColumnCatalog> for TableCatalog {
    fn add(&self, name: String, item: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        self.inner.add(name, item)
    }

    fn delete(&self, name: &str) -> Result<(), CatalogError> {
        self.inner.delete(name)
    }

    fn all(&self) -> Vec<Arc<ColumnCatalog>> {
        self.inner.all()
    }

    fn get_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.inner.get_id_by_name(name)
    }

    fn get_by_id(&self, id: ColumnId) -> Option<Arc<ColumnCatalog>> {
        self.inner.get_by_id(id)
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<ColumnCatalog>> {
        self.inner.get_by_name(name)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn id(&self) -> ColumnId {
        self.inner.id()
    }

    fn set_id(&mut self, id: ColumnId) {
        self.inner.set_id(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, DataTypeExt, DataTypeKind};

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(
            0,
            "a".into(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(1, "b".into(), DataTypeKind::Boolean.not_null().to_column());
        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new("test".to_string(), col_catalogs).unwrap();

        assert_eq!(table_catalog.contains_column("a"), true);
        assert_eq!(table_catalog.contains_column("b"), true);
        assert_eq!(table_catalog.contains_column("c"), false);

        assert_eq!(table_catalog.get_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_id_by_name("b"), Some(1));

        let column_catalog = table_catalog.get_by_id(0).unwrap();
        assert_eq!(column_catalog.name(), "a");
        assert_eq!(
            column_catalog.datatype(),
            DataType::new(DataTypeKind::Int(None), false)
        );

        let column_catalog = table_catalog.get_by_id(1).unwrap();
        assert_eq!(column_catalog.name(), "b");
        assert_eq!(
            column_catalog.datatype(),
            DataType::new(DataTypeKind::Boolean, false)
        );
    }
}
