use crate::catalog::{Catalog, CatalogError, CatalogTemp, ColumnCatalog, TableCatalog};
use std::sync::Arc;
use crate::types::TableId;

pub(crate) struct SchemaCatalog {
    inner: CatalogTemp<TableCatalog>
}

impl SchemaCatalog {
    pub(crate) fn new(schema_name: String) -> SchemaCatalog {
        SchemaCatalog {
            inner: CatalogTemp::new(schema_name, "table"),
        }
    }

    pub(crate) fn add_table(&self, table_name: String, columns: Vec<ColumnCatalog>) -> Result<TableId, CatalogError> {
        self.inner.add(table_name.clone(), TableCatalog::new(table_name, columns)?)
    }
}

impl Catalog<TableCatalog> for SchemaCatalog {
    fn add(&self, name: String, item: TableCatalog) -> Result<TableId, CatalogError> {
        self.inner.add(name, item)
    }

    fn delete(&self, name: &str) -> Result<(), CatalogError> {
        self.inner.delete(name)
    }

    fn all(&self) -> Vec<Arc<TableCatalog>> {
        self.inner.all()
    }

    fn get_id_by_name(&self, name: &str) -> Option<TableId> {
        self.inner.get_id_by_name(name)
    }

    fn get_by_id(&self, id: TableId) -> Option<Arc<TableCatalog>> {
        self.inner.get_by_id(id)
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        self.inner.get_by_name(name)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn id(&self) -> TableId {
        self.inner.id()
    }

    fn set_id(&mut self, id: TableId) {
        self.inner.set_id(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_schema_catalog() {
        let col0 = ColumnCatalog::new(
            0,
            "a".into(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(1, "b".into(), DataTypeKind::Boolean.not_null().to_column());
        let col_catalogs = vec![col0, col1];
        let mut schema_catalog = SchemaCatalog::new("test_scheme".to_string());
        let table_id = schema_catalog
            .add_table("test_table".to_string(), col_catalogs)
            .unwrap();
        assert_eq!(table_id, 0);

        let table_catalog = schema_catalog.get_by_id(table_id).unwrap();
        assert_eq!(table_catalog.name(), "test_table");

        let table_catalog = schema_catalog
            .get_by_name(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog.name(), "test_table");

        let table_catalog = schema_catalog
            .delete("test_table")
            .unwrap();
        assert_eq!(table_catalog, ());

        let table_catalog = schema_catalog.delete("test_table");
        assert_eq!(
            table_catalog,
            Err(CatalogError::NotFound("table", "test_table".into()))
        );
    }
}
