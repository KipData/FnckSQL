use crate::catalog::{CatalogError, SchemaCatalog, DEFAULT_SCHEMA_NAME, CatalogTemp, Catalog};
use crate::types::CatalogId;
use std::sync::Arc;

pub(crate) struct DatabaseCatalog {
    inner: CatalogTemp<SchemaCatalog>,
}

impl DatabaseCatalog {
    pub(crate) fn new(database_name: String) -> Result<Self, CatalogError> {
        let db_catalog = DatabaseCatalog { inner: CatalogTemp::new(database_name, "schema") };
        let default_schema = DEFAULT_SCHEMA_NAME.to_string();

        db_catalog.add(default_schema.clone(), SchemaCatalog::new(default_schema))?;

        Ok(db_catalog)
    }

    pub(crate) fn add_schema(&self, schema_name: String) -> Result<CatalogId, CatalogError> {
        self.add(schema_name.clone(), SchemaCatalog::new(schema_name))
    }
}

impl Catalog<SchemaCatalog> for DatabaseCatalog {
    fn add(&self, name: String, item: SchemaCatalog) -> Result<CatalogId, CatalogError> {
        self.inner.add(name, item)
    }

    fn delete(&self, name: &str) -> Result<(), CatalogError> {
        self.inner.delete(name)
    }

    fn all(&self) -> Vec<Arc<SchemaCatalog>> {
        self.inner.all()
    }

    fn get_id_by_name(&self, name: &str) -> Option<CatalogId> {
        self.inner.get_id_by_name(name)
    }

    fn get_by_id(&self, id: CatalogId) -> Option<Arc<SchemaCatalog>> {
        self.inner.get_by_id(id)
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<SchemaCatalog>> {
        self.inner.get_by_name(name)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn id(&self) -> CatalogId {
        self.inner.id()
    }

    fn set_id(&mut self, id: CatalogId) {
        self.inner.set_id(id)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{Catalog, ColumnCatalog, DatabaseCatalog, SchemaCatalog, TableCatalog};
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_database_catalog() {
        let col0 = ColumnCatalog::new(
            0,
            "a".to_string(),
            DataTypeKind::Int(Some(32)).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(
            1,
            "b".to_string(),
            DataTypeKind::Boolean.not_null().to_column(),
        );
        let col_catalogs = vec![col0, col1];
        let mut _schema_catalog = SchemaCatalog::new("test_scheme".to_string());
        let _table_catalog = TableCatalog::new("test_table".to_string(), col_catalogs);

        let database_catalog = DatabaseCatalog::new("test_database".to_string()).unwrap();
        let schema_id = database_catalog.add_schema("test_schema".into()).unwrap();
        assert_eq!(schema_id, 1);

        let schema_catalog = database_catalog.get_by_id(schema_id).unwrap();
        assert_eq!(schema_catalog.name(), "test_schema");
    }
}
