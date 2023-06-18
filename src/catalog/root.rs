use crate::catalog::{Catalog, CatalogError, CatalogTemp, DatabaseCatalog, DEFAULT_DATABASE_NAME};
use std::sync::Arc;
use crate::types::DatabaseId;

pub(crate) struct RootCatalog {
    inner: CatalogTemp<DatabaseCatalog>,
}

impl RootCatalog {
    pub(crate) fn new() -> Result<RootCatalog, CatalogError> {
        let root_catalog = RootCatalog {
            inner: CatalogTemp::new("".to_string(), "database"),
        };
        root_catalog.add_database(DEFAULT_DATABASE_NAME.into())?;

        Ok(root_catalog)
    }

    pub(crate) fn add_database(&self, database_name: String) -> Result<DatabaseId, CatalogError> {
        self.inner.add(database_name.clone(), DatabaseCatalog::new(database_name)?)
    }
}

impl Catalog<DatabaseCatalog> for RootCatalog {
    fn add(&self, name: String, item: DatabaseCatalog) -> Result<DatabaseId, CatalogError> {
        self.inner.add(name, item)
    }

    fn delete(&self, name: &str) -> Result<(), CatalogError> {
        self.inner.delete(name)
    }

    fn all(&self) -> Vec<Arc<DatabaseCatalog>> {
        self.inner.all()
    }

    fn get_id_by_name(&self, name: &str) -> Option<DatabaseId> {
        self.inner.get_id_by_name(name)
    }

    fn get_by_id(&self, id: DatabaseId) -> Option<Arc<DatabaseCatalog>> {
        self.inner.get_by_id(id)
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<DatabaseCatalog>> {
        self.inner.get_by_name(name)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn id(&self) -> DatabaseId {
        self.inner.id()
    }

    fn set_id(&mut self, id: DatabaseId) {
        self.inner.set_id(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, DEFAULT_SCHEMA_NAME};
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_root_catalog() {
        let root_catalog = RootCatalog::new().unwrap();
        let database_id = root_catalog
            .get_id_by_name(DEFAULT_DATABASE_NAME)
            .unwrap();
        let database_catalog = root_catalog.get_by_id(database_id).unwrap();
        let schema_catalog = database_catalog
            .get_by_name(DEFAULT_SCHEMA_NAME)
            .unwrap();

        let col0 = ColumnCatalog::new(
            0,
            "a".to_string(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(
            1,
            "b".to_string(),
            DataTypeKind::Boolean.not_null().to_column(),
        );
        let col_catalogs = vec![col0, col1];

        let table_id = schema_catalog
            .add_table("test_table".into(), col_catalogs)
            .unwrap();

        assert_eq!(table_id, 0);
        assert_eq!(database_catalog.name(), DEFAULT_DATABASE_NAME);
        assert_eq!(schema_catalog.name(), DEFAULT_SCHEMA_NAME);
    }
}
