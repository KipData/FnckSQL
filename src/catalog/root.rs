use crate::catalog::{CatalogError, DatabaseCatalog, DatabaseCatalogRef, DEFAULT_DATABASE_NAME};
use crate::types::DatabaseIdT;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

pub(crate) struct RootCatalog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Database name to database id mapping
    database_idxs: HashMap<String, DatabaseIdT>,
    /// Database id to database catalog mapping
    databases: BTreeMap<DatabaseIdT, Arc<DatabaseCatalog>>,
    next_database_id: DatabaseIdT,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    pub(crate) fn new() -> RootCatalog {
        let root_catalog = RootCatalog {
            inner: Mutex::new(Inner::default()),
        };
        root_catalog
            .add_database(DEFAULT_DATABASE_NAME.into())
            .unwrap();
        root_catalog
    }

    pub(crate) fn add_database(&self, database_name: String) -> Result<DatabaseIdT, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.database_idxs.contains_key(&database_name) {
            return Err(CatalogError::Duplicated("database", database_name));
        }
        let database_id = inner.next_database_id;
        inner.next_database_id += 1;
        let database_catalog = Arc::new(DatabaseCatalog::new(database_id, database_name.clone()));
        inner.database_idxs.insert(database_name, database_id);
        inner.databases.insert(database_id, database_catalog);
        Ok(database_id)
    }

    pub(crate) fn delete_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .database_idxs
            .remove(database_name)
            .ok_or_else(|| CatalogError::NotFound("database", database_name.into()))?;
        inner.databases.remove(&id);
        Ok(())
    }

    pub(crate) fn get_all_databases(&self) -> BTreeMap<DatabaseIdT, DatabaseCatalogRef> {
        let inner = self.inner.lock().unwrap();
        inner.databases.clone()
    }

    pub(crate) fn get_database_id_by_name(&self, name: &str) -> Option<DatabaseIdT> {
        let inner = self.inner.lock().unwrap();
        inner.database_idxs.get(name).cloned()
    }

    pub(crate) fn get_database_by_id(
        &self,
        database_id: DatabaseIdT,
    ) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.databases.get(&database_id).cloned()
    }

    pub(crate) fn get_database_by_name(&self, name: &str) -> Option<Arc<DatabaseCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner
            .database_idxs
            .get(name)
            .and_then(|id| inner.databases.get(id))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, DEFAULT_SCHEMA_NAME};
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_root_catalog() {
        let root_catalog = RootCatalog::new();
        let database_id = root_catalog
            .get_database_id_by_name(DEFAULT_DATABASE_NAME)
            .unwrap();
        let database_catalog = root_catalog.get_database_by_id(database_id).unwrap();
        let schema_catalog = database_catalog
            .get_schema_by_name(DEFAULT_SCHEMA_NAME)
            .unwrap();

        let col0 = ColumnCatalog::new(
            0,
            "a".into(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(1, "b".into(), DataTypeKind::Boolean.not_null().to_column());
        let col_catalogs = vec![col0, col1];

        let table_id = schema_catalog
            .add_table("test_table".into(), col_catalogs, false)
            .unwrap();

        assert_eq!(table_id, 0);

        assert_eq!(database_catalog.name(), DEFAULT_DATABASE_NAME);
        assert_eq!(schema_catalog.name(), DEFAULT_SCHEMA_NAME);
    }
}
