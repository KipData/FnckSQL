use crate::catalog::{CatalogError, SchemaCatalog, SchemaCatalogRef, DEFAULT_SCHEMA_NAME};
use crate::types::{DatabaseIdT, SchemaIdT};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct DatabaseCatalog {
    database_id: DatabaseIdT,
    inner: Mutex<Inner>,
}

struct Inner {
    database_name: String,
    /// schema_name -> schema_id
    schema_idxs: HashMap<String, SchemaIdT>,
    /// schema_id -> schema_catalog
    schemas: BTreeMap<SchemaIdT, Arc<SchemaCatalog>>,
    next_schema_id: SchemaIdT,
}

impl DatabaseCatalog {
    pub(crate) fn new(database_id: DatabaseIdT, database_name: String) -> Self {
        let db_catalog = DatabaseCatalog {
            database_id,
            inner: Mutex::new(Inner {
                database_name,
                schema_idxs: HashMap::new(),
                schemas: BTreeMap::new(),
                next_schema_id: 0,
            }),
        };
        let _ = db_catalog.add_schema(DEFAULT_SCHEMA_NAME.into()).is_ok();
        db_catalog
    }

    pub(crate) fn add_schema(&self, schema_name: String) -> Result<SchemaIdT, CatalogError> {
        let mut inner = self.inner.lock();
        if inner.schema_idxs.contains_key(&schema_name) {
            return Err(CatalogError::Duplicated("schema", schema_name));
        }
        let schema_id = inner.next_schema_id;
        inner.next_schema_id += 1;
        let schema_catalog = Arc::new(SchemaCatalog::new(schema_id, schema_name.clone()));
        inner.schema_idxs.insert(schema_name, schema_id);
        inner.schemas.insert(schema_id, schema_catalog);
        Ok(schema_id)
    }

    pub(crate) fn delete_schema(&self, schema_name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock();
        let id = inner
            .schema_idxs
            .remove(schema_name)
            .ok_or_else(|| CatalogError::NotFound("schema", schema_name.into()))?;
        inner.schemas.remove(&id);
        Ok(())
    }

    pub(crate) fn get_all_schemas(&self) -> BTreeMap<SchemaIdT, SchemaCatalogRef> {
        let inner = self.inner.lock();
        inner.schemas.clone()
    }

    pub(crate) fn get_schema_id_by_name(&self, name: &str) -> Option<SchemaIdT> {
        let inner = self.inner.lock();
        inner.schema_idxs.get(name).cloned()
    }

    pub(crate) fn get_schema_by_id(&self, schema_id: SchemaIdT) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock();
        inner.schemas.get(&schema_id).cloned()
    }

    pub(crate) fn get_schema_by_name(&self, name: &str) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock();
        inner
            .schema_idxs
            .get(name)
            .and_then(|schema_id| inner.schemas.get(schema_id))
            .cloned()
    }

    pub(crate) fn name(&self) -> String {
        let inner = self.inner.lock();
        inner.database_name.clone()
    }

    pub(crate) fn id(&self) -> DatabaseIdT {
        self.database_id
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, DatabaseCatalog, SchemaCatalog, TableCatalog};
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
        let mut _schema_catalog = SchemaCatalog::new(0, "test_scheme".to_string());
        let _table_catalog = TableCatalog::new(0, "test_table".to_string(), col_catalogs, false);

        let database_catalog = DatabaseCatalog::new(0, "test_database".to_string());
        let schema_id = database_catalog.add_schema("test_schema".into()).unwrap();
        assert_eq!(schema_id, 1);

        let schema_catalog = database_catalog.get_schema_by_id(schema_id).unwrap();
        assert_eq!(schema_catalog.name(), "test_schema");
    }
}
