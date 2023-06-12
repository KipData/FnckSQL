use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog};
use crate::types::{SchemaIdT, TableIdT};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct SchemaCatalog {
    schema_id: SchemaIdT,
    inner: Mutex<Inner>,
}

struct Inner {
    schema_name: String,
    table_idxs: HashMap<String, TableIdT>,
    tables: BTreeMap<TableIdT, Arc<TableCatalog>>,
    next_table_id: TableIdT,
}

impl SchemaCatalog {
    pub(crate) fn new(schema_id: SchemaIdT, schema_name: String) -> SchemaCatalog {
        SchemaCatalog {
            schema_id,
            inner: Mutex::new(Inner {
                schema_name,
                table_idxs: HashMap::new(),
                tables: BTreeMap::new(),
                next_table_id: 0,
            }),
        }
    }

    pub(crate) fn add_table(
        &self,
        table_name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
    ) -> Result<TableIdT, CatalogError> {
        let mut inner = self.inner.lock();
        if inner.table_idxs.contains_key(&table_name) {
            return Err(CatalogError::Duplicated("column", table_name));
        }
        let table_id = inner.next_table_id;
        inner.next_table_id += 1;
        let table_catalog = Arc::new(TableCatalog::new(
            table_id,
            table_name.clone(),
            columns,
            is_materialized_view,
        ));
        inner.table_idxs.insert(table_name, table_id);
        inner.tables.insert(table_id, table_catalog);
        Ok(table_id)
    }

    pub(crate) fn delete_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock();

        let id = inner
            .table_idxs
            .remove(table_name)
            .ok_or_else(|| CatalogError::NotFound("table", table_name.into()))?;
        inner.tables.remove(&id);
        Ok(())
    }

    pub(crate) fn get_all_tables(&self) -> BTreeMap<TableIdT, Arc<TableCatalog>> {
        let inner = self.inner.lock();
        inner.tables.clone()
    }

    pub(crate) fn get_table_id_by_name(&self, name: &str) -> Option<TableIdT> {
        let inner = self.inner.lock();
        inner.table_idxs.get(name).cloned()
    }

    pub(crate) fn get_table_by_id(&self, table_id: TableIdT) -> Option<Arc<TableCatalog>> {
        let inner = self.inner.lock();
        inner.tables.get(&table_id).cloned()
    }

    pub(crate) fn get_table_by_name(&self, name: &str) -> Option<Arc<TableCatalog>> {
        let inner = self.inner.lock();
        inner
            .table_idxs
            .get(name)
            .and_then(|id| inner.tables.get(id))
            .cloned()
    }

    pub(crate) fn id(&self) -> SchemaIdT {
        self.schema_id
    }
    pub fn name(&self) -> String {
        let inner = self.inner.lock();
        inner.schema_name.clone()
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
        let mut schema_catalog = SchemaCatalog::new(0, "test_scheme".to_string());
        let table_id = schema_catalog
            .add_table("test_table".to_string(), col_catalogs, false)
            .unwrap();
        assert_eq!(table_id, 0);

        let table_catalog = schema_catalog.get_table_by_id(table_id).unwrap();
        assert_eq!(table_catalog.name(), "test_table");

        let table_catalog = schema_catalog
            .get_table_by_name(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog.name(), "test_table");

        let table_catalog = schema_catalog
            .delete_table(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog, ());

        let table_catalog = schema_catalog.delete_table(&String::from("test_table"));
        assert_eq!(
            table_catalog,
            Err(CatalogError::NotFound("table", "test_table".into()))
        );
    }
}
