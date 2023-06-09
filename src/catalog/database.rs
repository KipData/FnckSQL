use crate::catalog::{SchemaCatalog, SchemaCatalogRef};
use crate::types::{DatabaseIdT, SchemaIdT};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct DatabaseCatalog {
    database_id: DatabaseIdT,
    database_name: String,
    /// schema_name -> schema_id
    schema_idxs: HashMap<String, SchemaIdT>,
    /// schema_id -> schema_catalog
    schemas: BTreeMap<SchemaIdT, SchemaCatalogRef>,
    next_schema_id: SchemaIdT,
}

impl DatabaseCatalog {
    pub(crate) fn add_schema(
        &mut self,
        schema_name: String,
        schema_catalog: SchemaCatalog,
    ) -> Result<SchemaIdT, String> {
        if self.schema_idxs.contains_key(&schema_name) {
            Err(String::from("Duplicated schema name!"))
        } else {
            let schema_id = self.next_schema_id;
            self.next_schema_id += 1;
            let schema_catalog = Arc::new(schema_catalog);
            self.schema_idxs.insert(schema_name, schema_id);
            self.schemas.insert(schema_id, schema_catalog);
            Ok(schema_id)
        }
    }

    pub(crate) fn delete_schema(&mut self, schema_name: &str) -> Result<(), String> {
        if self.schema_idxs.contains_key(schema_name) {
            let id = self.schema_idxs.remove(schema_name).unwrap();
            self.schemas.remove(&id);
            Ok(())
        } else {
            Err(String::from("Schema does not exist: ") + schema_name)
        }
    }

    pub(crate) fn get_all_schemas(&self) -> &BTreeMap<SchemaIdT, SchemaCatalogRef> {
        &self.schemas
    }

    pub(crate) fn get_schema_id_by_name(&self, name: &String) -> Option<SchemaIdT> {
        self.schema_idxs.get(name).cloned()
    }

    pub(crate) fn get_schema_by_id(&self, schema_id: SchemaIdT) -> Option<SchemaCatalogRef> {
        self.schemas.get(&schema_id).cloned()
    }

    pub(crate) fn get_schema_by_name(&self, name: &String) -> Option<SchemaCatalogRef> {
        match self.get_schema_id_by_name(name) {
            Some(v) => self.get_schema_by_id(v),
            None => None,
        }
    }

    pub(crate) fn get_database_name(&self) -> String {
        self.database_name.clone()
    }

    pub(crate) fn get_database_id(&self) -> DatabaseIdT {
        self.database_id
    }

    pub(crate) fn new(database_id: DatabaseIdT, database_name: String) -> Self {
        Self {
            database_id,
            database_name,
            schema_idxs: HashMap::new(),
            schemas: BTreeMap::new(),
            next_schema_id: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnDesc, DatabaseCatalog, SchemaCatalog, TableCatalog};
    use crate::types::{BoolType, Int32Type};

    #[test]
    fn test_database_catalog() {
        let mut schema_catalog = SchemaCatalog::new(0, String::from("test_schema"));
        let column_names = vec!["a".to_string(), "b".to_string()];
        let columns = vec![
            ColumnDesc::new(Int32Type { nullable: false }, true),
            ColumnDesc::new(BoolType { nullable: false }, false),
        ];
        let table_catalog =
            TableCatalog::new(0, "test_table".to_string(), column_names, columns, false);
        let table_id = schema_catalog
            .add_table(String::from("test_table"), table_catalog)
            .unwrap();
        assert_eq!(table_id, 0);

        let mut database_catalog = DatabaseCatalog::new(0, String::from("test_database"));

        let schema_id = database_catalog
            .add_schema(String::from("test_schema"), schema_catalog)
            .unwrap();
        assert_eq!(schema_id, 0);

        let schema_catalog = database_catalog.get_schema_by_id(schema_id).unwrap();
        let table_catalog = schema_catalog.get_table_by_id(table_id).unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let schema_catalog = database_catalog
            .get_schema_by_name(&String::from("test_schema"))
            .unwrap();
        let table_catalog = schema_catalog
            .get_table_by_name(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let schema_catalog = database_catalog.get_all_schemas();
        assert_eq!(schema_catalog.len(), 1);

        let schema_catalog = database_catalog.delete_schema("test_schema");
        assert_eq!(schema_catalog, Ok(()));

        let schema_catalog = database_catalog.delete_schema("test_schema");
        assert_eq!(
            schema_catalog,
            Err(String::from("Schema does not exist: test_schema"))
        );
    }
}
