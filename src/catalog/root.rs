use crate::catalog::{DatabaseCatalog, DatabaseCatalogRef};
use crate::types::DatabaseIdT;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct RootCatalog {
    /// Database name to database id mapping
    database_idxs: HashMap<String, DatabaseIdT>,
    /// Database id to database catalog mapping
    databases: BTreeMap<DatabaseIdT, DatabaseCatalogRef>,
    next_database_id: DatabaseIdT,
}

impl RootCatalog {
    pub(crate) fn add_database(
        &mut self,
        database_name: String,
        database_catalog: DatabaseCatalog,
    ) -> Result<DatabaseIdT, String> {
        if self.database_idxs.contains_key(&database_name) {
            Err(String::from("Duplicated database name!"))
        } else {
            let database_id = self.next_database_id;
            self.next_database_id += 1;
            let database_catalog = Arc::new(database_catalog);
            self.database_idxs.insert(database_name, database_id);
            self.databases.insert(database_id, database_catalog);
            Ok(database_id)
        }
    }

    pub(crate) fn delete_database(&mut self, database_name: &String) -> Result<(), String> {
        if self.database_idxs.contains_key(database_name) {
            let id = self.database_idxs.remove(database_name).unwrap();
            self.databases.remove(&id);
            Ok(())
        } else {
            Err(String::from("Database does not exist: ") + database_name)
        }
    }

    pub(crate) fn get_all_databases(&self) -> &BTreeMap<DatabaseIdT, DatabaseCatalogRef> {
        &self.databases
    }

    pub(crate) fn get_database_id_by_name(&self, name: &String) -> Option<DatabaseIdT> {
        self.database_idxs.get(name).cloned()
    }

    pub(crate) fn get_database_by_id(
        &self,
        database_id: DatabaseIdT,
    ) -> Option<DatabaseCatalogRef> {
        self.databases.get(&database_id).cloned()
    }

    pub(crate) fn get_database_by_name(&self, name: &String) -> Option<DatabaseCatalogRef> {
        match self.get_database_id_by_name(name) {
            Some(v) => self.get_database_by_id(v),
            None => None,
        }
    }

    pub(crate) fn new() -> RootCatalog {
        RootCatalog {
            database_idxs: HashMap::new(),
            databases: BTreeMap::new(),
            next_database_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDesc, SchemaCatalog, TableCatalog};
    use crate::types::{BoolType, DataTypeEnum, Int32Type};

    #[test]
    fn test_root_catalog() {
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
        database_catalog
            .add_schema(String::from("test_schema"), schema_catalog)
            .unwrap();

        let mut root_catalog = RootCatalog::new();
        let database_id = root_catalog
            .add_database(String::from("test_table"), database_catalog)
            .unwrap();
        assert_eq!(database_id, 0);

        let database_catalog = root_catalog.get_database_by_id(database_id).unwrap();
        let schema_catalog = database_catalog.get_schema_by_id(0).unwrap();
        let table_catalog = schema_catalog.get_table_by_id(0).unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let mut database_catalog = root_catalog
            .get_database_by_name(&String::from("test_table"))
            .unwrap();
        let schema_catalog = database_catalog
            .get_schema_by_name(&String::from("test_schema"))
            .unwrap();
        let table_catalog = schema_catalog
            .get_table_by_name(&String::from("test_table"))
            .unwrap();
        let column_catalog = table_catalog
            .get_column_by_name(&String::from("b"))
            .unwrap();
        assert_eq!(column_catalog.column_name(), "b");
        assert_eq!(
            column_catalog.column_datatype().get_type(),
            DataTypeEnum::Bool
        );
        assert_eq!(table_catalog.get_table_name(), "test_table");
    }
}
