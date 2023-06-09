use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::catalog::{TableCatalog, TableCatalogRef};
use crate::types::{SchemaIdT, TableIdT};

pub(crate) struct SchemaCatalog {
    schema_id: SchemaIdT,
    schema_name: String,
    table_idxs: HashMap<String, TableIdT>,
    tables: BTreeMap<TableIdT, TableCatalogRef>,
    next_table_id: TableIdT,
}

impl SchemaCatalog {
    pub(crate) fn new(schema_id: SchemaIdT, schema_name: String) -> SchemaCatalog {
        SchemaCatalog {
            schema_id,
            schema_name,
            table_idxs: HashMap::new(),
            tables: BTreeMap::new(),
            next_table_id: 0,
        }
    }

    pub(crate) fn add_table(
        &mut self,
        table_name: String,
        table_catalog: TableCatalog,
    ) -> Result<TableIdT, String> {
        if self.table_idxs.contains_key(&table_name) {
            Err(String::from("Duplicated table name!"))
        } else {
            let table_id = self.next_table_id;
            self.next_table_id += 1;
            let table_catalog = Arc::new(table_catalog);
            self.table_idxs.insert(table_name, table_id);
            self.tables.insert(table_id, table_catalog);
            Ok(table_id)
        }
    }

    pub(crate) fn delete_table(&mut self, table_name: &String) -> Result<(), String> {
        if self.table_idxs.contains_key(table_name) {
            let id = self.table_idxs.remove(table_name).unwrap();
            self.tables.remove(&id);
            Ok(())
        } else {
            Err(String::from("Table does not exist: ") + table_name)
        }
    }

    pub(crate) fn get_all_tables(&self) -> &BTreeMap<TableIdT, TableCatalogRef> {
        &self.tables
    }

    pub(crate) fn get_table_id_by_name(&self, name: &String) -> Option<TableIdT> {
        self.table_idxs.get(name).cloned()
    }

    pub(crate) fn get_table_by_id(&self, table_id: TableIdT) -> Option<TableCatalogRef> {
        self.tables.get(&table_id).cloned()
    }

    pub(crate) fn get_table_by_name(&self, name: &String) -> Option<TableCatalogRef> {
        match self.get_table_id_by_name(name) {
            Some(v) => self.get_table_by_id(v),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDesc;
    use crate::types::{BoolType, Int32Type};

    #[test]
    fn test_schema_catalog() {
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

        let table_catalog = schema_catalog.get_table_by_id(table_id).unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let table_catalog = schema_catalog.get_all_tables().get(&table_id).unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let table_catalog = schema_catalog
            .get_table_by_name(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog.get_table_name(), "test_table");

        let table_catalog = schema_catalog
            .delete_table(&String::from("test_table"))
            .unwrap();
        assert_eq!(table_catalog, ());

        let table_catalog = schema_catalog.delete_table(&String::from("test_table"));
        assert_eq!(
            table_catalog,
            Err(String::from("Table does not exist: test_table"))
        );
    }
}
