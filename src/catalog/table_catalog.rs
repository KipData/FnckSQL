use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::ColumnCatalogRef;
use crate::types::{ColumnIdT, TableIdT};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) struct TableCatalog {
    table_id: TableIdT,
    table_name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnIdT>,
    /// Mapping from column ids to column catalogs
    columns: BTreeMap<ColumnIdT, ColumnCatalogRef>,
    /// The next column id to be assigned
    next_column_id: ColumnIdT,
    /// Whether the table is a materialized view
    is_materialized_view: bool,
}

impl TableCatalog {
    /// Create a new table catalog with the given table id and table name.
    pub(crate) fn new(
        table_id: TableIdT,
        table_name: String,
        column_names: Vec<String>,
        columns: Vec<ColumnDesc>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        let mut table_catalog = TableCatalog {
            table_id,
            table_name,
            column_idxs: HashMap::new(),
            columns: BTreeMap::new(),
            next_column_id: 0,
            is_materialized_view,
        };
        for (name, desc) in column_names.into_iter().zip(columns.into_iter()) {
            table_catalog.add_column(name, desc).unwrap_or_default();
        }
        table_catalog
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(
        &mut self,
        column_name: String,
        column_desc: ColumnDesc,
    ) -> Result<ColumnIdT, String> {
        if self.column_idxs.contains_key(&column_name) {
            Err(String::from("Duplicated column names!"))
        } else {
            let column_id = self.next_column_id;
            self.next_column_id += 1;

            let column_catalog = Arc::new(ColumnCatalog::new(
                column_id,
                column_name.clone(),
                column_desc,
            ));
            self.column_idxs.insert(column_name, column_id);
            self.columns.insert(column_id, column_catalog);
            Ok(column_id)
        }
    }

    /// Check if the table catalog contains a column with the given name.
    pub(crate) fn contains_column(&self, name: &String) -> bool {
        self.column_idxs.contains_key(name)
    }

    /// Get all columns in the table catalog.
    pub(crate) fn get_all_columns(&self) -> &BTreeMap<ColumnIdT, ColumnCatalogRef> {
        &self.columns
    }

    /// Get the column id of the column with the given name.
    pub(crate) fn get_column_id_by_name(&self, name: &String) -> Option<ColumnIdT> {
        match self.column_idxs.get(name) {
            Some(v) => Some(*v),
            None => None,
        }
    }

    /// Get the column catalog of the column with the given id.
    pub(crate) fn get_column_by_id(&self, column_id: ColumnIdT) -> Option<ColumnCatalogRef> {
        match self.columns.get(&column_id) {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }

    /// Get the column catalog of the column with the given name.
    pub(crate) fn get_column_by_name(&self, name: &String) -> Option<ColumnCatalogRef> {
        match self.get_column_id_by_name(name) {
            Some(id) => self.get_column_by_id(id),
            None => None,
        }
    }

    /// Get the table id of the table.
    pub(crate) fn get_table_id(&self) -> TableIdT {
        self.table_id
    }

    /// Get the table name of the table.
    pub(crate) fn get_table_name(&self) -> &String {
        &self.table_name
    }
}

mod tests {
    use super::*;
    use crate::types::{BoolType, DataTypeEnum, Int32Type};

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |

    fn test_table_catalog() {
        let column_names = vec!["a".to_string(), "b".to_string()];
        let columns = vec![
            ColumnDesc::new(Int32Type::new(false), true),
            ColumnDesc::new(BoolType::new(false), false),
        ];
        let table_catalog = TableCatalog::new(0, "test".to_string(), column_names, columns, false);

        assert_eq!(table_catalog.contains_column(&"a".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"b".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"c".to_string()), false);

        assert_eq!(
            table_catalog
                .get_column_id_by_name(&String::from("a"))
                .unwrap(),
            0
        );
        assert_eq!(
            table_catalog
                .get_column_id_by_name(&String::from("b"))
                .unwrap(),
            1
        );

        let column_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(column_catalog.get_column_name(), "a".to_string());
        assert_eq!(
            column_catalog.get_column_datatype().as_ref().get_type(),
            DataTypeEnum::Int32
        );

        let column_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(column_catalog.get_column_name(), "b".to_string());
        assert_eq!(
            column_catalog.get_column_datatype().as_ref().get_type(),
            DataTypeEnum::Bool
        );
    }
}
