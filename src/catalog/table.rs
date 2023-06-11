use crate::catalog::{CatalogError, ColumnCatalog};
use crate::types::{ColumnIdT, TableIdT};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

pub(crate) struct TableCatalog {
    table_id: TableIdT,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnIdT>,
    /// Mapping from column ids to column catalogs
    columns: BTreeMap<ColumnIdT, ColumnCatalog>,

    #[allow(dead_code)]
    /// The next column id to be assigned
    is_materialized_view: bool,
    /// Whether the table is a materialized view
    next_column_id: ColumnIdT,
}

impl TableCatalog {
    /// Create a new table catalog with the given table id and table name.
    pub(crate) fn new(
        table_id: TableIdT,
        table_name: String,
        columns: Vec<ColumnCatalog>,
        is_materialized_view: bool,
    ) -> TableCatalog {
        let table_catalog = TableCatalog {
            table_id,
            inner: Mutex::new(Inner {
                name: table_name.clone(),
                column_idxs: HashMap::new(),
                columns: BTreeMap::new(),
                is_materialized_view,
                next_column_id: 0,
            }),
        };
        for col_catalog in columns.into_iter() {
            table_catalog.add_column(col_catalog).unwrap();
        }
        table_catalog
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(&self, col_catalog: ColumnCatalog) -> Result<ColumnIdT, CatalogError> {
        let mut inner = self.inner.lock().unwrap();

        if inner.column_idxs.contains_key(col_catalog.name()) {
            return Err(CatalogError::Duplicated(
                "column",
                col_catalog.name().into(),
            ));
        }
        inner.next_column_id += 1;
        let id = col_catalog.id();

        inner
            .column_idxs
            .insert(col_catalog.name().to_string(), col_catalog.id());
        inner.columns.insert(id, col_catalog);
        Ok(id)
    }

    /// Check if the table catalog contains a column with the given name.
    pub(crate) fn contains_column(&self, name: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.contains_key(name)
    }

    /// Get all columns in the table catalog.
    pub(crate) fn get_all_columns(&self) -> BTreeMap<ColumnIdT, ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.clone()
    }

    /// Get the column id of the column with the given name.
    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<ColumnIdT> {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.get(name).cloned()
    }

    /// Get the column catalog of the column with the given id.
    pub(crate) fn get_column_by_id(&self, column_id: ColumnIdT) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.get(&column_id).cloned()
    }

    /// Get the column catalog of the column with the given name.
    pub(crate) fn get_column_by_name(&self, name: &String) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        let column_id = inner.column_idxs.get(name)?;
        inner.columns.get(column_id).cloned()
    }

    /// Get the table id of the table.
    pub(crate) fn id(&self) -> TableIdT {
        self.table_id
    }

    /// Get the table name of the table.
    pub(crate) fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }
}

mod tests {
    use super::*;
    use crate::types::{DataType, DataTypeExt, DataTypeKind};

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(
            0,
            "a".into(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = ColumnCatalog::new(1, "b".into(), DataTypeKind::Boolean.not_null().to_column());
        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, "test".to_string(), col_catalogs, false);

        assert_eq!(table_catalog.contains_column("a"), true);
        assert_eq!(table_catalog.contains_column("b"), true);
        assert_eq!(table_catalog.contains_column("c"), false);

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));

        let column_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(column_catalog.name(), "a");
        assert_eq!(
            column_catalog.datatype(),
            DataType::new(DataTypeKind::Int(None), false)
        );

        let column_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(column_catalog.name(), "b");
        assert_eq!(
            column_catalog.datatype(),
            DataType::new(DataTypeKind::Boolean, false)
        );
    }
}
