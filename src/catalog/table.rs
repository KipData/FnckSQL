use std::collections::{BTreeMap, HashMap};
use itertools::Itertools;
use snowflake::ProcessUniqueId;
use crate::catalog::{CatalogError, Column};
use crate::types::{ColumnId, TableId};

pub struct Table {
    pub id: TableId,
    pub name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, Column>,
}

impl Table {
    pub(crate) fn get_all_columns(&self) -> Vec<(ColumnId, &Column)> {
        self.columns.iter()
            .map(|(col_id, col)| (*col_id, col))
            .collect_vec()
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(&mut self, col_catalog: Column) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(&col_catalog.name) {
            return Err(CatalogError::Duplicated(
                "column",
                col_catalog.name.into(),
            ));
        }

        let col_id = col_catalog.id;

        self.column_idxs.insert(col_catalog.name.to_owned(), col_id);
        self.columns.insert(col_id, col_catalog);

        Ok(col_id)
    }

    pub(crate) fn new(table_name: String, columns: Vec<Column>) -> Result<Table, CatalogError> {
        let mut table_catalog = Table {
            id: ProcessUniqueId::new(),
            name: table_name,
            column_idxs: HashMap::new(),
            columns: BTreeMap::new(),
        };

        for col_catalog in columns.into_iter() {
            let _ = table_catalog.add_column(col_catalog)?;
        }

        Ok(table_catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, DataTypeExt, DataTypeKind};

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |
    fn test_table_catalog() {
        let col0 = Column::new(
            "a".into(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = Column::new(
            "b".into(),
            DataTypeKind::Boolean.not_null().to_column()
        );
        let col_catalogs = vec![col0, col1];
        let table_catalog = Table::new(
            "test".to_string(),
            col_catalogs
        );

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
