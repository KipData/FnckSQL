use std::collections::BTreeMap;

use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableName};

#[derive(Debug, Clone)]
pub struct RootCatalog {
    table_idxs: BTreeMap<TableName, TableCatalog>,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    #[allow(dead_code)]
    pub fn new() -> Self {
        RootCatalog {
            table_idxs: Default::default(),
        }
    }

    pub(crate) fn get_table(&self, name: &String) -> Option<&TableCatalog> {
        self.table_idxs.get(name)
    }

    pub(crate) fn add_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableName, CatalogError> {
        if self.table_idxs.contains_key(&table_name) {
            return Err(CatalogError::Duplicated("column", table_name.to_string()));
        }
        let table = TableCatalog::new(table_name.clone(), columns)?;

        self.table_idxs.insert(table_name.clone(), table);

        Ok(table_name)
    }

    pub(crate) fn drop_table(&mut self, table_name: &String) -> Result<(), CatalogError> {
        self.table_idxs
            .retain(|name, _| name.as_str() != table_name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::LogicalType;
    use std::sync::Arc;

    #[test]
    fn test_root_catalog() {
        let mut root_catalog = RootCatalog::new();

        let col0 = ColumnCatalog::new(
            "a".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, false, false),
            None,
        );
        let col1 = ColumnCatalog::new(
            "b".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, false, false),
            None,
        );
        let col_catalogs = vec![col0, col1];

        let table_id_1 = root_catalog
            .add_table(Arc::new("test_table_1".to_string()), col_catalogs.clone())
            .unwrap();

        let table_id_2 = root_catalog
            .add_table(Arc::new("test_table_2".to_string()), col_catalogs)
            .unwrap();

        assert_ne!(table_id_1, table_id_2);
    }
}
