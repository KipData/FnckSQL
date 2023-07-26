use std::collections::BTreeMap;

use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog};
use crate::types::{IdGenerator, TableIdx};

#[derive(Debug, Clone)]
pub struct RootCatalog {
    generator: IdGenerator,
    pub table_idxs: BTreeMap<String, TableIdx>,
    pub tables: Vec<TableCatalog>,
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
            generator: IdGenerator::new(),
            table_idxs: Default::default(),
            tables: Default::default(),
        }
    }

    pub(crate) fn get_table_id_by_name(&self, name: &str) -> Option<TableIdx> {
        self.table_idxs.get(name).cloned()
    }

    pub(crate) fn get_table(&self, table_id: TableIdx) -> Option<&TableCatalog> {
        self.tables.get(table_id)
    }

    pub(crate) fn get_table_by_name(&self, name: &str) -> Option<&TableCatalog> {
        let id = self.table_idxs.get(name)?;
        self.tables.get(*id)
    }

    pub(crate) fn add_table(
        &mut self,
        table_name: String,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableIdx, CatalogError> {
        if self.table_idxs.contains_key(&table_name) {
            return Err(CatalogError::Duplicated("column", table_name));
        }
        let mut table = TableCatalog::new(table_name.to_owned(), columns)?;
        let table_id = self.generator.build();

        table.id = Some(table_id);
        self.table_idxs.insert(table_name, table_id);
        self.tables.insert(table_id, table);

        Ok(table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::LogicalType;

    #[test]
    fn test_root_catalog() {
        let mut root_catalog = RootCatalog::new();

        let col0 = ColumnCatalog::new(
            "a".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, false),
        );
        let col1 = ColumnCatalog::new(
            "b".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, false),
        );
        let col_catalogs = vec![col0, col1];

        let table_id_1 = root_catalog
            .add_table("test_table_1".into(), col_catalogs.clone())
            .unwrap();

        let table_id_2 = root_catalog
            .add_table("test_table_2".into(), col_catalogs)
            .unwrap();

        assert_ne!(table_id_1, table_id_2);
    }
}
