use crate::catalog::{CatalogError, Column, Table};
use crate::types::TableId;
use std::collections::BTreeMap;

pub struct Root {
    table_idxs: BTreeMap<String, TableId>,
    tables: BTreeMap<TableId, Table>,
}

impl Root {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Root { table_idxs: Default::default(), tables: Default::default() }
    }

    pub(crate) fn get_table_id_by_name(&self, name: &str) -> Option<TableId> {
        self.table_idxs.get(name).cloned()
    }

    pub(crate) fn get_table(&self, table_id: TableId) -> Option<&Table> {
        self.tables.get(&table_id)
    }

    pub(crate) fn add_table(&mut self, table_name: String, columns: Vec<Column>) -> Result<TableId, CatalogError> {
        if self.table_idxs.contains_key(&table_name) {
            return Err(CatalogError::Duplicated("column", table_name));
        }
        let table = Table::new(table_name.to_owned(), columns)?;
        let table_id = table.id;

        self.table_idxs.insert(table_name, table_id);
        self.tables.insert(table_id, table);

        Ok(table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_root_catalog() {
        let mut root_catalog = Root::new();

        let col0 = Column::new(
            "a".to_string(),
            DataTypeKind::Int(None).not_null().to_column(),
        );
        let col1 = Column::new(
            "b".to_string(),
            DataTypeKind::Boolean.not_null().to_column(),
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
