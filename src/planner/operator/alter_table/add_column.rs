use crate::catalog::{ColumnCatalog, TableName};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct AddColumnOperator {
    pub table_name: TableName,
    pub if_not_exists: bool,
    pub column: ColumnCatalog,
}
