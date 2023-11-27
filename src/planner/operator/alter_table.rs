use crate::catalog::{ColumnCatalog, TableName};

#[derive(Debug, PartialEq, Clone)]
pub enum AlterTableOperator {
    AddColumn(AddColumn),
    DropColumn,
    DropPrimaryKey,
    RenameColumn,
    RenameTable,
    ChangeColumn,
    AlterColumn,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AddColumn {
    pub table_name: TableName,
    pub if_not_exists: bool,
    pub column: ColumnCatalog,
}
