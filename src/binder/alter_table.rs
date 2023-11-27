use sqlparser::ast::{AlterTableOperation, ObjectName};

use std::sync::Arc;

use super::Binder;
use crate::binder::{lower_case_name, split_name, BindError};
use crate::catalog::ColumnCatalog;
use crate::planner::operator::alter_table::{AddColumn, AlterTableOperator};
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::Transaction;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> Result<LogicalPlan, BindError> {
        let table_name: Arc<String> = Arc::new(split_name(&lower_case_name(name))?.1.to_string());

        // we need convert ColumnDef to ColumnCatalog

        let plan = match operation {
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
            } => {
                if let Some(table) = self.context.table(&table_name) {
                    let plan = ScanOperator::new(table_name.clone(), table);
    
                    LogicalPlan {
                        operator: Operator::AlterTable(AlterTableOperator::AddColumn(AddColumn {
                            table_name,
                            if_not_exists: *if_not_exists,
                            column: ColumnCatalog::from(column_def.clone()),
                        })),
                        childrens: vec![plan],
                    }
                } else {
                    return Err(BindError::InvalidTable(format!(
                        "not found table {}",
                        table_name
                    )))
                }
            },
            AlterTableOperation::DropColumn {
                column_name: _,
                if_exists: _,
                cascade: _,
            } => todo!(),
            AlterTableOperation::DropPrimaryKey => todo!(),
            AlterTableOperation::RenameColumn {
                old_column_name: _,
                new_column_name: _,
            } => todo!(),
            AlterTableOperation::RenameTable { table_name: _ } => todo!(),
            AlterTableOperation::ChangeColumn {
                old_name: _,
                new_name: _,
                data_type: _,
                options: _,
            } => todo!(),
            AlterTableOperation::AlterColumn {
                column_name: _,
                op: _,
            } => todo!(),
            _ => todo!(),
        };

        Ok(plan)
    }
}
