use sqlparser::ast::{AlterTableOperation, ObjectName};

use std::sync::Arc;

use super::{is_valid_identifier, Binder};
use crate::binder::lower_case_name;
use crate::errors::DatabaseError;
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;

impl<T: Transaction> Binder<'_, '_, T> {
    pub(crate) fn bind_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: Arc<String> = Arc::new(lower_case_name(name)?);
        let table = self
            .context
            .table(table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let plan = match operation {
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table);
                let column = self.bind_column(column_def, None)?;

                if !is_valid_identifier(column.name()) {
                    return Err(DatabaseError::InvalidColumn(
                        "illegal column naming".to_string(),
                    ));
                }
                LogicalPlan::new(
                    Operator::AddColumn(AddColumnOperator {
                        table_name,
                        if_not_exists: *if_not_exists,
                        column,
                    }),
                    Childrens::Only(plan),
                )
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                ..
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table);
                let column_name = column_name.value.clone();

                LogicalPlan::new(
                    Operator::DropColumn(DropColumnOperator {
                        table_name,
                        if_exists: *if_exists,
                        column_name,
                    }),
                    Childrens::Only(plan),
                )
            }
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
