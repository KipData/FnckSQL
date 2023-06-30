use super::Binder;
use crate::binder::{lower_case_name, split_name};
use crate::catalog::{Column, ColumnDesc};
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::LogicalPlan;
use crate::types::ColumnId;
use anyhow::Result;
use sqlparser::ast::{ColumnDef, ObjectName};
use std::collections::HashSet;

impl Binder {
    pub(super) fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: &[ColumnDef],
    ) -> Result<LogicalCreateTablePlan> {
        let name = lower_case_name(&name);

        let (_, table_name) = split_name(&name)?;

        let table = self
            .context
            .catalog
            .get_table_by_name(table_name)
            .ok_or_else(|| {
                anyhow::Error::msg(format!("table {} not found", table_name.to_string()))
            })?;

        // check duplicated column names
        let mut set = HashSet::new();
        for col in columns.iter() {
            if !set.insert(col.name.value.clone()) {
                return Err(anyhow::Error::msg(format!(
                    "bind duplicated column {}",
                    col.name.value.clone()
                )));
            }
        }

        let mut columns: Vec<Column> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let mut col = Column::from(col);
                col
            })
            .collect();

        let plan = LogicalCreateTablePlan {
            table_name: table_name.to_string(),
            columns: columns
                .into_iter()
                .map(|col| (col.name.to_string(), col.desc.clone()))
                .collect(),
        };
        Ok(plan)
    }
}
