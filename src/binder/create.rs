use super::Binder;
use crate::binder::{lower_case_name, split_name};
use crate::catalog::{Column, ColumnDesc};
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::LogicalPlan;
use crate::types::{ColumnId, TableId};
use anyhow::Result;
use sqlparser::ast::{ColumnDef, ObjectName};
use std::collections::HashSet;

impl Binder {
    pub(crate) fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: &[ColumnDef],
    ) -> Result<LogicalCreateTablePlan> {
        let name = lower_case_name(&name);

        let (_, table_name) = split_name(&name)?;

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
            .map(|(_, col)| Column::from(col))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::Root;
    use crate::types::{DataTypeExt, DataTypeKind};
    use sqlparser::ast::CharacterLength;
    use std::sync::Arc;

    #[test]
    fn test_create_bind() {
        let sql = "create table t1 (id int , name varchar(10))";
        let mut binder = Binder::new(BinderContext::new(Root::new()));
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let plan1 = binder.bind(&stmt[0]).unwrap();

        let character_length = CharacterLength {
            length: 10,
            unit: None,
        };
        let plan2 = LogicalPlan::CreateTable(LogicalCreateTablePlan {
            table_name: "t1".to_string(),
            columns: vec![
                (
                    "id".to_string(),
                    DataTypeKind::Int(None).nullable().to_column(),
                ),
                (
                    "name".to_string(),
                    DataTypeKind::Varchar(Option::from(character_length))
                        .nullable()
                        .to_column(),
                ),
            ],
        });

        assert_eq!(plan1, plan2);
    }
}
