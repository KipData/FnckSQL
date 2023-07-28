use std::collections::HashSet;
use anyhow::Result;
use sqlparser::ast::{ColumnDef, ObjectName};

use super::Binder;
use crate::binder::{lower_case_name, split_name};
use crate::catalog::ColumnCatalog;
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::operator::create_table::CreateOperator;

impl Binder {
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
    ) -> Result<LogicalCreateTablePlan> {
        let name = lower_case_name(&name);
        let (_, table_name) = split_name(&name)?;

        // check duplicated column names
        let mut set = HashSet::new();
        for col in columns.iter() {
            let col_name = &col.name.value;
            if !set.insert(col_name.clone()) {
                return Err(anyhow::Error::msg(format!(
                    "bind duplicated column {}",
                    col_name
                )));
            }
        }

        let columns: Vec<ColumnCatalog> = columns
            .iter()
            .map(|col| ColumnCatalog::from(col.clone()))
            .collect();

        let plan = LogicalCreateTablePlan {
            operator: CreateOperator {
                table_name: table_name.to_string(),
                columns
            },
        };
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::planner::LogicalPlan;
    use crate::types::LogicalType;

    #[test]
    fn test_create_bind() {
        let sql = "create table t1 (id int , name varchar(10))";
        let binder = Binder::new(BinderContext::new(RootCatalog::new()));
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let plan1 = binder.bind(&stmt[0]).unwrap();

        let plan2 = LogicalPlan::CreateTable(LogicalCreateTablePlan {
            operator: CreateOperator {
                table_name: "t1".to_string(),
                columns: vec![
                    ColumnCatalog::new(
                        "id".to_string(),
                        false,
                        ColumnDesc::new(LogicalType::Integer, false)
                    ),
                    ColumnCatalog::new(
                        "name".to_string(),
                        false,
                        ColumnDesc::new(LogicalType::Varchar, false)
                    )
                ],
            },
        });

        assert_eq!(plan1, plan2);
    }
}
