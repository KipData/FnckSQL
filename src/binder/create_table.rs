use std::collections::HashSet;
use sqlparser::ast::{ColumnDef, ObjectName};

use super::Binder;
use crate::binder::{BindError, lower_case_name, split_name};
use crate::catalog::ColumnCatalog;
use crate::planner::LogicalPlan;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::Operator;

impl Binder {
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, table_name) = split_name(&name)?;

        // check duplicated column names
        let mut set = HashSet::new();
        for col in columns.iter() {
            let col_name = &col.name.value;
            if !set.insert(col_name.clone()) {
                return Err(BindError::AmbiguousColumn(col_name.to_string()));
            }
        }

        let columns: Vec<ColumnCatalog> = columns
            .iter()
            .map(|col| ColumnCatalog::from(col.clone()))
            .collect();

        let plan = LogicalPlan {
            operator: Operator::CreateTable(
                CreateTableOperator {
                    table_name: table_name.to_string(),
                    columns
                }
            ),
            childrens: vec![],
        };
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::{ColumnDesc, RootCatalog};
    use crate::types::LogicalType;

    #[test]
    fn test_create_bind() {
        let sql = "create table t1 (id int , name varchar(10) null)";
        let binder = Binder::new(BinderContext::new(RootCatalog::new()));
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let plan1 = binder.bind(&stmt[0]).unwrap();

        match plan1.operator {
            Operator::CreateTable(op) => {
                assert_eq!(op.table_name, "t1".to_string());
                assert_eq!(op.columns[0].name, "id".to_string());
                assert_eq!(op.columns[0].nullable, false);
                assert_eq!(op.columns[0].desc, ColumnDesc::new(LogicalType::Integer, false));
                assert_eq!(op.columns[1].name, "name".to_string());
                assert_eq!(op.columns[1].nullable, true);
                assert_eq!(op.columns[1].desc, ColumnDesc::new(LogicalType::Varchar, false));
            }
            _ => unreachable!()
        }

    }
}
