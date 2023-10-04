use itertools::Itertools;
use sqlparser::ast::{ColumnDef, ObjectName, TableConstraint};
use std::collections::HashSet;
use std::sync::Arc;

use super::Binder;
use crate::binder::{lower_case_name, split_name, BindError};
use crate::catalog::ColumnCatalog;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Storage;

impl<S: Storage> Binder<S> {
    // TODO: TableConstraint
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
        _constraints: &[TableConstraint]
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        // check duplicated column names
        let mut set = HashSet::new();
        for col in columns.iter() {
            let col_name = &col.name.value;
            if !set.insert(col_name.clone()) {
                return Err(BindError::AmbiguousColumn(col_name.to_string()));
            }
        }
        let columns = columns
            .iter()
            .map(|col| ColumnCatalog::from(col.clone()))
            .collect_vec();

        let primary_key_count = columns.iter().filter(|col| col.desc.is_primary).count();

        if primary_key_count != 1 {
            return Err(BindError::InvalidTable(
                "The primary key field must exist and have at least one".to_string(),
            ));
        }

        let plan = LogicalPlan {
            operator: Operator::CreateTable(CreateTableOperator {
                table_name,
                columns,
            }),
            childrens: vec![],
        };
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::ColumnDesc;
    use crate::storage::kip::KipStorage;
    use crate::types::LogicalType;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_bind() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await.unwrap();

        let sql = "create table t1 (id int primary key, name varchar(10) null)";
        let binder = Binder::new(BinderContext::new(storage));
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let plan1 = binder.bind(&stmt[0]).await.unwrap();

        match plan1.operator {
            Operator::CreateTable(op) => {
                assert_eq!(op.table_name, Arc::new("t1".to_string()));
                assert_eq!(op.columns[0].name, "id".to_string());
                assert_eq!(op.columns[0].nullable, false);
                assert_eq!(
                    op.columns[0].desc,
                    ColumnDesc::new(LogicalType::Integer, true, false)
                );
                assert_eq!(op.columns[1].name, "name".to_string());
                assert_eq!(op.columns[1].nullable, true);
                assert_eq!(
                    op.columns[1].desc,
                    ColumnDesc::new(LogicalType::Varchar(Some(10)), false, false)
                );
            }
            _ => unreachable!(),
        }
    }
}
