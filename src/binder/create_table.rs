use itertools::Itertools;
use sqlparser::ast::{ColumnDef, ColumnOption, ObjectName, TableConstraint};
use std::collections::HashSet;
use std::sync::Arc;

use super::{is_valid_identifier, Binder};
use crate::binder::lower_case_name;
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::LogicalType;

impl<T: Transaction> Binder<'_, '_, T> {
    // TODO: TableConstraint
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
        if_not_exists: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        if !is_valid_identifier(&table_name) {
            return Err(DatabaseError::InvalidTable(
                "illegal table naming".to_string(),
            ));
        }
        {
            // check duplicated column names
            let mut set = HashSet::new();
            for col in columns.iter() {
                let col_name = &col.name.value;
                if !set.insert(col_name) {
                    return Err(DatabaseError::DuplicateColumn(col_name.clone()));
                }
                if !is_valid_identifier(col_name) {
                    return Err(DatabaseError::InvalidColumn(
                        "illegal column naming".to_string(),
                    ));
                }
            }
        }
        let mut columns: Vec<ColumnCatalog> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| self.bind_column(col, Some(i)))
            .try_collect()?;
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    columns: column_names,
                    is_primary,
                    ..
                } => {
                    for (i, column_name) in column_names
                        .iter()
                        .map(|ident| ident.value.to_lowercase())
                        .enumerate()
                    {
                        if let Some(column) = columns
                            .iter_mut()
                            .find(|column| column.name() == column_name)
                        {
                            if *is_primary {
                                column.desc_mut().set_primary(Some(i));
                            } else {
                                column.desc_mut().set_unique(true);
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }

        if columns.iter().filter(|col| col.desc().is_primary()).count() == 0 {
            return Err(DatabaseError::InvalidTable(
                "the primary key field must exist and have at least one".to_string(),
            ));
        }

        Ok(LogicalPlan::new(
            Operator::CreateTable(CreateTableOperator {
                table_name,
                columns,
                if_not_exists,
            }),
            Childrens::None,
        ))
    }

    pub fn bind_column(
        &mut self,
        column_def: &ColumnDef,
        column_index: Option<usize>,
    ) -> Result<ColumnCatalog, DatabaseError> {
        let column_name = column_def.name.value.to_lowercase();
        let mut column_desc = ColumnDesc::new(
            LogicalType::try_from(column_def.data_type.clone())?,
            None,
            false,
            None,
        )?;
        let mut nullable = true;

        // TODO: 这里可以对更多字段可设置内容进行补充
        for option_def in &column_def.options {
            match &option_def.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Unique { is_primary, .. } => {
                    if *is_primary {
                        column_desc.set_primary(column_index);
                        nullable = false;
                        // Skip other options when using primary key
                        break;
                    } else {
                        column_desc.set_unique(true);
                    }
                }
                ColumnOption::Default(expr) => {
                    let mut expr = self.bind_expr(expr)?;

                    if !expr.referenced_columns(true).is_empty() {
                        return Err(DatabaseError::UnsupportedStmt(
                            "column is not allowed to exist in `default`".to_string(),
                        ));
                    }
                    if expr.return_type() != column_desc.column_datatype {
                        expr = ScalarExpression::TypeCast {
                            expr: Box::new(expr),
                            ty: column_desc.column_datatype.clone(),
                        }
                    }
                    column_desc.default = Some(expr);
                }
                _ => todo!(),
            }
        }

        Ok(ColumnCatalog::new(column_name, nullable, column_desc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::ColumnDesc;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use sqlparser::ast::CharLengthUnits;
    use std::cell::RefCell;
    use std::hash::RandomState;
    use std::sync::atomic::AtomicUsize;
    use tempfile::TempDir;

    #[test]
    fn test_create_bind() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let scala_functions = Default::default();
        let table_functions = Default::default();

        let sql = "create table t1 (id int primary key, name varchar(10) null)";
        let args = RefCell::new(Vec::new());
        let mut binder = Binder::new(
            BinderContext::new(
                &table_cache,
                &view_cache,
                &transaction,
                &scala_functions,
                &table_functions,
                Arc::new(AtomicUsize::new(0)),
            ),
            &args,
            None,
        );
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let plan1 = binder.bind(&stmt[0]).unwrap();

        match plan1.operator {
            Operator::CreateTable(op) => {
                assert_eq!(op.table_name, Arc::new("t1".to_string()));
                assert_eq!(op.columns[0].name(), "id");
                assert_eq!(op.columns[0].nullable(), false);
                assert_eq!(
                    op.columns[0].desc(),
                    &ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?
                );
                assert_eq!(op.columns[1].name(), "name");
                assert_eq!(op.columns[1].nullable(), true);
                assert_eq!(
                    op.columns[1].desc(),
                    &ColumnDesc::new(
                        LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
                        None,
                        false,
                        None
                    )?
                );
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
