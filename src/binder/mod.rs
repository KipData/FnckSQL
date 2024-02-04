pub mod aggregate;
mod alter_table;
mod analyze;
pub mod copy;
mod create_table;
mod delete;
mod describe;
mod distinct;
mod drop_table;
mod explain;
pub mod expr;
mod insert;
mod select;
mod show;
mod truncate;
mod update;

use sqlparser::ast::{Ident, ObjectName, ObjectType, SetExpr, Statement};
use std::collections::BTreeMap;

use crate::catalog::{TableCatalog, TableName};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::JoinType;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

pub enum InputRefType {
    AggCall,
    GroupBy,
}

#[derive(Clone)]
pub struct BinderContext<'a, T: Transaction> {
    transaction: &'a T,
    pub(crate) bind_table: BTreeMap<TableName, (TableCatalog, Option<JoinType>)>,
    aliases: BTreeMap<String, ScalarExpression>,
    table_aliases: BTreeMap<String, TableName>,
    group_by_exprs: Vec<ScalarExpression>,
    pub(crate) agg_calls: Vec<ScalarExpression>,
}

impl<'a, T: Transaction> BinderContext<'a, T> {
    pub fn new(transaction: &'a T) -> Self {
        BinderContext {
            transaction,
            bind_table: Default::default(),
            aliases: Default::default(),
            table_aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
        }
    }

    pub fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
        if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(real_name.clone())
        } else {
            self.transaction.table(table_name)
        }
    }

    // Tips: The order of this index is based on Aggregate being bound first.
    pub fn input_ref_index(&self, ty: InputRefType) -> usize {
        match ty {
            InputRefType::AggCall => self.agg_calls.len(),
            InputRefType::GroupBy => self.agg_calls.len() + self.group_by_exprs.len(),
        }
    }

    pub fn add_alias(
        &mut self,
        alias: String,
        expr: ScalarExpression,
    ) -> Result<(), DatabaseError> {
        let is_exist = self.aliases.insert(alias.clone(), expr).is_some();
        if is_exist {
            return Err(DatabaseError::InvalidColumn(format!(
                "{} duplicated",
                alias
            )));
        }

        Ok(())
    }

    pub fn add_table_alias(
        &mut self,
        alias: String,
        table: TableName,
    ) -> Result<(), DatabaseError> {
        let is_alias_exist = self
            .table_aliases
            .insert(alias.clone(), table.clone())
            .is_some();
        if is_alias_exist {
            return Err(DatabaseError::InvalidTable(format!("{} duplicated", alias)));
        }

        Ok(())
    }

    pub fn add_bind_table(
        &mut self,
        table: TableName,
        table_catalog: TableCatalog,
        join_type: Option<JoinType>,
    ) -> Result<(), DatabaseError> {
        let is_bound = self
            .bind_table
            .insert(table.clone(), (table_catalog.clone(), join_type))
            .is_some();
        if is_bound {
            return Err(DatabaseError::InvalidTable(format!("{} duplicated", table)));
        }

        Ok(())
    }

    pub fn has_agg_call(&self, expr: &ScalarExpression) -> bool {
        self.group_by_exprs.contains(expr)
    }
}

pub struct Binder<'a, T: Transaction> {
    context: BinderContext<'a, T>,
}

impl<'a, T: Transaction> Binder<'a, T> {
    pub fn new(context: BinderContext<'a, T>) -> Self {
        Binder { context }
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<LogicalPlan, DatabaseError> {
        let plan = match stmt {
            Statement::Query(query) => self.bind_query(query)?,
            Statement::AlterTable { name, operation } => self.bind_alter_table(name, operation)?,
            Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => self.bind_create_table(name, columns, constraints, *if_not_exists)?,
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => match object_type {
                ObjectType::Table => self.bind_drop_table(&names[0], if_exists)?,
                _ => todo!(),
            },
            Statement::Insert {
                table_name,
                columns,
                source,
                overwrite,
                ..
            } => {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.bind_insert(table_name.to_owned(), columns, &values.rows, *overwrite)?
                } else {
                    todo!()
                }
            }
            Statement::Update {
                table,
                selection,
                assignments,
                ..
            } => {
                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_update(table, selection, assignments)?
                }
            }
            Statement::Delete {
                from, selection, ..
            } => {
                let table = &from[0];

                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_delete(table, selection)?
                }
            }
            Statement::Analyze { table_name, .. } => self.bind_analyze(table_name)?,
            Statement::Truncate { table_name, .. } => self.bind_truncate(table_name)?,
            Statement::ShowTables { .. } => self.bind_show_tables()?,
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source.clone(), *to, target.clone(), options)?,
            Statement::Explain { statement, .. } => {
                let plan = self.bind(statement)?;

                self.bind_explain(plan)?
            }
            Statement::ExplainTable {
                describe_alias: true,
                table_name,
            } => self.bind_describe(table_name)?,
            _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
        };
        Ok(plan)
    }
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect(),
    )
}

/// Split an object name into `(schema name, table name)`.
fn split_name(name: &ObjectName) -> Result<&str, DatabaseError> {
    Ok(match name.0.as_slice() {
        [table] => &table.value,
        _ => return Err(DatabaseError::InvalidTable(name.to_string())),
    })
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !s.chars().next().unwrap_or_default().is_numeric()
        && !s.chars().all(|c| c == '_')
}

#[cfg(test)]
pub mod test {
    use crate::binder::{is_valid_identifier, Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::planner::LogicalPlan;
    use crate::storage::kip::KipStorage;
    use crate::storage::{Storage, Transaction};
    use crate::types::LogicalType::Integer;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) async fn build_test_catalog(
        path: impl Into<PathBuf> + Send,
    ) -> Result<KipStorage, DatabaseError> {
        let storage = KipStorage::new(path).await?;
        let mut transaction = storage.transaction().await?;

        let _ = transaction.create_table(
            Arc::new("t1".to_string()),
            vec![
                ColumnCatalog::new(
                    "c1".to_string(),
                    false,
                    ColumnDesc::new(Integer, true, false, None),
                    None,
                ),
                ColumnCatalog::new(
                    "c2".to_string(),
                    false,
                    ColumnDesc::new(Integer, false, true, None),
                    None,
                ),
            ],
            false,
        )?;

        let _ = transaction.create_table(
            Arc::new("t2".to_string()),
            vec![
                ColumnCatalog::new(
                    "c3".to_string(),
                    false,
                    ColumnDesc::new(Integer, true, false, None),
                    None,
                ),
                ColumnCatalog::new(
                    "c4".to_string(),
                    false,
                    ColumnDesc::new(Integer, false, false, None),
                    None,
                ),
            ],
            false,
        )?;

        transaction.commit().await?;

        Ok(storage)
    }

    pub async fn select_sql_run<S: AsRef<str>>(sql: S) -> Result<LogicalPlan, DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = build_test_catalog(temp_dir.path()).await?;
        let transaction = storage.transaction().await?;
        let mut binder = Binder::new(BinderContext::new(&transaction));
        let stmt = crate::parser::parse_sql(sql)?;

        Ok(binder.bind(&stmt[0])?)
    }

    #[test]
    pub fn test_valid_identifier() {
        assert!(is_valid_identifier("valid_table"));
        assert!(is_valid_identifier("valid_column"));
        assert!(is_valid_identifier("_valid_column"));
        assert!(is_valid_identifier("valid_column_1"));

        assert!(!is_valid_identifier("invalid_name&"));
        assert!(!is_valid_identifier("1_invalid_name"));
        assert!(!is_valid_identifier("____"));
    }
}
