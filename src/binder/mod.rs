pub mod aggregate;
mod alter_table;
mod analyze;
pub mod copy;
mod create_index;
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
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::catalog::{TableCatalog, TableName};
use crate::db::Functions;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::JoinType;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

pub enum InputRefType {
    AggCall,
    GroupBy,
}

pub enum CommandType {
    DQL,
    DML,
    DDL,
}

pub fn command_type(stmt: &Statement) -> Result<CommandType, DatabaseError> {
    match stmt {
        Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. } => Ok(CommandType::DDL),
        Statement::Query(_)
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. } => Ok(CommandType::DQL),
        Statement::Analyze { .. }
        | Statement::Truncate { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::Insert { .. }
        | Statement::Copy { .. } => Ok(CommandType::DML),
        stmt => Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
    }
}

// Tips: only query now!
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum QueryBindStep {
    From,
    Join,
    Where,
    Agg,
    Having,
    Distinct,
    Sort,
    Project,
    Limit,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum SubQueryType {
    SubQuery(LogicalPlan),
    InSubQuery(bool, LogicalPlan),
}

#[derive(Clone)]
pub struct BinderContext<'a, T: Transaction> {
    pub(crate) functions: &'a Functions,
    pub(crate) transaction: &'a T,
    // Tips: When there are multiple tables and Wildcard, use BTreeMap to ensure that the order of the output tables is certain.
    pub(crate) bind_table: BTreeMap<(TableName, Option<JoinType>), &'a TableCatalog>,
    // alias
    expr_aliases: HashMap<String, ScalarExpression>,
    table_aliases: HashMap<String, TableName>,
    // agg
    group_by_exprs: Vec<ScalarExpression>,
    pub(crate) agg_calls: Vec<ScalarExpression>,

    bind_step: QueryBindStep,
    sub_queries: HashMap<QueryBindStep, Vec<SubQueryType>>,

    temp_table_id: Arc<AtomicUsize>,
    pub(crate) allow_default: bool,
}

impl<'a, T: Transaction> BinderContext<'a, T> {
    pub fn new(
        transaction: &'a T,
        functions: &'a Functions,
        temp_table_id: Arc<AtomicUsize>,
    ) -> Self {
        BinderContext {
            functions,
            transaction,
            bind_table: Default::default(),
            expr_aliases: Default::default(),
            table_aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
            bind_step: QueryBindStep::From,
            sub_queries: Default::default(),
            temp_table_id,
            allow_default: false,
        }
    }

    pub fn temp_table(&mut self) -> TableName {
        Arc::new(format!(
            "_temp_table_{}_",
            self.temp_table_id.fetch_add(1, Ordering::SeqCst)
        ))
    }

    pub fn step(&mut self, bind_step: QueryBindStep) {
        self.bind_step = bind_step;
    }

    pub fn is_step(&self, bind_step: &QueryBindStep) -> bool {
        &self.bind_step == bind_step
    }

    pub fn step_now(&self) -> QueryBindStep {
        self.bind_step
    }

    pub fn sub_query(&mut self, sub_query: SubQueryType) {
        self.sub_queries
            .entry(self.bind_step)
            .or_default()
            .push(sub_query)
    }

    pub fn sub_queries_at_now(&mut self) -> Option<Vec<SubQueryType>> {
        self.sub_queries.remove(&self.bind_step)
    }

    pub fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
        if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(real_name.clone())
        } else {
            self.transaction.table(table_name)
        }
    }

    pub fn table_and_bind(
        &mut self,
        table_name: TableName,
        join_type: Option<JoinType>,
    ) -> Result<&TableCatalog, DatabaseError> {
        let table = if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(real_name.clone())
        } else {
            self.transaction.table(table_name.clone())
        }
        .ok_or(DatabaseError::TableNotFound)?;

        self.bind_table
            .insert((table_name.clone(), join_type), table);

        Ok(table)
    }

    /// get table from bindings
    pub fn bind_table(&self, table_name: &str) -> Result<&TableCatalog, DatabaseError> {
        let default_name = Arc::new(table_name.to_owned());
        let real_name = self.table_aliases.get(table_name).unwrap_or(&default_name);
        self.bind_table
            .iter()
            .find(|((t, _), _)| t == real_name)
            .ok_or(DatabaseError::InvalidTable(table_name.into()))
            .map(|v| *v.1)
    }

    // Tips: The order of this index is based on Aggregate being bound first.
    pub fn input_ref_index(&self, ty: InputRefType) -> usize {
        match ty {
            InputRefType::AggCall => self.agg_calls.len(),
            InputRefType::GroupBy => self.agg_calls.len() + self.group_by_exprs.len(),
        }
    }

    pub fn add_alias(&mut self, alias: String, expr: ScalarExpression) {
        self.expr_aliases.insert(alias, expr);
    }

    pub fn add_table_alias(&mut self, alias: String, table: TableName) {
        self.table_aliases.insert(alias.clone(), table.clone());
    }

    pub fn has_agg_call(&self, expr: &ScalarExpression) -> bool {
        self.group_by_exprs.contains(expr)
    }
}

pub struct Binder<'a, T: Transaction> {
    context: BinderContext<'a, T>,
    pub(crate) parent: Option<&'a Binder<'a, T>>,
}

impl<'a, T: Transaction> Binder<'a, T> {
    pub fn new(context: BinderContext<'a, T>, parent: Option<&'a Binder<'a, T>>) -> Self {
        Binder { context, parent }
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
                    self.bind_insert(table_name, columns, &values.rows, *overwrite)?
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
            Statement::CreateIndex {
                table_name,
                name,
                columns,
                if_not_exists,
                unique,
                ..
            } => self.bind_create_index(table_name, name, columns, *if_not_exists, *unique)?,
            _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
        };
        Ok(plan)
    }

    pub fn bind_set_expr(&mut self, set_expr: &SetExpr) -> Result<LogicalPlan, DatabaseError> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select(select, &[]),
            SetExpr::Query(query) => self.bind_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right),
            _ => todo!(),
        }
    }
}

fn lower_ident(ident: &Ident) -> String {
    ident.value.to_lowercase()
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> Result<String, DatabaseError> {
    if name.0.len() == 1 {
        return Ok(lower_ident(&name.0[0]));
    }
    Err(DatabaseError::InvalidTable(name.to_string()))
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
    use std::sync::atomic::AtomicUsize;
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
                ),
                ColumnCatalog::new(
                    "c2".to_string(),
                    false,
                    ColumnDesc::new(Integer, false, true, None),
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
                ),
                ColumnCatalog::new(
                    "c4".to_string(),
                    false,
                    ColumnDesc::new(Integer, false, false, None),
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
        let functions = Default::default();
        let mut binder = Binder::new(
            BinderContext::new(&transaction, &functions, Arc::new(AtomicUsize::new(0))),
            None,
        );
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
