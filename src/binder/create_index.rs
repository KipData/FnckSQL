use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::IndexType;
use sqlparser::ast::{ObjectName, OrderByExpr};
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_create_index(
        &mut self,
        table_name: &ObjectName,
        name: &ObjectName,
        exprs: &[OrderByExpr],
        if_not_exists: bool,
        is_unique: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(table_name)?);
        let index_name = lower_case_name(name)?;
        let ty = if is_unique {
            IndexType::Unique
        } else if exprs.len() == 1 {
            IndexType::Normal
        } else {
            IndexType::Composite
        };

        let table = self
            .context
            .table_and_bind(table_name.clone(), None, None)?;
        let plan = ScanOperator::build(table_name.clone(), table);
        let mut columns = Vec::with_capacity(exprs.len());

        for expr in exprs {
            // TODO: Expression Index
            match self.bind_expr(&expr.expr)? {
                ScalarExpression::ColumnRef(column) => columns.push(column),
                expr => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "'CREATE INDEX' by {}",
                        expr
                    )))
                }
            }
        }

        Ok(LogicalPlan::new(
            Operator::CreateIndex(CreateIndexOperator {
                table_name,
                columns,
                index_name,
                if_not_exists,
                ty,
            }),
            vec![plan],
        ))
    }
}
