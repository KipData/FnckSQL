use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(crate) fn bind_analyze(&mut self, name: &ObjectName) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        let table_catalog = self
            .context
            .table_and_bind(table_name.clone(), None, None)?;
        let index_metas = table_catalog.indexes.clone();

        let scan_op = TableScanOperator::build(table_name.clone(), table_catalog);
        Ok(LogicalPlan::new(
            Operator::Analyze(AnalyzeOperator {
                table_name,
                index_metas,
            }),
            vec![scan_op],
        ))
    }
}
