use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use itertools::Itertools;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_analyze(&mut self, name: &ObjectName) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        let table_catalog = self.context.table_and_bind(table_name.clone(), None)?;
        let columns = table_catalog
            .columns()
            .filter_map(|column| column.desc.is_index().then_some(column.clone()))
            .collect_vec();

        let scan_op = ScanOperator::build(table_name.clone(), table_catalog);
        Ok(LogicalPlan::new(
            Operator::Analyze(AnalyzeOperator {
                table_name,
                columns,
            }),
            vec![scan_op],
        ))
    }
}
