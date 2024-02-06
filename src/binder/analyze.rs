use crate::binder::{lower_case_name, split_name, Binder};
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
        let name = lower_case_name(name);
        let name = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        let table_catalog = self.context.table_and_bind(table_name.clone(), None)?;
        let columns = table_catalog
            .columns_with_id()
            .filter_map(|(_, column)| column.desc.is_index().then_some(column.clone()))
            .collect_vec();

        let scan_op = ScanOperator::build(table_name.clone(), table_catalog);
        let plan = LogicalPlan {
            operator: Operator::Analyze(AnalyzeOperator {
                table_name,
                columns,
            }),
            childrens: vec![scan_op],
            physical_option: None,
        };
        Ok(plan)
    }
}
