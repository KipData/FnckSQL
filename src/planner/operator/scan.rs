use std::sync::Arc;

use crate::types::TableIdx;
use crate::{
    catalog::ColumnRefId, expression::ScalarExpression,
    planner::logical_select_plan::LogicalSelectPlan,
};

use super::{sort::SortField, Operator};

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub table_ref_id: TableIdx,
    pub columns: Vec<ColumnRefId>,
    pub sort_fields: Vec<SortField>,
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub pre_where: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Option<usize>,
}
impl ScanOperator {
    pub fn new(table_ref_id: TableIdx) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Scan(ScanOperator {
                table_ref_id,
                columns: vec![],
                sort_fields: vec![],
                pre_where: vec![],
                limit: None,
            })),
            children: vec![],
        }
    }
}
