use itertools::Itertools;
use crate::catalog::TableCatalog;
use crate::types::{ColumnId, TableId};
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;

use super::{sort::SortField, Operator};

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub table_id: TableId,
    pub columns: Vec<ScalarExpression>,
    pub sort_fields: Vec<SortField>,
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub pre_where: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Option<usize>,
}
impl ScanOperator {
    pub fn new(table_id: TableId, table_catalog: &TableCatalog) -> LogicalPlan {
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .all_columns()
            .into_iter()
            .map(|(_, col)| ScalarExpression::ColumnRef(col.clone()))
            .collect_vec();

        LogicalPlan {
            operator: Operator::Scan(ScanOperator {
                table_id,
                columns,
                sort_fields: vec![],
                pre_where: vec![],
                limit: None,
            }),
            childrens: vec![],
        }
    }
}
