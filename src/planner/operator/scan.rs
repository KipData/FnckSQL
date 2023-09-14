use itertools::Itertools;
use crate::catalog::{TableCatalog, TableName};
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::storage::Bounds;

use super::{sort::SortField, Operator};

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub table_name: TableName,
    pub columns: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Bounds,

    // IndexScan only
    pub sort_fields: Vec<SortField>,
    // IndexScan only
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub pre_where: Vec<ScalarExpression>,
}
impl ScanOperator {
    pub fn new(table_name: TableName, table_catalog: &TableCatalog) -> LogicalPlan {
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();

        LogicalPlan {
            operator: Operator::Scan(ScanOperator {
                table_name,
                columns,
                sort_fields: vec![],
                pre_where: vec![],
                limit: (None, None),
            }),
            childrens: vec![],
        }
    }
}
