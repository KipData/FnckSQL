use crate::catalog::{TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::storage::Bounds;
use crate::types::index::IndexMetaRef;
use itertools::Itertools;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct ScanOperator {
    pub index_metas: Vec<IndexMetaRef>,

    pub table_name: TableName,
    pub columns: Vec<ScalarExpression>,
    // Support push down limit.
    pub limit: Bounds,

    // IndexScan only
    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub index_by: Option<(IndexMetaRef, Vec<ConstantBinary>)>,
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
                index_metas: table_catalog.indexes.clone(),
                table_name,
                columns,

                limit: (None, None),
                index_by: None,
            }),
            childrens: vec![],
        }
    }
}
