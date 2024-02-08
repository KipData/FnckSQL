use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::planner::LogicalPlan;
use crate::storage::Bounds;
use crate::types::index::IndexInfo;
use crate::types::ColumnId;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

use super::Operator;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ScanOperator {
    pub table_name: TableName,
    pub primary_key: ColumnId,
    pub columns: Vec<(usize, ColumnRef)>,
    // Support push down limit.
    pub limit: Bounds,

    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub index_infos: Vec<IndexInfo>,
}
impl ScanOperator {
    pub fn build(table_name: TableName, table_catalog: &TableCatalog) -> LogicalPlan {
        let mut primary_key_option = None;
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .columns_with_id()
            .enumerate()
            .map(|(i, (_, column))| {
                if column.desc.is_primary {
                    primary_key_option = column.id();
                }

                (i, column.clone())
            })
            .collect_vec();
        let index_infos = table_catalog
            .indexes
            .iter()
            .map(|meta| IndexInfo {
                meta: meta.clone(),
                binaries: None,
            })
            .collect_vec();

        LogicalPlan {
            operator: Operator::Scan(ScanOperator {
                index_infos,
                table_name,
                primary_key: primary_key_option.unwrap(),
                columns,
                limit: (None, None),
            }),
            childrens: vec![],
            physical_option: None,
        }
    }
}

impl fmt::Display for ScanOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let projection_columns = self
            .columns
            .iter()
            .map(|(_, column)| column.name().to_string())
            .join(", ");
        let (offset, limit) = self.limit;

        write!(f, "Scan {} -> [{}]", self.table_name, projection_columns)?;
        if let Some(limit) = limit {
            write!(f, ", Limit: {}", limit)?;
        }
        if let Some(offset) = offset {
            write!(f, ", Offset: {}", offset)?;
        }

        Ok(())
    }
}
