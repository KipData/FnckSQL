use crate::catalog::TableName;
use crate::types::index::IndexMetaRef;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct AnalyzeOperator {
    pub table_name: TableName,
    pub index_metas: Vec<IndexMetaRef>,
}
