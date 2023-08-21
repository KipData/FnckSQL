use crate::catalog::ColumnCatalog;
use crate::types::value::DataValue;

#[derive(Debug, PartialEq, Clone)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<DataValue>>,
    pub columns: Vec<ColumnCatalog>
}