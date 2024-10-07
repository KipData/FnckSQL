use crate::catalog::TableCatalog;
use crate::errors::DatabaseError;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::{SchemaRef, Tuple};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableFunction {
    pub(crate) args: Vec<ScalarExpression>,
    pub(crate) inner: Arc<dyn TableFunctionImpl>,
}

impl PartialEq for TableFunction {
    fn eq(&self, other: &Self) -> bool {
        self.summary() == other.summary()
    }
}

impl Eq for TableFunction {}

impl Hash for TableFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.summary().hash(state);
    }
}

#[typetag::serde(tag = "table")]
pub trait TableFunctionImpl: Debug + Send + Sync {
    fn eval(
        &self,
        args: &[ScalarExpression],
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>, DatabaseError>;

    fn summary(&self) -> &FunctionSummary;

    fn output_schema(&self) -> &SchemaRef;

    fn table(&self) -> &'static TableCatalog;
}

impl TableFunction {
    pub fn summary(&self) -> &FunctionSummary {
        self.inner.summary()
    }

    pub fn output_schema(&self) -> &SchemaRef {
        self.inner.output_schema()
    }

    pub fn table(&self) -> &'static TableCatalog {
        self.inner.table()
    }
}
