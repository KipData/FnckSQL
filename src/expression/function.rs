use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// for `datafusion`
/// - `None` unknown monotonicity or non-monotonicity
/// - `Some(true)` monotonically increasing
/// - `Some(false)` monotonically decreasing
pub type FuncMonotonicity = Vec<Option<bool>>;

#[derive(Debug, Clone)]
pub struct ScalarFunction {
    pub(crate) args: Vec<ScalarExpression>,
    pub(crate) inner: Arc<dyn ScalarFunctionImpl>,
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.summary() == other.summary()
    }
}

impl Eq for ScalarFunction {}

impl Hash for ScalarFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.summary().hash(state);
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct FunctionSummary {
    pub(crate) name: String,
    pub(crate) arg_types: Vec<LogicalType>,
}

pub trait ScalarFunctionImpl: Debug + Send + Sync {
    fn eval(&self, args: &[ScalarExpression], tuple: &Tuple) -> Result<DataValue, DatabaseError>;

    // TODO: Exploiting monotonicity when optimizing `ScalarFunctionImpl::monotonicity()`
    fn monotonicity(&self) -> Option<FuncMonotonicity>;

    fn return_type(&self) -> &LogicalType;

    fn summary(&self) -> &FunctionSummary;
}

impl ScalarFunction {
    pub fn summary(&self) -> &FunctionSummary {
        self.inner.summary()
    }
}
