use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use serde::Deserialize;
use serde::Serialize;
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Lower {
    summary: FunctionSummary,
}

impl Lower {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "lower".to_lowercase();
        let arg_types = vec![LogicalType::Varchar(None, CharLengthUnits::Characters)];
        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types,
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for Lower {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        exprs: &[ScalarExpression],
        tuples: Option<(&Tuple, &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError> {
        let mut value = exprs[0].eval(tuples)?;
        if !matches!(value.logical_type(), LogicalType::Varchar(_, _)) {
            value = value.cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?;
        }
        if let DataValue::Utf8 {
            value: Some(value),
            ty,
            unit,
        } = &mut value
        {
            *value = value.to_lowercase();
        }
        Ok(value)
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        todo!()
    }

    fn return_type(&self) -> &LogicalType {
        &LogicalType::Varchar(None, CharLengthUnits::Characters)
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
