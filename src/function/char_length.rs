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
pub(crate) struct CharLength {
    summary: FunctionSummary,
}

pub type CharacterLength = CharLength;

impl CharacterLength {
    #[allow(unused_mut)]
    pub(crate) fn new2() -> Arc<Self> {
        let function_name = "character_length".to_lowercase();
        let arg_types = vec![LogicalType::Varchar(None, CharLengthUnits::Characters)];
        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types,
            },
        })
    }
}

impl CharLength {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "char_length".to_lowercase();
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
impl ScalarFunctionImpl for CharLength {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        exprs: &[ScalarExpression],
        tuples: &Tuple,
        columns: &[ColumnRef],
    ) -> Result<DataValue, DatabaseError> {
        let value = exprs[0].eval(tuples, columns)?;
        let mut value = DataValue::clone(&value);
        if !matches!(value.logical_type(), LogicalType::Varchar(_, _)) {
            value = DataValue::clone(&value)
                .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?;
        }
        let mut length: u64 = 0;
        if let DataValue::Utf8 {
            value: Some(value),
            ty,
            unit,
        } = &mut value
        {
            length = value.len() as u64;
        }
        Ok(DataValue::UInt64(Some(length)))
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
