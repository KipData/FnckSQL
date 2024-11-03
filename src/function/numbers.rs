use crate::catalog::ColumnCatalog;
use crate::catalog::ColumnDesc;
use crate::catalog::TableCatalog;
use crate::errors::DatabaseError;
use crate::expression::function::table::TableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

lazy_static! {
    static ref NUMBERS: TableCatalog = {
        TableCatalog::new(
            Arc::new("numbers".to_lowercase()),
            vec![ColumnCatalog::new(
                "number".to_lowercase(),
                true,
                ColumnDesc::new(LogicalType::Integer, false, false, None).unwrap(),
            )],
        )
        .unwrap()
    };
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Numbers {
    summary: FunctionSummary,
}

impl Numbers {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "numbers".to_lowercase();

        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types: vec![LogicalType::Integer],
            },
        })
    }
}

#[typetag::serde]
impl TableFunctionImpl for Numbers {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        args: &[ScalarExpression],
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>, DatabaseError> {
        let tuple = Tuple {
            id: None,
            values: Vec::new(),
        };

        let mut value = args[0].eval(&tuple, &[])?;

        if value.logical_type() != LogicalType::Integer {
            value = Arc::new(DataValue::clone(&value).cast(&LogicalType::Integer)?);
        }
        let num = value.i32().ok_or(DatabaseError::NotNull)?;

        Ok(Box::new((0..num).map(|i| {
            Ok(Tuple {
                id: None,
                values: vec![Arc::new(DataValue::Int32(Some(i)))],
            })
        }))
            as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
    }

    fn output_schema(&self) -> &SchemaRef {
        NUMBERS.schema_ref()
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }

    fn table(&self) -> &'static TableCatalog {
        &NUMBERS
    }
}
