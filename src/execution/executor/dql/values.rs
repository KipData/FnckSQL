use futures_async_stream::try_stream;
use crate::execution::ExecutorError;
use crate::execution::physical_plan::physical_values::PhysicalValues;
use crate::planner::operator::values::ValuesOperator;
use crate::types::tuple::Tuple;

pub struct Values { }

impl Values {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(plan: PhysicalValues) {
        let ValuesOperator { columns, rows } = plan.op;

        for values in rows {
            yield Tuple {
                id: None,
                columns: columns.clone(),
                values,
            };
        };
    }
}