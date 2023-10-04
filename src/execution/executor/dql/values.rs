use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Values {
    op: ValuesOperator,
}

impl From<ValuesOperator> for Values {
    fn from(op: ValuesOperator) -> Self {
        Values { op }
    }
}

impl<S: Storage> Executor<S> for Values {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl Values {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let ValuesOperator { columns, rows } = self.op;

        for values in rows {
            yield Tuple {
                id: None,
                columns: columns.clone(),
                values,
            };
        }
    }
}
