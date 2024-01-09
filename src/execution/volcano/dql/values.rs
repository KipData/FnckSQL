use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Values {
    op: ValuesOperator,
}

impl From<ValuesOperator> for Values {
    fn from(op: ValuesOperator) -> Self {
        Values { op }
    }
}

impl<T: Transaction> Executor<T> for Values {
    fn execute(self, _transaction: &RefCell<T>) -> BoxedExecutor {
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
