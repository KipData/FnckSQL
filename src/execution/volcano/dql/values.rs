use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
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

impl<T: Transaction> ReadExecutor<T> for Values {
    fn execute(self, _: &T) -> BoxedExecutor {
        self._execute()
    }
}

impl Values {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute(self) {
        let ValuesOperator { schema_ref, rows } = self.op;

        for values in rows {
            yield Tuple {
                id: None,
                schema_ref: schema_ref.clone(),
                values,
            };
        }
    }
}
