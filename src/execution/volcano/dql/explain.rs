use crate::catalog::ColumnCatalog;
use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct Explain {
    plan: LogicalPlan,
}

impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain { plan }
    }
}

impl<T: Transaction> ReadExecutor<T> for Explain {
    fn execute(self, _: &T) -> BoxedExecutor {
        self._execute()
    }
}

impl Explain {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute(self) {
        let columns = Arc::new(vec![Arc::new(ColumnCatalog::new_dummy("PLAN".to_string()))]);
        let values = vec![Arc::new(DataValue::Utf8(Some(self.plan.explain(0))))];

        yield Tuple {
            id: None,
            columns,
            values,
        };
    }
}
