use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::types::tuple::Tuple;

pub struct Values {
    op: ValuesOperator,
}

impl From<ValuesOperator> for Values {
    fn from(op: ValuesOperator) -> Self {
        Values { op }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values {
    fn execute(self, _: (&'a TableCache, &'a StatisticsMetaCache), _: &T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let ValuesOperator { rows, .. } = self.op;

                for values in rows {
                    yield Ok(Tuple { id: None, values });
                }
            },
        )
    }
}
