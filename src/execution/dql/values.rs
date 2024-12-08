use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::mem;

pub struct Values {
    op: ValuesOperator,
}

impl From<ValuesOperator> for Values {
    fn from(op: ValuesOperator) -> Self {
        Values { op }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        _: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let ValuesOperator { rows, schema_ref } = self.op;

                for mut values in rows {
                    for (i, value) in values.iter_mut().enumerate() {
                        let ty = schema_ref[i].datatype().clone();

                        if value.logical_type() != ty {
                            *value = throw!(mem::replace(value, DataValue::Null).cast(&ty));
                        }
                    }

                    yield Ok(Tuple::new(None, values));
                }
            },
        )
    }
}
