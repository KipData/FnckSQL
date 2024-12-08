use crate::execution::{Executor, ReadExecutor};
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::tuple::Tuple;

pub struct Dummy {}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Dummy {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        _: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                yield Ok(Tuple::new(None, Vec::new()));
            },
        )
    }
}
