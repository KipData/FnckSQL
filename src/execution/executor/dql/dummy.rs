use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Dummy {}

impl<T: Transaction> Executor<T> for Dummy {
    fn execute<'a>(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute()
    }
}

impl Dummy {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {}
}
