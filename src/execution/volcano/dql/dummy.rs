use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Dummy {}

impl<T: Transaction> ReadExecutor<T> for Dummy {
    fn execute(self, _: &T) -> BoxedExecutor {
        self._execute()
    }
}

impl Dummy {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute(self) {}
}
