use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Dummy {}

impl<S: Storage> Executor<S> for Dummy {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl Dummy {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {}
}
