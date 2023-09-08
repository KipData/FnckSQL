use futures_async_stream::try_stream;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct Filter {
    predicate: ScalarExpression,
    input: BoxedExecutor
}

impl From<(FilterOperator, BoxedExecutor)> for Filter {
    fn from((FilterOperator { predicate, having }, input): (FilterOperator, BoxedExecutor)) -> Self {
        Filter {
            predicate,
            input
        }
    }
}

impl<S: Storage> Executor<S> for Filter {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl Filter {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let Filter { predicate, input } = self;

        #[for_await]
        for tuple in input {
            let tuple = tuple?;
            if let DataValue::Boolean(option) = predicate.eval_column(&tuple).as_ref() {
                if let Some(true) = option{
                    yield tuple;
                } else {
                    continue
                }
            } else {
                unreachable!("only bool");
            }
        }
    }
}