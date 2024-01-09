use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Filter {
    predicate: ScalarExpression,
    input: BoxedExecutor,
}

impl From<(FilterOperator, BoxedExecutor)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, BoxedExecutor)) -> Self {
        Filter { predicate, input }
    }
}

impl<T: Transaction> Executor<T> for Filter {
    fn execute(self, _transaction: &RefCell<T>) -> BoxedExecutor {
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
            if let DataValue::Boolean(option) = predicate.eval(&tuple)?.as_ref() {
                if let Some(true) = option {
                    yield tuple;
                } else {
                    continue;
                }
            } else {
                unreachable!("only bool");
            }
        }
    }
}
