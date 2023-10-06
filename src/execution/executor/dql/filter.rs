use crate::execution::executor::{BoxedExecutor, Executor};
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
}

impl From<FilterOperator> for Filter {
    fn from(FilterOperator { predicate, .. }: FilterOperator) -> Filter {
        Filter { predicate }
    }
}

impl<T: Transaction> Executor<T> for Filter {
    fn execute(self, inputs: Vec<BoxedExecutor>, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute(inputs)
    }
}

impl Filter {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self, mut inputs: Vec<BoxedExecutor>) {
        let Filter { predicate } = self;

        #[for_await]
        for tuple in inputs.remove(0) {
            let tuple = tuple?;
            if let DataValue::Boolean(option) = predicate.eval_column(&tuple)?.as_ref() {
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
