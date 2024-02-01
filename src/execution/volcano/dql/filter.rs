use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;

pub struct Filter {
    predicate: ScalarExpression,
    input: LogicalPlan,
}

impl From<(FilterOperator, LogicalPlan)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, LogicalPlan)) -> Self {
        Filter { predicate, input }
    }
}

impl<T: Transaction> ReadExecutor<T> for Filter {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Filter {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let Filter { predicate, input } = self;

        #[for_await]
        for tuple in build_read(input, transaction) {
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
