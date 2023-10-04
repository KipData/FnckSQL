use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cmp::Ordering;

pub struct Sort {
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    input: BoxedExecutor,
}

impl From<(SortOperator, BoxedExecutor)> for Sort {
    fn from((SortOperator { sort_fields, limit }, input): (SortOperator, BoxedExecutor)) -> Self {
        Sort {
            sort_fields,
            limit,
            input,
        }
    }
}

impl<S: Storage> Executor<S> for Sort {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl Sort {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let Sort {
            sort_fields,
            limit,
            input,
        } = self;
        let mut tuples: Vec<Tuple> = vec![];

        #[for_await]
        for tuple in input {
            tuples.push(tuple?);
        }

        tuples.sort_by(|tuple_1, tuple_2| {
            let mut ordering = Ordering::Equal;

            for SortField {
                expr,
                asc,
                nulls_first,
            } in &sort_fields
            {
                let value_1 = expr.eval_column(tuple_1).unwrap();
                let value_2 = expr.eval_column(tuple_2).unwrap();

                ordering = value_1.partial_cmp(&value_2).unwrap_or_else(|| {
                    match (value_1.is_null(), value_2.is_null()) {
                        (false, true) => {
                            if *nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (true, false) => {
                            if *nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        _ => Ordering::Equal,
                    }
                });

                if !*asc {
                    ordering = ordering.reverse();
                }

                if ordering != Ordering::Equal {
                    break;
                }
            }

            ordering
        });

        let len = limit.unwrap_or(tuples.len());

        for tuple in tuples.drain(..len) {
            yield tuple;
        }
    }
}
