use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;
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

impl<T: Transaction> Executor<T> for Sort {
    fn execute(self, _transaction: &RefCell<T>) -> BoxedExecutor {
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

        let sort_values: Vec<Vec<ValueRef>> = tuples
            .iter()
            .map(|tuple| {
                sort_fields
                    .iter()
                    .map(|SortField { expr, .. }| expr.eval(tuple))
                    .try_collect()
            })
            .try_collect()?;

        tuples = tuples
            .into_iter()
            .enumerate()
            .sorted_by(|(i_1, _), (i_2, _)| {
                let mut ordering = Ordering::Equal;

                for (
                    sort_index,
                    SortField {
                        asc, nulls_first, ..
                    },
                ) in sort_fields.iter().enumerate()
                {
                    let value_1 = &sort_values[*i_1][sort_index];
                    let value_2 = &sort_values[*i_2][sort_index];

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
            })
            .map(|(_, tuple)| tuple)
            .collect_vec();

        let len = limit.unwrap_or(tuples.len());

        for tuple in tuples.drain(..len) {
            yield tuple;
        }
    }
}
