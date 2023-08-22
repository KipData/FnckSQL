use std::cmp::Ordering;
use futures_async_stream::try_stream;
use crate::execution::executor::BoxedExecutor;
use crate::execution::ExecutorError;
use crate::planner::operator::sort::SortField;
use crate::types::tuple::Tuple;

pub struct Sort { }

impl Sort {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(sort_fields: Vec<SortField>, limit: Option<usize>, input: BoxedExecutor) {
        let mut tuples: Vec<Tuple> = vec![];

        #[for_await]
        for tuple in input {
            tuples.push(tuple?);
        }

        tuples.sort_by(|tuple_1, tuple_2| {
            let mut ordering = Ordering::Equal;

            for SortField { expr, desc, nulls_first } in &sort_fields {
                let value_1 = expr.eval_column(tuple_1);
                let value_2 = expr.eval_column(tuple_2);

                ordering = value_1.partial_cmp(&value_2)
                    .unwrap_or_else(|| match (value_1.is_null(), value_2.is_null()) {
                        (false, true) => if *nulls_first { Ordering::Less } else { Ordering::Greater },
                        (true, false) => if *nulls_first { Ordering::Greater } else { Ordering::Less },
                        (true, true) => Ordering::Equal,
                        _ => unreachable!(),
                    });

                if *desc {
                    ordering = ordering.reverse();
                }

                if ordering != Ordering::Equal {
                   break
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
