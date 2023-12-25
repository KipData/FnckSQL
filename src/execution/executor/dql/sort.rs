use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::storage::Transaction;
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;
use std::mem;

const BUCKET_SIZE: usize = u8::MAX as usize + 1;

// LSD Radix Sort
fn radix_sort<T>(mut tuples: Vec<(T, Vec<u8>)>) -> Vec<T> {
    if let Some(max_len) = tuples.iter().map(|(_, bytes)| bytes.len()).max() {
        // init buckets
        let mut temp_buckets = Vec::with_capacity(BUCKET_SIZE);
        for i in 0..BUCKET_SIZE {
            temp_buckets[i] = Vec::new();
        }

        for i in (0..max_len).rev() {
            for (t, bytes) in tuples {
                let index = if bytes.len() > i { bytes[i] } else { 0 };

                temp_buckets[index as usize].push((t, bytes));
            }

            tuples = temp_buckets
                .iter_mut()
                .map(|group| mem::replace(group, vec![]))
                .flatten()
                .collect_vec();
        }
        return tuples.into_iter().map(|(tuple, _)| tuple).collect_vec();
    }
    Vec::new()
}

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
        let tuples_with_keys: Vec<(Tuple, Vec<u8>)> = tuples
            .into_iter()
            .map(|tuple| {
                let mut full_key = Vec::new();

                for SortField {
                    expr,
                    nulls_first,
                    asc,
                } in &sort_fields
                {
                    let mut key = Vec::new();

                    expr.eval(&tuple)?.memcomparable_encode(&mut key)?;
                    key.push(if *nulls_first { u8::MIN } else { u8::MAX });

                    if !asc {
                        for byte in key.iter_mut() {
                            *byte ^= 0xFF;
                        }
                    }
                    full_key.extend(key);
                }
                Ok::<(Tuple, Vec<u8>), TypeError>((tuple, full_key))
            })
            .try_collect()?;
        let mut tuples = radix_sort(tuples_with_keys);
        let len = limit.unwrap_or(tuples.len());

        for tuple in tuples.drain(..len) {
            yield tuple;
        }
    }
}

#[test]
fn test_sort() {
    let tupels = vec![
        (0, "abc".as_bytes().to_vec()),
        (1, "abz".as_bytes().to_vec()),
        (2, "abe".as_bytes().to_vec()),
        (3, "abcd".as_bytes().to_vec()),
    ];

    assert_eq!(radix_sort(tupels), vec![0, 3, 2, 1])
}
