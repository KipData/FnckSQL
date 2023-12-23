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
fn radix_sort<T>(tuple_groups: &mut Vec<(T, Vec<u8>)>) {
    if let Some(max_len) = tuple_groups.iter().map(|(_, bytes)| bytes.len()).max() {
        // init buckets
        let mut temp_buckets = Vec::new();
        for _ in 0..BUCKET_SIZE {
            temp_buckets.push(Vec::new());
        }

        // Use Option Vector to avoid empty data allocation
        let mut temp_groups = tuple_groups
            .drain(..)
            .into_iter()
            .map(|item| Some(item))
            .collect_vec();

        for i in (0..max_len).rev() {
            for option in temp_groups.into_iter() {
                let (t, bytes) = option.unwrap();
                let index = if bytes.len() > i { bytes[i] } else { 0 };
                temp_buckets[index as usize].push(Some((t, bytes)));
            }

            temp_groups = temp_buckets
                .iter_mut()
                .map(|group| mem::replace(group, vec![]))
                .flatten()
                .collect_vec();
        }
        tuple_groups.extend(temp_groups.into_iter().flatten());
    }
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
        let mut tuples_with_keys: Vec<(Tuple, Vec<u8>)> = tuples
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

        radix_sort(&mut tuples_with_keys);

        let len = limit.unwrap_or(tuples_with_keys.len());

        for tuple in tuples_with_keys.drain(..len).map(|(tuple, _)| tuple) {
            yield tuple;
        }
    }
}

#[test]
fn test_sort() {
    let mut tupels = vec![
        (0, "abc".as_bytes().to_vec()),
        (1, "abz".as_bytes().to_vec()),
        (2, "abe".as_bytes().to_vec()),
        (3, "abcd".as_bytes().to_vec()),
    ];

    radix_sort(&mut tupels);

    assert_eq!(
        tupels,
        vec![
            (0, "abc".as_bytes().to_vec()),
            (3, "abcd".as_bytes().to_vec()),
            (2, "abe".as_bytes().to_vec()),
            (1, "abz".as_bytes().to_vec()),
        ]
    )
}
