use crate::catalog::ColumnRef;
use crate::execution::volcano::dql::aggregate::{create_accumulators, Accumulator};
use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use ahash::HashMap;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;

pub struct HashAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    groupby_exprs: Vec<ScalarExpression>,
    input: BoxedExecutor,
}

impl From<(AggregateOperator, BoxedExecutor)> for HashAggExecutor {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
            },
            input,
        ): (AggregateOperator, BoxedExecutor),
    ) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for HashAggExecutor {
    fn execute<'a>(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute()
    }
}

pub(crate) struct HashAggStatus {
    agg_calls: Vec<ScalarExpression>,
    groupby_exprs: Vec<ScalarExpression>,

    group_columns: Vec<ColumnRef>,
    group_hash_accs: HashMap<Vec<ValueRef>, Vec<Box<dyn Accumulator>>>,
}

impl HashAggStatus {
    pub(crate) fn new(
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> Self {
        HashAggStatus {
            agg_calls,
            groupby_exprs,
            group_columns: vec![],
            group_hash_accs: Default::default(),
        }
    }

    pub(crate) fn update(&mut self, tuple: Tuple) -> Result<(), ExecutorError> {
        // 1. build group and agg columns for hash_agg columns.
        // Tips: AggCall First
        if self.group_columns.is_empty() {
            self.group_columns = self
                .agg_calls
                .iter()
                .chain(self.groupby_exprs.iter())
                .map(|expr| expr.output_column())
                .collect_vec();
        }

        // 2.1 evaluate agg exprs and collect the result values for later accumulators.
        let values: Vec<ValueRef> = self
            .agg_calls
            .iter()
            .map(|expr| {
                if let ScalarExpression::AggCall { args, .. } = expr {
                    args[0].eval(&tuple)
                } else {
                    unreachable!()
                }
            })
            .try_collect()?;

        let group_keys: Vec<ValueRef> = self
            .groupby_exprs
            .iter()
            .map(|expr| expr.eval(&tuple))
            .try_collect()?;

        for (acc, value) in self
            .group_hash_accs
            .entry(group_keys)
            .or_insert_with(|| create_accumulators(&self.agg_calls))
            .iter_mut()
            .zip_eq(values.iter())
        {
            acc.update_value(value)?;
        }

        Ok(())
    }

    pub(crate) fn to_tuples(&mut self) -> Result<Vec<Tuple>, ExecutorError> {
        Ok(self
            .group_hash_accs
            .drain()
            .map(|(group_keys, accs)| {
                // Tips: Accumulator First
                let values: Vec<ValueRef> = accs
                    .iter()
                    .map(|acc| acc.evaluate())
                    .chain(group_keys.into_iter().map(Ok))
                    .try_collect()?;

                Ok::<Tuple, ExecutorError>(Tuple {
                    id: None,
                    columns: self.group_columns.clone(),
                    values,
                })
            })
            .try_collect()?)
    }
}

impl HashAggExecutor {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let HashAggExecutor {
            agg_calls,
            groupby_exprs,
            ..
        } = self;

        let mut agg_status = HashAggStatus::new(agg_calls, groupby_exprs);

        #[for_await]
        for tuple in self.input {
            agg_status.update(tuple?)?;
        }

        for tuple in agg_status.to_tuples()? {
            yield tuple;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::volcano::dql::aggregate::hash_agg::HashAggExecutor;
    use crate::execution::volcano::dql::test::build_integers;
    use crate::execution::volcano::dql::values::Values;
    use crate::execution::volcano::{try_collect, Executor};
    use crate::execution::ExecutorError;
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::storage::kip::KipStorage;
    use crate::storage::Storage;
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_hash_agg() -> Result<(), ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await.unwrap();
        let transaction = RefCell::new(storage.transaction().await?);
        let desc = ColumnDesc::new(LogicalType::Integer, false, false, None);

        let t1_columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                true,
                desc.clone(),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                true,
                desc.clone(),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c3".to_string(),
                true,
                desc.clone(),
                None,
            )),
        ];

        let operator = AggregateOperator {
            groupby_exprs: vec![ScalarExpression::ColumnRef(t1_columns[0].clone())],
            agg_calls: vec![ScalarExpression::AggCall {
                distinct: false,
                kind: AggKind::Sum,
                args: vec![ScalarExpression::ColumnRef(t1_columns[1].clone())],
                ty: LogicalType::Integer,
            }],
        };

        let input = Values::from(ValuesOperator {
            rows: vec![
                vec![
                    Arc::new(DataValue::Int32(Some(0))),
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Int32(Some(4))),
                ],
                vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(3))),
                    Arc::new(DataValue::Int32(Some(5))),
                ],
                vec![
                    Arc::new(DataValue::Int32(Some(0))),
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(2))),
                ],
                vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Int32(Some(3))),
                ],
            ],
            columns: t1_columns,
        })
        .execute(&transaction);

        let tuples =
            try_collect(&mut HashAggExecutor::from((operator, input)).execute(&transaction))
                .await?;

        println!("hash_agg_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 2);

        let vec_values = tuples.into_iter().map(|tuple| tuple.values).collect_vec();

        assert!(vec_values.contains(&build_integers(vec![Some(3), Some(0)])));
        assert!(vec_values.contains(&build_integers(vec![Some(5), Some(1)])));

        Ok(())
    }
}
