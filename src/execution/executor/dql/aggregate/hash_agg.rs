use ahash::{HashMap, HashMapExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::executor::dql::aggregate::create_accumulators;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;

pub struct HashAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
    pub groupby_exprs: Vec<ScalarExpression>,
    pub input: BoxedExecutor,
}

impl From<(AggregateOperator, BoxedExecutor)> for HashAggExecutor {
    fn from((AggregateOperator { agg_calls, groupby_exprs }, input): (AggregateOperator, BoxedExecutor)) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
        }
    }
}

impl<S: Storage> Executor<S> for HashAggExecutor {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl HashAggExecutor {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let mut group_and_agg_columns_option = None;
        let mut group_hash_accs = HashMap::new();

        #[for_await]
        for tuple in self.input {
            let tuple = tuple?;

            // 1. build group and agg columns for hash_agg columns.
            // Tips: AggCall First
            group_and_agg_columns_option.get_or_insert_with(|| {
                self.agg_calls
                    .iter()
                    .chain(self.groupby_exprs.iter())
                    .map(|expr| expr.output_columns(&tuple))
                    .collect_vec()
            });

            // 2.1 evaluate agg exprs and collect the result values for later accumulators.
            let values = self.agg_calls
                .iter()
                .map(|expr| {
                    if let ScalarExpression::AggCall { args, .. } = expr {
                        args[0].eval_column(&tuple)
                    } else {
                        unreachable!()
                    }
                })
                .collect_vec();

            let group_keys = self.groupby_exprs
                .iter()
                .map(|expr| expr.eval_column(&tuple))
                .collect_vec();

            for (acc, value) in group_hash_accs
                .entry(group_keys)
                .or_insert_with(|| create_accumulators(&self.agg_calls))
                .iter_mut()
                .zip_eq(values.iter())
            {
                acc.update_value(value)?;
            }
        }

        if let Some(group_and_agg_columns) = group_and_agg_columns_option {
            for (group_keys, accs) in group_hash_accs {
                // Tips: Accumulator First
                let values: Vec<ValueRef> = accs.iter()
                    .map(|acc| acc.evaluate())
                    .chain(group_keys
                        .into_iter()
                        .map(|key| Ok(key)))
                    .try_collect()?;

                yield Tuple {
                    id: None,
                    columns: group_and_agg_columns.clone(),
                    values,
                };
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use itertools::Itertools;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::executor::dql::aggregate::hash_agg::HashAggExecutor;
    use crate::execution::executor::dql::values::Values;
    use crate::execution::executor::{Executor, try_collect};
    use crate::execution::executor::dql::test::build_integers;
    use crate::execution::ExecutorError;
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::storage::memory::MemStorage;
    use crate::types::LogicalType;
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;


    #[tokio::test]
    async fn test_hash_agg() -> Result<(), ExecutorError> {
        let mem_storage = MemStorage::new();
        let desc = ColumnDesc::new(LogicalType::Integer, false);

        let t1_columns = vec![
            Arc::new(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ];

        let operator = AggregateOperator {
            groupby_exprs: vec![
                ScalarExpression::ColumnRef(t1_columns[0].clone())
            ],
            agg_calls: vec![
                ScalarExpression::AggCall {
                    distinct: false,
                    kind: AggKind::Sum,
                    args: vec![
                        ScalarExpression::ColumnRef(t1_columns[1].clone())
                    ],
                    ty: LogicalType::Integer,
                }
            ],
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
                ]
            ],
            columns: t1_columns,
        }).execute(&mem_storage);

        let tuples = try_collect(&mut HashAggExecutor::from((operator, input)).execute(&mem_storage)).await?;

        println!("hash_agg_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 2);

        let vec_values = tuples
            .into_iter()
            .map(|tuple| tuple.values)
            .collect_vec();

        assert!(vec_values.contains(&build_integers(vec![Some(3), Some(0)])));
        assert!(vec_values.contains(&build_integers(vec![Some(5), Some(1)])));

        Ok(())
    }
}