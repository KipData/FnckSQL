use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::execution::executor::dql::join::joins_nullable;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::storage::Transaction;
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt, RandomState};
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;
use std::sync::Arc;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
}

impl From<JoinOperator> for HashJoin {
    fn from(JoinOperator { on, join_type }: JoinOperator) -> HashJoin {
        HashJoin { on, ty: join_type }
    }
}

impl<T: Transaction> Executor<T> for HashJoin {
    fn execute(self, inputs: Vec<BoxedExecutor>, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute(inputs)
    }
}

impl HashJoin {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self, mut inputs: Vec<BoxedExecutor>) {
        let HashJoin { on, ty } = self;

        if ty == JoinType::Cross {
            unreachable!("Cross join should not be in HashJoinExecutor");
        }
        let ((on_left_keys, on_right_keys), filter): (
            (Vec<ScalarExpression>, Vec<ScalarExpression>),
            _,
        ) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => unreachable!("HashJoin must has on condition"),
        };

        let mut join_columns = Vec::new();
        let mut used_set = HashSet::<u64>::new();
        let mut left_map = HashMap::new();

        let hash_random_state = RandomState::with_seeds(0, 0, 0, 0);
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        let mut left_init_flag = false;
        #[for_await]
        for tuple in inputs.remove(0) {
            let tuple: Tuple = tuple?;
            let hash = Self::hash_row(&on_left_keys, &hash_random_state, &tuple)?;

            if !left_init_flag {
                Self::columns_filling(&tuple, &mut join_columns, left_force_nullable);
                left_init_flag = true;
            }

            left_map.entry(hash).or_insert(Vec::new()).push(tuple);
        }

        // probe phase
        let mut right_init_flag = false;
        #[for_await]
        for tuple in inputs.remove(0) {
            let tuple: Tuple = tuple?;
            let right_cols_len = tuple.columns.len();
            let hash = Self::hash_row(&on_right_keys, &hash_random_state, &tuple)?;

            if !right_init_flag {
                Self::columns_filling(&tuple, &mut join_columns, right_force_nullable);
                right_init_flag = true;
            }

            let mut join_tuples = if let Some(tuples) = left_map.get(&hash) {
                let _ = used_set.insert(hash);

                tuples
                    .iter()
                    .map(|Tuple { values, .. }| {
                        let full_values = values
                            .iter()
                            .cloned()
                            .chain(tuple.values.clone())
                            .collect_vec();

                        Tuple {
                            id: None,
                            columns: join_columns.clone(),
                            values: full_values,
                        }
                    })
                    .collect_vec()
            } else if matches!(ty, JoinType::Right | JoinType::Full) {
                let empty_len = join_columns.len() - right_cols_len;
                let values = join_columns[..empty_len]
                    .iter()
                    .map(|col| Arc::new(DataValue::none(col.datatype())))
                    .chain(tuple.values)
                    .collect_vec();

                vec![Tuple {
                    id: None,
                    columns: join_columns.clone(),
                    values,
                }]
            } else {
                vec![]
            };

            // on filter
            if let (Some(expr), false) = (
                &filter,
                join_tuples.is_empty() || matches!(ty, JoinType::Full | JoinType::Cross),
            ) {
                let mut filter_tuples = Vec::with_capacity(join_tuples.len());

                for mut tuple in join_tuples {
                    if let DataValue::Boolean(option) = expr.eval_column(&tuple)?.as_ref() {
                        if let Some(false) | None = option {
                            let full_cols_len = tuple.columns.len();
                            let left_cols_len = full_cols_len - right_cols_len;

                            match ty {
                                JoinType::Left => {
                                    for i in left_cols_len..full_cols_len {
                                        let value_type = tuple.columns[i].datatype();

                                        tuple.values[i] = Arc::new(DataValue::none(value_type))
                                    }
                                    filter_tuples.push(tuple)
                                }
                                JoinType::Right => {
                                    for i in 0..left_cols_len {
                                        let value_type = tuple.columns[i].datatype();

                                        tuple.values[i] = Arc::new(DataValue::none(value_type))
                                    }
                                    filter_tuples.push(tuple)
                                }
                                _ => (),
                            }
                        } else {
                            filter_tuples.push(tuple)
                        }
                    } else {
                        unreachable!("only bool");
                    }
                }

                join_tuples = filter_tuples;
            }

            for tuple in join_tuples {
                yield tuple
            }
        }

        if matches!(ty, JoinType::Left | JoinType::Full) {
            for (hash, tuples) in left_map {
                if used_set.contains(&hash) {
                    continue;
                }

                for Tuple {
                    mut values,
                    columns,
                    ..
                } in tuples
                {
                    let mut right_empties = join_columns[columns.len()..]
                        .iter()
                        .map(|col| Arc::new(DataValue::none(col.datatype())))
                        .collect_vec();

                    values.append(&mut right_empties);

                    yield Tuple {
                        id: None,
                        columns: join_columns.clone(),
                        values,
                    }
                }
            }
        }
    }

    fn columns_filling(tuple: &Tuple, join_columns: &mut Vec<ColumnRef>, force_nullable: bool) {
        let mut new_columns = tuple
            .columns
            .iter()
            .cloned()
            .map(|col| {
                let mut new_catalog = ColumnCatalog::clone(&col);
                new_catalog.nullable = force_nullable;

                Arc::new(new_catalog)
            })
            .collect_vec();

        join_columns.append(&mut new_columns);
    }

    fn hash_row(
        on_keys: &[ScalarExpression],
        hash_random_state: &RandomState,
        tuple: &Tuple,
    ) -> Result<u64, TypeError> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval_column(tuple)?);
        }

        Ok(hash_random_state.hash_one(values))
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::executor::dql::join::hash_join::HashJoin;
    use crate::execution::executor::dql::test::build_integers;
    use crate::execution::executor::dql::values::Values;
    use crate::execution::executor::{try_collect, BoxedExecutor, Executor};
    use crate::execution::ExecutorError;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::storage::kip::KipStorage;
    use crate::storage::{Storage, Transaction};
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::cell::RefCell;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_join_values<T: Transaction>(
        _t: &RefCell<T>,
    ) -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        BoxedExecutor,
        BoxedExecutor,
    ) {
        let desc = ColumnDesc::new(LogicalType::Integer, false, false);

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

        let t2_columns = vec![
            Arc::new(ColumnCatalog::new(
                "c4".to_string(),
                true,
                desc.clone(),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c5".to_string(),
                true,
                desc.clone(),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c6".to_string(),
                true,
                desc.clone(),
                None,
            )),
        ];

        let on_keys = vec![(
            ScalarExpression::ColumnRef(t1_columns[0].clone()),
            ScalarExpression::ColumnRef(t2_columns[0].clone()),
        )];

        let values_t1 = Values::from(ValuesOperator {
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
                    Arc::new(DataValue::Int32(Some(3))),
                    Arc::new(DataValue::Int32(Some(5))),
                    Arc::new(DataValue::Int32(Some(7))),
                ],
            ],
            columns: t1_columns,
        });

        let values_t2 = Values::from(ValuesOperator {
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
                    Arc::new(DataValue::Int32(Some(4))),
                    Arc::new(DataValue::Int32(Some(6))),
                    Arc::new(DataValue::Int32(Some(8))),
                ],
                vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(1))),
                ],
            ],
            columns: t2_columns,
        });

        (
            on_keys,
            values_t1.execute(vec![], &_t),
            values_t2.execute(vec![], &_t),
        )
    }

    #[tokio::test]
    async fn test_inner_join() -> Result<(), ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = RefCell::new(storage.transaction().await?);
        let (keys, left, right) = build_join_values(&transaction);

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Inner,
        };
        let mut executor = HashJoin::from(op).execute(vec![left, right], &transaction);
        let tuples = try_collect(&mut executor).await?;

        println!("inner_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 3);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_left_join() -> Result<(), ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = RefCell::new(storage.transaction().await?);
        let (keys, left, right) = build_join_values(&transaction);

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Left,
        };
        let mut executor = HashJoin::from(op).execute(vec![left, right], &transaction);
        let tuples = try_collect(&mut executor).await?;

        println!("left_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 4);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            tuples[3].values,
            build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_right_join() -> Result<(), ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = RefCell::new(storage.transaction().await?);
        let (keys, left, right) = build_join_values(&transaction);

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Right,
        };
        let mut executor = HashJoin::from(op).execute(vec![left, right], &transaction);
        let tuples = try_collect(&mut executor).await?;

        println!("right_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 4);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_join() -> Result<(), ExecutorError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = RefCell::new(storage.transaction().await?);
        let (keys, left, right) = build_join_values(&transaction);

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Full,
        };
        let mut executor = HashJoin::from(op).execute(vec![left, right], &transaction);
        let tuples = try_collect(&mut executor).await?;

        println!("full_test: \n{}", create_table(&tuples));

        assert_eq!(tuples.len(), 5);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            tuples[4].values,
            build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
        );

        Ok(())
    }
}
