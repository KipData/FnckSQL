use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::join::joins_nullable;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use ahash::{HashMap, HashSet, HashSetExt, RandomState};
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::mem;
use std::sync::Arc;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        HashJoin {
            on,
            ty: join_type,
            left_input,
            right_input,
        }
    }
}

impl<T: Transaction> ReadExecutor<T> for HashJoin {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

pub(crate) struct HashJoinStatus {
    ty: JoinType,
    filter: Option<ScalarExpression>,

    join_columns: Vec<ColumnRef>,
    used_set: HashSet<u64>,
    build_map: HashMap<u64, Vec<Tuple>>,
    hash_random_state: RandomState,

    left_init_flag: bool,
    left_force_nullable: bool,
    on_left_keys: Vec<ScalarExpression>,
    right_init_flag: bool,
    right_force_nullable: bool,
    on_right_keys: Vec<ScalarExpression>,
}

impl HashJoinStatus {
    pub(crate) fn new(on: JoinCondition, ty: JoinType) -> Self {
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
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

        HashJoinStatus {
            ty,
            filter,

            join_columns: vec![],
            used_set: HashSet::new(),
            build_map: Default::default(),
            hash_random_state: RandomState::with_seeds(0, 0, 0, 0),

            left_init_flag: false,
            left_force_nullable,
            on_left_keys,
            right_init_flag: false,
            right_force_nullable,
            on_right_keys,
        }
    }

    pub(crate) fn left_build(&mut self, tuple: Tuple) -> Result<(), DatabaseError> {
        let HashJoinStatus {
            on_left_keys,
            hash_random_state,
            left_init_flag,
            join_columns,
            left_force_nullable,
            build_map,
            ..
        } = self;

        let hash = Self::hash_row(on_left_keys, hash_random_state, &tuple)?;

        if !*left_init_flag {
            Self::columns_filling(&tuple, join_columns, *left_force_nullable);
            let _ = mem::replace(left_init_flag, true);
        }

        build_map
            .entry(hash)
            .or_insert_with(|| Vec::new())
            .push(tuple);

        Ok(())
    }

    pub(crate) fn right_probe(&mut self, tuple: Tuple) -> Result<Vec<Tuple>, DatabaseError> {
        let HashJoinStatus {
            hash_random_state,
            join_columns,
            on_right_keys,
            right_init_flag,
            right_force_nullable,
            build_map,
            used_set,
            ty,
            filter,
            ..
        } = self;

        let right_cols_len = tuple.columns.len();
        let hash = Self::hash_row(&on_right_keys, &hash_random_state, &tuple)?;

        if !*right_init_flag {
            Self::columns_filling(&tuple, join_columns, *right_force_nullable);
            let _ = mem::replace(right_init_flag, true);
        }

        let mut join_tuples = if let Some(tuples) = build_map.get(&hash) {
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
                if let DataValue::Boolean(option) = expr.eval(&tuple)?.as_ref() {
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

        Ok(join_tuples)
    }

    pub(crate) fn build_drop(&mut self) -> Vec<Tuple> {
        let HashJoinStatus {
            join_columns,
            build_map,
            used_set,
            ty,
            ..
        } = self;

        matches!(ty, JoinType::Left | JoinType::Full)
            .then(|| {
                build_map
                    .drain()
                    .filter(|(hash, _)| !used_set.contains(hash))
                    .map(|(_, mut tuples)| {
                        for Tuple {
                            values,
                            columns,
                            id,
                        } in tuples.iter_mut()
                        {
                            let _ = mem::replace(id, None);
                            let (mut right_values, mut right_columns): (
                                Vec<ValueRef>,
                                Vec<ColumnRef>,
                            ) = join_columns[columns.len()..]
                                .iter()
                                .map(|col| (Arc::new(DataValue::none(col.datatype())), col.clone()))
                                .unzip();

                            values.append(&mut right_values);
                            columns.append(&mut right_columns);
                        }
                        tuples
                    })
                    .flatten()
                    .collect_vec()
            })
            .unwrap_or_else(|| vec![])
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
    ) -> Result<u64, DatabaseError> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval(tuple)?);
        }

        Ok(hash_random_state.hash_one(values))
    }
}

impl HashJoin {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let HashJoin {
            on,
            ty,
            left_input,
            right_input,
        } = self;

        let mut join_status = HashJoinStatus::new(on, ty);

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        #[for_await]
        for tuple in build_read(left_input, transaction) {
            let tuple: Tuple = tuple?;

            join_status.left_build(tuple)?;
        }

        // probe phase
        #[for_await]
        for tuple in build_read(right_input, transaction) {
            let tuple: Tuple = tuple?;

            for tuple in join_status.right_probe(tuple)? {
                yield tuple
            }
        }

        for tuple in join_status.build_drop() {
            yield tuple
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::volcano::dql::join::hash_join::HashJoin;
    use crate::execution::volcano::dql::test::build_integers;
    use crate::execution::volcano::{try_collect, ReadExecutor};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::LogicalPlan;
    use crate::storage::kip::KipStorage;
    use crate::storage::Storage;
    use crate::types::tuple::create_table;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_join_values() -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        LogicalPlan,
        LogicalPlan,
    ) {
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

        let values_t1 = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
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
            }),
            childrens: vec![],
            physical_option: None,
        };

        let values_t2 = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
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
            }),
            childrens: vec![],
            physical_option: None,
        };

        (on_keys, values_t1, values_t2)
    }

    #[tokio::test]
    async fn test_inner_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = storage.transaction().await?;
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Inner,
        };
        let mut executor = HashJoin::from((op, left, right)).execute(&transaction);
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
    async fn test_left_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = storage.transaction().await?;
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Left,
        };
        let mut executor = HashJoin::from((op, left, right)).execute(&transaction);
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
    async fn test_right_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = storage.transaction().await?;
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Right,
        };
        let mut executor = HashJoin::from((op, left, right)).execute(&transaction);
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
    async fn test_full_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let transaction = storage.transaction().await?;
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Full,
        };
        let mut executor = HashJoin::from((op, left, right)).execute(&transaction);
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
