use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::throw;
use crate::types::tuple::{Schema, SchemaRef, Tuple};
use crate::types::value::{DataValue, ValueRef, NULL_VALUE};
use crate::utils::bit_vector::BitVector;
use ahash::HashMap;
use itertools::Itertools;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type, .. }, left_input, right_input): (
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

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashJoin {
    fn execute(
        self,
        cache: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let HashJoin {
                    on,
                    ty,
                    mut left_input,
                    mut right_input,
                } = self;
                let mut join_status = HashJoinStatus::new(
                    on,
                    ty,
                    left_input.output_schema(),
                    right_input.output_schema(),
                );
                let join_status_ptr: *mut HashJoinStatus = &mut join_status;

                // build phase:
                // 1.construct hashtable, one hash key may contains multiple rows indices.
                // 2.merged all left tuples.
                let mut coroutine = build_read(left_input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    throw!(unsafe { (*join_status_ptr).left_build(tuple) });
                }

                // probe phase
                let mut coroutine = build_read(right_input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    unsafe {
                        let mut coroutine = (*join_status_ptr).right_probe(tuple);

                        while let CoroutineState::Yielded(tuple) =
                            Pin::new(&mut coroutine).resume(())
                        {
                            yield tuple;
                        }
                    }
                }

                unsafe {
                    if let Some(mut coroutine) = (*join_status_ptr).build_drop() {
                        while let CoroutineState::Yielded(tuple) =
                            Pin::new(&mut coroutine).resume(())
                        {
                            yield tuple;
                        }
                    };
                }
            },
        )
    }
}

pub(crate) struct HashJoinStatus {
    ty: JoinType,
    filter: Option<ScalarExpression>,
    build_map: HashMap<Vec<ValueRef>, (Vec<Tuple>, bool, bool)>,

    full_schema_ref: SchemaRef,
    left_schema_len: usize,
    on_left_keys: Vec<ScalarExpression>,
    on_right_keys: Vec<ScalarExpression>,
}

impl HashJoinStatus {
    pub(crate) fn new(
        on: JoinCondition,
        ty: JoinType,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> Self {
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
        if on_left_keys.is_empty() || on_right_keys.is_empty() {
            todo!("`NestLoopJoin` should be used when there is no equivalent condition")
        }
        assert!(!on_left_keys.is_empty());
        assert!(!on_right_keys.is_empty());

        let fn_process = |schema: &mut Vec<ColumnRef>, force_nullable| {
            for column in schema.iter_mut() {
                let mut temp = ColumnCatalog::clone(column);
                temp.nullable = force_nullable;

                *column = Arc::new(temp);
            }
        };
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);
        let left_schema_len = left_schema.len();

        let mut join_schema = Vec::clone(left_schema);
        fn_process(&mut join_schema, left_force_nullable);
        let mut right_schema = Vec::clone(right_schema);
        fn_process(&mut right_schema, right_force_nullable);

        join_schema.append(&mut right_schema);

        HashJoinStatus {
            ty,
            filter,
            build_map: Default::default(),

            full_schema_ref: Arc::new(join_schema),
            left_schema_len,
            on_left_keys,
            on_right_keys,
        }
    }

    pub(crate) fn left_build(&mut self, tuple: Tuple) -> Result<(), DatabaseError> {
        let HashJoinStatus {
            on_left_keys,
            build_map,
            full_schema_ref,
            left_schema_len,
            ..
        } = self;
        let values = Self::eval_keys(on_left_keys, &tuple, &full_schema_ref[0..*left_schema_len])?;

        build_map
            .entry(values)
            .or_insert_with(|| (Vec::new(), false, false))
            .0
            .push(tuple);

        Ok(())
    }

    pub(crate) fn right_probe(&mut self, tuple: Tuple) -> Executor {
        Box::new(
            #[coroutine]
            move || {
                let HashJoinStatus {
                    on_right_keys,
                    full_schema_ref,
                    build_map,
                    ty,
                    filter,
                    left_schema_len,
                    ..
                } = self;

                let right_cols_len = tuple.values.len();
                let values = throw!(Self::eval_keys(
                    on_right_keys,
                    &tuple,
                    &full_schema_ref[*left_schema_len..]
                ));
                let has_null = values.iter().any(|value| value.is_null());

                if let (false, Some((tuples, is_used, is_filtered))) =
                    (has_null, build_map.get_mut(&values))
                {
                    let mut bits_option = None;
                    *is_used = true;

                    match ty {
                        JoinType::LeftSemi => {
                            if *is_filtered {
                                return;
                            } else {
                                bits_option = Some(BitVector::new(tuples.len()));
                            }
                        }
                        JoinType::LeftAnti => return,
                        _ => (),
                    }
                    for (i, Tuple { values, .. }) in tuples.iter().enumerate() {
                        let full_values = values
                            .iter()
                            .cloned()
                            .chain(tuple.values.clone())
                            .collect_vec();
                        let tuple = Tuple {
                            id: None,
                            values: full_values,
                        };
                        if let Some(tuple) = throw!(Self::filter(
                            tuple,
                            full_schema_ref,
                            filter,
                            ty,
                            *left_schema_len
                        )) {
                            if let Some(bits) = bits_option.as_mut() {
                                bits.set_bit(i, true);
                            } else {
                                yield Ok(tuple);
                            }
                        }
                    }
                    if let Some(bits) = bits_option {
                        let mut cnt = 0;
                        tuples.retain(|_| {
                            let res = bits.get_bit(cnt);
                            cnt += 1;
                            res
                        });
                        *is_filtered = true
                    }
                } else if matches!(ty, JoinType::RightOuter | JoinType::Full) {
                    let empty_len = full_schema_ref.len() - right_cols_len;
                    let values = (0..empty_len)
                        .map(|_| NULL_VALUE.clone())
                        .chain(tuple.values)
                        .collect_vec();
                    let tuple = Tuple { id: None, values };
                    if let Some(tuple) = throw!(Self::filter(
                        tuple,
                        full_schema_ref,
                        filter,
                        ty,
                        *left_schema_len
                    )) {
                        yield Ok(tuple);
                    }
                }
            },
        )
    }

    pub(crate) fn filter(
        mut tuple: Tuple,
        schema: &Schema,
        filter: &Option<ScalarExpression>,
        join_ty: &JoinType,
        left_schema_len: usize,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if let (Some(expr), false) = (filter, matches!(join_ty, JoinType::Full | JoinType::Cross)) {
            match expr.eval(&tuple, schema)?.as_ref() {
                DataValue::Boolean(Some(false) | None) => {
                    let full_schema_len = schema.len();

                    match join_ty {
                        JoinType::LeftOuter => {
                            for i in left_schema_len..full_schema_len {
                                tuple.values[i] = NULL_VALUE.clone();
                            }
                        }
                        JoinType::RightOuter => {
                            for i in 0..left_schema_len {
                                tuple.values[i] = NULL_VALUE.clone();
                            }
                        }
                        _ => return Ok(None),
                    }
                }
                DataValue::Boolean(Some(true)) => (),
                _ => return Err(DatabaseError::InvalidType),
            }
        }

        Ok(Some(tuple))
    }

    pub(crate) fn build_drop(&mut self) -> Option<Executor> {
        let HashJoinStatus {
            full_schema_ref,
            build_map,
            ty,
            filter,
            left_schema_len,
            ..
        } = self;

        match ty {
            JoinType::LeftOuter | JoinType::Full => {
                Some(Self::right_null_tuple(build_map, full_schema_ref))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => Some(Self::one_side_tuple(
                build_map,
                full_schema_ref,
                filter,
                ty,
                *left_schema_len,
            )),
            _ => None,
        }
    }

    fn right_null_tuple<'a>(
        build_map: &'a mut HashMap<Vec<ValueRef>, (Vec<Tuple>, bool, bool)>,
        schema: &'a Schema,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                for (_, (left_tuples, is_used, _)) in build_map.drain() {
                    if is_used {
                        continue;
                    }
                    for mut tuple in left_tuples {
                        while tuple.values.len() != schema.len() {
                            tuple.values.push(NULL_VALUE.clone());
                        }
                        yield Ok(tuple);
                    }
                }
            },
        )
    }

    fn one_side_tuple<'a>(
        build_map: &'a mut HashMap<Vec<ValueRef>, (Vec<Tuple>, bool, bool)>,
        schema: &'a Schema,
        filter: &'a Option<ScalarExpression>,
        join_ty: &'a JoinType,
        left_schema_len: usize,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let is_left_semi = matches!(join_ty, JoinType::LeftSemi);

                for (_, (left_tuples, mut is_used, is_filtered)) in build_map.drain() {
                    if is_left_semi {
                        is_used = !is_used;
                    }
                    if is_used {
                        continue;
                    }
                    if is_filtered {
                        for tuple in left_tuples {
                            yield Ok(tuple);
                        }
                        continue;
                    }
                    for tuple in left_tuples {
                        if let Some(tuple) = throw!(Self::filter(
                            tuple,
                            schema,
                            filter,
                            join_ty,
                            left_schema_len
                        )) {
                            yield Ok(tuple);
                        }
                    }
                }
            },
        )
    }

    fn eval_keys(
        on_keys: &[ScalarExpression],
        tuple: &Tuple,
        schema: &[ColumnRef],
    ) -> Result<Vec<ValueRef>, DatabaseError> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval(tuple, schema)?);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::join::hash_join::HashJoin;
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::LogicalPlan;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::ShardingLruCache;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_join_values() -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        LogicalPlan,
        LogicalPlan,
    ) {
        let desc = ColumnDesc::new(LogicalType::Integer, false, false, None);

        let t1_columns = vec![
            Arc::new(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ];

        let t2_columns = vec![
            Arc::new(ColumnCatalog::new("c4".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c5".to_string(), true, desc.clone())),
            Arc::new(ColumnCatalog::new("c6".to_string(), true, desc.clone())),
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
                schema_ref: Arc::new(t1_columns),
            }),
            childrens: vec![],
            physical_option: None,
            _output_schema_ref: None,
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
                schema_ref: Arc::new(t2_columns),
            }),
            childrens: vec![],
            physical_option: None,
            _output_schema_ref: None,
        };

        (on_keys, values_t1, values_t2)
    }

    #[test]
    fn test_inner_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let table_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Inner,
        };
        let executor =
            HashJoin::from((op, left, right)).execute((&table_cache, &meta_cache), &transaction);
        let tuples = try_collect(executor)?;

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

    #[test]
    fn test_left_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let table_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::LeftOuter,
        };
        //Outer
        {
            let executor = HashJoin::from((op.clone(), left.clone(), right.clone()));
            let tuples = try_collect(executor.execute((&table_cache, &meta_cache), &transaction))?;

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
        }
        // Semi
        {
            let mut executor = HashJoin::from((op.clone(), left.clone(), right.clone()));
            executor.ty = JoinType::LeftSemi;
            let mut tuples =
                try_collect(executor.execute((&table_cache, &meta_cache), &transaction))?;

            assert_eq!(tuples.len(), 2);
            tuples.sort_by_key(|tuple| {
                let mut bytes = Vec::new();
                tuple.values[0].memcomparable_encode(&mut bytes).unwrap();
                bytes
            });

            assert_eq!(
                tuples[0].values,
                build_integers(vec![Some(0), Some(2), Some(4)])
            );
            assert_eq!(
                tuples[1].values,
                build_integers(vec![Some(1), Some(3), Some(5)])
            );
        }
        // Anti
        {
            let mut executor = HashJoin::from((op, left, right));
            executor.ty = JoinType::LeftAnti;
            let tuples = try_collect(executor.execute((&table_cache, &meta_cache), &transaction))?;

            assert_eq!(tuples.len(), 1);
            assert_eq!(
                tuples[0].values,
                build_integers(vec![Some(3), Some(5), Some(7)])
            );
        }

        Ok(())
    }

    #[test]
    fn test_right_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let table_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::RightOuter,
        };
        let executor =
            HashJoin::from((op, left, right)).execute((&table_cache, &meta_cache), &transaction);
        let tuples = try_collect(executor)?;

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

    #[test]
    fn test_full_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let table_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new())?);
        let (keys, left, right) = build_join_values();

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Full,
        };
        let executor =
            HashJoin::from((op, left, right)).execute((&table_cache, &meta_cache), &transaction);
        let tuples = try_collect(executor)?;

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
