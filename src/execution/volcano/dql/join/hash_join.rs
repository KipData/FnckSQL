use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::join::joins_nullable;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::{DataValue, ValueRef, NULL_VALUE};
use ahash::HashMap;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
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

impl<T: Transaction> ReadExecutor<T> for HashJoin {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

pub(crate) struct HashJoinStatus {
    ty: JoinType,
    filter: Option<ScalarExpression>,
    build_map: HashMap<Vec<ValueRef>, (Vec<Tuple>, bool)>,

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
            ..
        } = self;
        let values = Self::eval_keys(on_left_keys, &tuple)?;

        build_map
            .entry(values)
            .or_insert_with(|| (Vec::new(), false))
            .0
            .push(tuple);

        Ok(())
    }

    pub(crate) fn right_probe(&mut self, tuple: Tuple) -> Result<Vec<Tuple>, DatabaseError> {
        let HashJoinStatus {
            on_right_keys,
            full_schema_ref,
            build_map,
            ty,
            filter,
            left_schema_len,
            ..
        } = self;

        let right_cols_len = tuple.schema_ref.len();
        let values = Self::eval_keys(on_right_keys, &tuple)?;

        let mut join_tuples = Vec::with_capacity(1);
        if let Some((tuples, is_used)) = build_map.get_mut(&values) {
            *is_used = true;
            join_tuples.reserve(tuples.len());

            for Tuple { values, .. } in tuples {
                let full_values = values
                    .iter()
                    .cloned()
                    .chain(tuple.values.clone())
                    .collect_vec();
                let tuple = Tuple {
                    id: None,
                    schema_ref: full_schema_ref.clone(),
                    values: full_values,
                };
                if let Some(tuple) = Self::filter(tuple, filter, ty, *left_schema_len)? {
                    join_tuples.push(tuple);
                }
            }
        } else if matches!(ty, JoinType::Right | JoinType::Full) {
            let empty_len = full_schema_ref.len() - right_cols_len;
            let values = (0..empty_len)
                .map(|_| NULL_VALUE.clone())
                .chain(tuple.values)
                .collect_vec();
            let tuple = Tuple {
                id: None,
                schema_ref: full_schema_ref.clone(),
                values,
            };
            if let Some(tuple) = Self::filter(tuple, filter, ty, *left_schema_len)? {
                join_tuples.push(tuple);
            }
        }

        Ok(join_tuples)
    }

    pub(crate) fn filter(
        mut tuple: Tuple,
        filter: &Option<ScalarExpression>,
        join_ty: &JoinType,
        left_schema_len: usize,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if let (Some(expr), false) = (filter, matches!(join_ty, JoinType::Full | JoinType::Cross)) {
            match expr.eval(&tuple)?.as_ref() {
                DataValue::Boolean(Some(false) | None) => {
                    let full_schema_len = tuple.schema_ref.len();

                    match join_ty {
                        JoinType::Left => {
                            for i in left_schema_len..full_schema_len {
                                tuple.values[i] = NULL_VALUE.clone();
                            }
                        }
                        JoinType::Right => {
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

    pub(crate) fn build_drop(&mut self) -> Option<BoxStream<Tuple>> {
        let HashJoinStatus {
            full_schema_ref,
            build_map,
            ty,
            ..
        } = self;

        matches!(ty, JoinType::Left | JoinType::Full).then(|| {
            stream::iter(
                build_map
                    .drain()
                    .filter_map(|(_, (mut left_tuples, is_used))| {
                        if !is_used {
                            for tuple in left_tuples.iter_mut() {
                                tuple.schema_ref = full_schema_ref.clone();

                                while tuple.values.len() != full_schema_ref.len() {
                                    tuple.values.push(NULL_VALUE.clone());
                                }
                            }
                            return Some(left_tuples);
                        }
                        None
                    })
                    .flatten(),
            )
            .boxed()
        })
    }

    fn eval_keys(
        on_keys: &[ScalarExpression],
        tuple: &Tuple,
    ) -> Result<Vec<ValueRef>, DatabaseError> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval(tuple)?);
        }

        Ok(values)
    }
}

impl HashJoin {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
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

        if let Some(stream) = join_status.build_drop() {
            #[for_await]
            for tuple in stream {
                yield tuple
            }
        };
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
