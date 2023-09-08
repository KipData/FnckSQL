use std::sync::Arc;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt, RandomState};
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::execution::executor::dql::join::joins_nullable;
use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: BoxedExecutor,
    right_input: BoxedExecutor
}

impl From<(JoinOperator, BoxedExecutor, BoxedExecutor)> for HashJoin {
    fn from((JoinOperator { on, join_type }, left_input, right_input): (JoinOperator, BoxedExecutor, BoxedExecutor)) -> Self {
        HashJoin {
            on,
            ty: join_type,
            left_input,
            right_input,
        }
    }
}

impl<S: Storage> Executor<S> for HashJoin {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl HashJoin {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let HashJoin { on, ty, left_input, right_input } = self;

        if ty == JoinType::Cross {
            unreachable!("Cross join should not be in HashJoinExecutor");
        }
        let ((on_left_keys, on_right_keys), filter): ((Vec<ScalarExpression>, Vec<ScalarExpression>), _) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => unreachable!("HashJoin must has on condition")
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
        for tuple in left_input {
            let tuple: Tuple = tuple?;
            let hash = Self::hash_row(&on_left_keys, &hash_random_state, &tuple);

            if !left_init_flag {
                Self::columns_filling(&tuple, &mut join_columns, left_force_nullable);
                left_init_flag = true;
            }

            left_map
                .entry(hash)
                .or_insert(Vec::new())
                .push(tuple);
        }

        // probe phase
        let mut right_init_flag = false;
        #[for_await]
        for tuple in right_input {
            let tuple: Tuple = tuple?;
            let right_cols_len = tuple.columns.len();
            let hash = Self::hash_row(&on_right_keys, &hash_random_state, &tuple);

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

                        Tuple { id: None, columns: join_columns.clone(), values: full_values }
                    })
                    .collect_vec()
            } else if matches!(ty, JoinType::Right | JoinType::Full) {
                let empty_len = join_columns.len() - right_cols_len;
                let values = join_columns[..empty_len]
                    .iter()
                    .map(|col| Arc::new(DataValue::none(col.datatype())))
                    .chain(tuple.values)
                    .collect_vec();

                vec![Tuple { id: None, columns: join_columns.clone(), values }]
            } else {
                vec![]
            };

            // on filter
            if let (Some(expr), false) = (&filter, join_tuples.is_empty() || matches!(ty, JoinType::Full | JoinType::Cross)) {
                let mut filter_tuples = Vec::with_capacity(join_tuples.len());

                for mut tuple in join_tuples {
                    if let DataValue::Boolean(option) = expr.eval_column(&tuple).as_ref() {
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
                                _ => ()
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
                    continue
                }

                for Tuple { mut values, columns, ..} in tuples {
                    let mut right_empties = join_columns[columns.len()..]
                        .iter()
                        .map(|col| Arc::new(DataValue::none(col.datatype())))
                        .collect_vec();

                    values.append(&mut right_empties);

                    yield Tuple { id: None, columns: join_columns.clone(), values }
                }
            }
        }
    }

    fn columns_filling(tuple: &Tuple, join_columns: &mut Vec<ColumnRef>, force_nullable: bool) {
        let mut new_columns = tuple.columns.iter()
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
        tuple: &Tuple
    ) -> u64 {
        let values = on_keys
            .iter()
            .map(|expr| expr.eval_column(tuple))
            .collect_vec();

        hash_random_state.hash_one(values)
    }
}