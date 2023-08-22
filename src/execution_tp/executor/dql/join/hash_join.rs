use ahash::{HashMap, HashMapExt, RandomState};
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::catalog::ColumnCatalog;
use crate::execution_ap::volcano_executor::join::joins_nullable;
use crate::execution_tp::executor::BoxedExecutor;
use crate::execution_tp::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct HashJoin { }

impl HashJoin {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(on: JoinCondition, ty: JoinType, left_input: BoxedExecutor, right_input: BoxedExecutor) {
        if ty == JoinType::Cross {
            unreachable!("Cross join should not be in HashJoinExecutor");
        }
        let ((on_left_keys, on_right_keys), filter): ((Vec<ScalarExpression>, Vec<ScalarExpression>), _) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => unreachable!("HashJoin must has on condition")
        };

        let mut join_columns = Vec::new();
        let hash_random_state = RandomState::with_seeds(0, 0, 0, 0);
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

        let mut left_map = HashMap::new();

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        let mut left_init_flag = false;
        #[for_await]
        for tuple in left_input {
            let tuple:Tuple = tuple?;
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
            let mut tuple: Tuple = tuple?;
            let right_cols_len = tuple.columns.len();
            let hash = Self::hash_row(&on_right_keys, &hash_random_state, &tuple);

            if !right_init_flag {
                Self::columns_filling(&tuple, &mut join_columns, right_force_nullable);
                right_init_flag = true;
            }

            //TODO: Repeat Join
            let mut tuple_option = if let Some(Tuple { mut values, .. }) = left_map
                .get_mut(&hash)
                .map(|tuples| tuples.remove(0))
            {
                values.append(&mut tuple.values);

                Some(Tuple { id: None, columns: join_columns.clone(), values })
            } else if matches!(ty, JoinType::Right | JoinType::Full) {
                let empty_len = join_columns.len() - right_cols_len;
                let values = join_columns[..empty_len]
                    .iter()
                    .map(|col| DataValue::none(col.datatype()))
                    .chain(tuple.values)
                    .collect_vec();

                Some(Tuple { id: None, columns: join_columns.clone(), values })
            } else {
                None
            };

            if let (Some(expr), Some(tuple), true) = (&filter, &mut tuple_option, !matches!(ty, JoinType::Full | JoinType::Cross)) {
                if let DataValue::Boolean(option) = expr.eval_column_tp(tuple) {
                    if let Some(false) | None = option {
                        let full_cols_len = tuple.columns.len();
                        let left_cols_len = full_cols_len - right_cols_len;

                        match ty {
                            JoinType::Left => {
                                for i in left_cols_len..full_cols_len {
                                    tuple.values[i] = DataValue::none(tuple.columns[i].datatype())
                                }
                            }
                            JoinType::Right => {
                                for i in 0..left_cols_len {
                                    tuple.values[i] = DataValue::none(tuple.columns[i].datatype())
                                }
                            }
                            _ => {
                                tuple_option = None;
                            }
                        }
                    }
                } else {
                    unreachable!("only bool");
                }
            }

            if let Some(tuple) = tuple_option {
                yield tuple;
            } else {
                continue
            }
        }

        if matches!(ty, JoinType::Left | JoinType::Full) {
            for (_, tuples) in left_map {
                for Tuple { mut values, columns, ..} in tuples {
                    let mut right_empties = join_columns[columns.len()..]
                        .iter()
                        .map(|col| DataValue::none(col.datatype()))
                        .collect_vec();

                    values.append(&mut right_empties);

                    yield Tuple { id: None, columns: join_columns.clone(), values }
                }
            }
        }
    }

    fn columns_filling(tuple: &Tuple, join_columns: &mut Vec<ColumnCatalog>, force_nullable: bool) {
        let mut new_columns = tuple.columns.iter()
            .cloned()
            .map(|mut col| {
                col.nullable = force_nullable;
                col
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
            .map(|expr| expr.eval_column_tp(tuple))
            .collect_vec();

        hash_random_state.hash_one(values)
    }
}