use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;
use ahash::{HashMap, HashMapExt, RandomState};
use arrow::array::{ArrayRef, BooleanArray, new_null_array, PrimitiveArray, UInt32Builder};
use arrow::compute;
use arrow::datatypes::{Field, Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::BoxedExecutor;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::util::hash_utils::create_hashes;

pub struct HashJoin { }

impl HashJoin {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(on: JoinCondition, ty: JoinType, left_input: BoxedExecutor, right_input: BoxedExecutor) {
        if ty == JoinType::Cross {
            unreachable!("Cross join should not be in HashJoinExecutor");
        }
        let ((on_left_keys, on_right_keys), filter) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => unreachable!("HashJoin must has on condition")
        };

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left batches into single batch.
        let mut left_hashmap = HashMap::new();
        let mut left_row_offset = 0;
        let mut left_batches = vec![];

        let hash_random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut join_fields: Vec<Field> = Vec::new();
        let (left_force_nullable, right_force_nullable) = match ty {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => (true, true),
        };

        #[for_await]
        for batch in left_input {
            let batch: RecordBatch = batch?;
            let rows_hashes = Self::hash_columns(&on_left_keys, &hash_random_state, &batch)?;

            if join_fields.is_empty() {
                Self::filling_fields(&mut join_fields, left_force_nullable, &batch);
            }

            for (row, hash) in rows_hashes.iter().enumerate() {
                left_hashmap
                    .entry(*hash)
                    .or_insert_with(Vec::new)
                    .push(row + left_row_offset);
            }

            left_row_offset += batch.num_rows();
            left_batches.push(batch);
        }

        let left_single_batch = if !left_batches.is_empty() {
            Some(compute::concat_batches(&left_batches[0].schema(), &left_batches)?)
        } else {
            None
        };

        // probe phase
        //
        // build visited_left_side to record the left data has been visited,
        // because probe phase only visit the right data, so if we use left-join or full-join,
        // the left unvisited data should be returned to meet the join semantics.
        let full_left_side: BTreeSet<u32> = (0..left_row_offset as u32)
            .into_iter()
            .collect();
        let mut visited_left_side = BTreeSet::new();
        let mut join_schema = None;

        #[for_await]
        for batch in right_input {
            let batch = batch?;
            let rows_hashes = Self::hash_columns(&on_right_keys, &hash_random_state, &batch)?;

            // init join_schema
            let schema = Self::init_schema(
                &mut join_fields,
                &mut join_schema,
                right_force_nullable,
                &batch
            );

            // 1. build left and right indices
            let mut left_indices = UInt32Builder::new();
            let mut right_indices = UInt32Builder::new();
            // for sqlrs: Get the hash and find it in the build index
            // TODO: For every item on the left and right we check if it matches
            // This possibly contains rows with hash collisions,
            // So we have to check here whether rows are equal or not
            for (row, hash) in rows_hashes.iter().enumerate() {
                if let Some(indices) = left_hashmap.get(hash) {
                    for &i in indices {
                        left_indices.append_value(i as u32);
                        right_indices.append_value(row as u32);
                    }
                } else if ty == JoinType::Right || ty == JoinType::Full {
                    // when no match, add the row with None for the left side
                    left_indices.append_null();
                    right_indices.append_value(row as u32);
                }
            }

            // 2. build intermediate batch that from left and right all columns
            let mut left_indices = left_indices.finish();
            let mut right_indices = right_indices.finish();

            let mut intermediate_batch = Self::build_batch(
                &left_single_batch,
                &batch,
                schema,
                &left_indices,
                &right_indices
            )?;

            if let Some(ref expr) = filter {
                if !(ty == JoinType::Full || ty == JoinType::Cross) {
                    let predicate = expr.eval_column(&intermediate_batch)?
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .cloned()
                        .expect("join filter expected evaluate boolean array");
                    left_indices = PrimitiveArray::<UInt32Type>::from(
                        compute::filter(&left_indices, &predicate)?.data().clone(),
                    );
                    if ty == JoinType::Right {
                        let abs = left_indices.len().abs_diff(right_indices.len());
                        if abs > 0 {
                            left_indices = left_indices.into_iter()
                                .chain((0..abs).map(|_| None))
                                .collect();
                        }
                    } else {
                        right_indices = PrimitiveArray::<UInt32Type>::from(
                            compute::filter(&right_indices, &predicate)?.data().clone(),
                        );
                    }

                    intermediate_batch = Self::build_batch(
                        &left_single_batch,
                        &batch,
                        schema,
                        &left_indices,
                        &right_indices
                    )?;
                }
            }

            if ty == JoinType::Left || ty == JoinType::Full {
                left_indices
                    .iter()
                    .flatten()
                    .for_each(|i| {
                        let _ = visited_left_side.insert(i);
                    });
            }
            yield intermediate_batch;

            if ty == JoinType::Left && full_left_side.len() == visited_left_side.len() {
                break;
            }
        }

        if let Some(left_batch) = left_single_batch {
            if !(ty == JoinType::Left || ty == JoinType::Full) {
                return Ok(());
            }

            let schema = Self::init_schema(
                &mut join_fields,
                &mut join_schema,
                left_force_nullable,
                &left_batch
            ).clone();

            let indices: PrimitiveArray<UInt32Type> = PrimitiveArray::from_iter_values(
                full_left_side
                    .symmetric_difference(&visited_left_side)
                    .cloned()
            );

            let mut arrays: Vec<ArrayRef> = left_batch
                .columns()
                .iter()
                .map(|col| compute::take(col, &indices, None))
                .try_collect()?;
            let offset = arrays.len();
            for field in schema.fields()[offset..].iter() {
                arrays.push(new_null_array(field.data_type(), indices.len()));
            }

            yield RecordBatch::try_new(schema, arrays)?;
        }
    }

    fn init_schema<'a>(
        join_fields: &mut Vec<Field>,
        join_schema: &'a mut Option<Arc<Schema>>,
        force_nullable: bool,
        batch: &RecordBatch
    ) -> &'a mut Arc<Schema> {
        join_schema.get_or_insert_with(|| {
            Self::filling_fields(join_fields, force_nullable, &batch);

            Arc::new(Schema::new(mem::take(join_fields)))
        })
    }

    fn filling_fields(join_fields: &mut Vec<Field>, force_nullable: bool, batch: &RecordBatch) {
        let mut fields = batch.schema().fields()
            .into_iter()
            .map(|field| field.clone().with_nullable(force_nullable))
            .collect_vec();

        join_fields.append(&mut fields);
    }

    fn build_batch(
        left_single_batch: &Option<RecordBatch>,
        right_batch: &RecordBatch,
        schema: &mut Arc<Schema>,
        left_indices: &PrimitiveArray<UInt32Type>,
        right_indices: &PrimitiveArray<UInt32Type>
    ) -> Result<RecordBatch, ExecutorError> {
        let full_arrays = if let Some(left_batch) = left_single_batch {
            Self::select_with_indices(left_batch, &left_indices)?
        } else {
            vec![]
        }.into_iter()
            .chain(Self::select_with_indices(right_batch, &right_indices)?)
            .collect_vec();
        Ok(RecordBatch::try_new(schema.clone(), full_arrays)?)
    }

    fn select_with_indices(batch: &RecordBatch, indices: &PrimitiveArray<UInt32Type>) -> Result<Vec<ArrayRef>, ExecutorError> {
        Ok(batch
            .columns()
            .iter()
            .map(|col| compute::take(col, &indices, None))
            .try_collect()?)
    }

    fn hash_columns(
        col_keys: &Vec<ScalarExpression>,
        hash_random_state: &RandomState,
        batch: &RecordBatch
    ) -> Result<Vec<u64>, ExecutorError> {
        let arrays: Vec<ArrayRef> = col_keys
            .iter()
            .map(|expr| expr.eval_column(&batch))
            .try_collect()?;

        let mut every_rows_hashes = vec![0; batch.num_rows()];
        create_hashes(&arrays, &hash_random_state, &mut every_rows_hashes)?;

        Ok(every_rows_hashes)
    }
}