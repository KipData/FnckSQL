use arrow::compute::{concat_batches, lexsort_to_indices, SortColumn, SortOptions, take, TakeOptions};
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_ap::ExecutorError;
use crate::execution_ap::volcano_executor::BoxedExecutor;
use crate::planner::operator::sort::SortField;

pub struct Sort { }

impl Sort {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(sort_fields: Vec<SortField>, limit: Option<usize>, input: BoxedExecutor) {
        let mut schema = None;
        let mut batches = vec![];

        #[for_await]
        for batch in input {
            let batch = batch?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let schema = schema.unwrap();
        let batch = concat_batches(&schema, &batches)?;

        let sort_columns = sort_fields
            .into_iter()
            .map(|SortField { expr, desc, nulls_first }| -> Result<SortColumn, ExecutorError> {
                let values = expr.eval_column(&batch)?;

                Ok(SortColumn {
                    values,
                    options: Some(SortOptions {
                        descending: desc,
                        nulls_first,
                    }),
                })
            })
            .try_collect::<Vec<_>>()?;

        let indices = lexsort_to_indices(&sort_columns, limit)?;

        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(
                    column,
                    &indices,
                    // for sqlrs: disable bound check overhead since indices are already generated from
                    // the same record batch
                    Some(TakeOptions { check_bounds: false, })
                )
            })
            .try_collect()?;

        yield RecordBatch::try_new(schema, columns)?;
    }
}