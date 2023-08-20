use arrow::array::as_boolean_array;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_ap::volcano_executor::BoxedExecutor;
use crate::execution_ap::ExecutorError;
use crate::expression::ScalarExpression;

pub struct Filter { }

impl Filter {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(predicate: ScalarExpression, input: BoxedExecutor) {
        #[for_await]
        for batch in input {
            let batch = batch?;
            let predicate = predicate.eval_column(&batch)?;

            yield filter_record_batch(&batch, as_boolean_array(&predicate))?;
        }
    }
}