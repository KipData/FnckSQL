use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_v1::volcano_executor::BoxedExecutor;
use crate::execution_v1::ExecutorError;
use crate::expression::ScalarExpression;

pub struct Projection { }

impl Projection {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(exprs: Vec<ScalarExpression>, input: BoxedExecutor) {
        // FIXME: 支持JOIN投射
        // #[for_await]
        // for batch in input {
        //     let batch = batch?;
        //     let columns = exprs
        //         .iter()
        //         .map(|e| e.eval_column(&batch))
        //         .try_collect();
        //     let fields = exprs.iter().map(|e| e.eval_field(&batch)).collect();
        //     let schema = SchemaRef::new(Schema::new_with_metadata(
        //         fields,
        //         batch.schema().metadata().clone(),
        //     ));
        //     yield RecordBatch::try_new(schema, columns?)?;
        // }
        #[for_await]
        for batch in input {
            yield batch?;
        }
    }
}