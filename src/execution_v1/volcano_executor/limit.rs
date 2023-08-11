use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::BoxedExecutor;

pub struct Limit {}

impl Limit {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(offset: Option<usize>, limit: Option<usize>, input: BoxedExecutor) {
        let offset_val = offset.unwrap_or(0);
        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let mut returned_count = 0;

        #[for_await]
        for batch in input {
            let batch = batch?;

            let cardinality = batch.num_rows();
            let limit_val = limit.unwrap_or(cardinality);

            let start = returned_count.max(offset_val) - returned_count;
            let end = {
                // from total returned rows level, the total_end is end index of whole returned
                // rows level.
                let total_end = offset_val + limit_val;
                let current_batch_end = returned_count + cardinality;
                // we choose the min of total_end and current_batch_end as the end index of to
                // match limit semantics.
                let real_end = total_end.min(current_batch_end);
                // to calculate the end index of current batch
                real_end - returned_count
            };
            returned_count += cardinality;

            // example: offset=1000, limit=2, cardinality=100
            // when first loop:
            // start = 0.max(1000)-0 = 1000
            // end = (1000+2).min(0+100)-0 = 100
            // so, start(1000) > end(100), we skip this loop batch.
            if start >= end {
                continue;
            }

            if (start..end) == (0..cardinality) {
                yield batch;
            } else {
                let length = end - start;
                yield batch.slice(start, length);
            }

            // dut to returned_count is always += cardinality, and returned_batch maybe slsliced,
            // so it will larger than real total_end.
            // example: offset=1, limit=4, cardinality=6, data=[(0..6)]
            // returned_count=6 > 1+4, meanwhile returned_batch size is 4 ([0..5])
            if returned_count >= offset_val + limit_val {
                break;
            }
        }
    }
}
