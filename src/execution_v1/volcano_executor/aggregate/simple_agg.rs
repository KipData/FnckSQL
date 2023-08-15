use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use futures_async_stream::try_stream;
use itertools::Itertools;
use arrow::record_batch::RecordBatch;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::aggregate::create_accumulators;
use crate::execution_v1::volcano_executor::BoxedExecutor;
use crate::expression::ScalarExpression;

pub struct Agg{}

impl Agg {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(exprs: Vec<ScalarExpression>, input: BoxedExecutor){
        let mut acc = create_accumulators(&exprs);

        let mut agg_fileds: Option<Vec<Field>> = None;
        #[for_await]
        for batch in input{
            let batch = batch?;
            // only support one epxrssion in aggregation, not supported example: `sum(distinct a)`
            let columns: Result<Vec<_>, ExecutorError> = exprs
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => {
                        // TODO: Only single-argument aggregate functions are supported
                        // 处理 ScalarExpression::AggCall 类型的表达式
                        args[0].eval_column(&batch)
                    }
                    // 处理其他类型的表达式
                    _ => unimplemented!(),
                })
                .try_collect();
            // build new schema for aggregation result
            if agg_fileds.is_none() {
                agg_fileds = Some(
                    exprs.iter()
                        .map(|expr| expr.output_columns().to_field())
                        .collect(),
                )
            }
            let columns = columns?;
            // sumAcc[0].update_batch(&columns[0]);
            for (acc, column) in acc.iter_mut().zip_eq(columns.iter()) {
                acc.update_batch(column)?;
            }
        }
        let mut columns: Vec<ArrayRef> = Vec::new();
        for acc in acc.iter() {
            let res = acc.evaluate()?;
            columns.push(res.to_array_of_size(1));
        }
        let schema = SchemaRef::new(Schema::new(agg_fileds.unwrap()));
        yield RecordBatch::try_new(schema, columns)?;

    }
}