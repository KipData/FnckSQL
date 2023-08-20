use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::catalog::ColumnCatalog;
use crate::execution::ExecutorError;
use crate::execution::physical_plan::physical_values::PhysicalValues;
use crate::planner::operator::values::ValuesOperator;
use crate::types::value::DataValue;

pub struct Values { }

impl Values {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(plan: PhysicalValues) {
        let ValuesOperator { col_catalogs, rows } = plan.op;

        let fields = col_catalogs.iter()
            .map(ColumnCatalog::to_field)
            .collect_vec();

        // 行转列
        let mut cols: Vec<Vec<DataValue>> = vec![Vec::new(); rows[0].len()];

        for row in rows {
            for (i, value) in row.into_iter().enumerate() {
                cols[i].push(value);
            }
        }

        let vec_array = cols.into_iter()
            .map(|col| {
                let mut builder = DataValue::new_builder(&col[0].logical_type())?;

                for value in col {
                    DataValue::append_for_builder(&value, &mut builder)?
                }

                Ok::<ArrayRef, ExecutorError>(builder.finish())
            })
            .try_collect()?;

        yield RecordBatch::try_new(Arc::new(Schema::new(fields)), vec_array)?;
    }
}