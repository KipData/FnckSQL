use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::catalog::CatalogError;
use crate::execution_v1::ExecutorError;
use crate::expression::ScalarExpression;
use crate::storage::{Storage, Table};
use crate::types::ColumnIdx;
use crate::types::value::DataValue;

pub struct Insert { }

impl Insert {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(table_name: String, col_idxs: Vec<ColumnIdx>, mut cols: Vec<Vec<ScalarExpression>>, storage: impl Storage) {
        if let Some(table) = storage.get_catalog().get_table_by_name(&table_name) {
            let row_len = cols[0].len();
            // 为了后继pop而倒序
            cols.reverse();

            let vec_arr_ref = table.all_columns()
                .into_iter()
                .map(|(col_idx, col_catalog)| {
                    if col_idxs.contains(&col_idx) {
                        let col = cols.pop().unwrap();
                        assert_eq!(col.len(), row_len);

                        let mut builder = DataValue::new_builder(&col[0].return_type())?;

                        for expr in col {
                            match expr {
                                ScalarExpression::Constant(value) => {
                                    DataValue::append_for_builder(&value, &mut builder)?;
                                },
                                _ => unreachable!()
                            }
                        }

                        Ok::<ArrayRef, ExecutorError>(builder.finish())
                    } else {
                        Ok(new_null_array(&DataType::from(col_catalog.datatype().clone()), row_len))
                    }
                })
                .try_collect()?;


            let new_batch = RecordBatch::try_new(table.schema(), vec_arr_ref)?;

            storage.get_table(table.id.unwrap())?.append(new_batch)?;
        } else {
            Err(CatalogError::NotFound("root", table_name.to_string()))?;
        }
    }
}