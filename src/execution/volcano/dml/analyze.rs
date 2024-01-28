use crate::catalog::{ColumnCatalog, ColumnRef, TableMeta, TableName};
use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::optimizer::core::column_meta::ColumnMeta;
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::OptimizerError;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_NUM_OF_BUCKETS: usize = 100;
const DEFAULT_COLUMN_METAS_PATH: &'static str = "kipsql_column_metas";

pub struct Analyze {
    table_name: TableName,
    input: BoxedExecutor,
    columns: Vec<ColumnRef>,
}

impl From<(AnalyzeOperator, BoxedExecutor)> for Analyze {
    fn from(
        (
            AnalyzeOperator {
                table_name,
                columns,
            },
            input,
        ): (AnalyzeOperator, BoxedExecutor),
    ) -> Self {
        Analyze {
            table_name,
            input,
            columns,
        }
    }
}

impl<T: Transaction> Executor<T> for Analyze {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl Analyze {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Analyze {
            table_name,
            input,
            columns,
        } = self;

        let mut builders = HashMap::with_capacity(columns.len());

        for column in &columns {
            builders.insert(column.id(), HistogramBuilder::new(column, None)?);
        }

        #[for_await]
        for tuple in input {
            let Tuple {
                columns, values, ..
            } = tuple?;

            for (i, column) in columns.iter().enumerate() {
                if !column.desc.is_index() {
                    continue;
                }

                if let Some(builder) = builders.get_mut(&column.id()) {
                    builder.append(&values[i])?
                }
            }
        }
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("It's the end of the world!")
            .as_secs();
        let dir_path = dirs::config_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_COLUMN_METAS_PATH)
            .join(table_name.as_str())
            .join(ts.to_string());
        fs::create_dir_all(&dir_path)?;

        let mut meta = TableMeta::empty(table_name.clone());

        for (column_id, builder) in builders {
            let path = dir_path.join(column_id.unwrap().to_string());
            let (histogram, sketch) = match builder.build(DEFAULT_NUM_OF_BUCKETS) {
                Ok(build) => build,
                Err(OptimizerError::TooManyBuckets) => continue,
                err => err?,
            };

            ColumnMeta::new(histogram, sketch).to_file(&path)?;

            meta.colum_meta_paths.push(path.to_string_lossy().into());
        }
        transaction.save_table_meta(&meta)?;

        let columns: Vec<ColumnRef> = vec![Arc::new(ColumnCatalog::new_dummy(
            "COLUMN_META_PATH".to_string(),
        ))];
        let values = meta
            .colum_meta_paths
            .into_iter()
            .map(|path| Arc::new(DataValue::Utf8(Some(path))))
            .collect_vec();

        yield Tuple {
            id: None,
            columns,
            values,
        };
    }
}
