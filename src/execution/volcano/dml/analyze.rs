use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::IndexMetaRef;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, fs};
use sqlparser::ast::CharLengthUnits;

const DEFAULT_NUM_OF_BUCKETS: usize = 100;
const DEFAULT_STATISTICS_META_PATH: &str = "fnck_sql_statistics_metas";

pub struct Analyze {
    table_name: TableName,
    input: LogicalPlan,
    index_metas: Vec<IndexMetaRef>,
}

impl From<(AnalyzeOperator, LogicalPlan)> for Analyze {
    fn from(
        (
            AnalyzeOperator {
                table_name,
                index_metas,
            },
            input,
        ): (AnalyzeOperator, LogicalPlan),
    ) -> Self {
        Analyze {
            table_name,
            input,
            index_metas,
        }
    }
}

impl<T: Transaction> WriteExecutor<T> for Analyze {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Analyze {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Analyze {
            table_name,
            mut input,
            index_metas,
        } = self;

        let schema = input.output_schema().clone();
        let mut builders = Vec::with_capacity(index_metas.len());
        let table = transaction
            .table(table_name.clone())
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;

        for index in table.indexes() {
            builders.push((
                index.id,
                index.column_exprs(&table)?,
                HistogramBuilder::new(index, None)?,
            ));
        }

        #[for_await]
        for tuple in build_read(input, transaction) {
            let tuple = tuple?;

            for (_, exprs, builder) in builders.iter_mut() {
                let values = Projection::projection(&tuple, exprs, &schema)?;

                if values.len() == 1 {
                    builder.append(&values[0])?;
                } else {
                    builder.append(&Arc::new(DataValue::Tuple(Some(values))))?;
                }
            }
        }
        let mut values = Vec::with_capacity(builders.len());
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("It's the end of the world!")
            .as_secs();
        let dir_path = dirs::config_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_STATISTICS_META_PATH)
            .join(table_name.as_str())
            .join(ts.to_string());
        fs::create_dir_all(&dir_path)?;

        for (index_id, _, builder) in builders {
            let path: String = dir_path.join(index_id.to_string()).to_string_lossy().into();
            let (histogram, sketch) = builder.build(DEFAULT_NUM_OF_BUCKETS)?;
            let meta = StatisticsMeta::new(histogram, sketch);

            meta.to_file(&path)?;
            values.push(Arc::new(DataValue::Utf8 {
                value: Some(path.clone()),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters
            }));
            transaction.save_table_meta(&table_name, path, meta)?;
        }
        yield Tuple { id: None, values };
    }
}

impl fmt::Display for AnalyzeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let indexes = self.index_metas.iter().map(|index| &index.name).join(", ");

        write!(f, "Analyze {} -> [{}]", self.table_name, indexes)?;

        Ok(())
    }
}
