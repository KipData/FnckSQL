use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::throw;
use crate::types::index::IndexMetaRef;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::fmt::Formatter;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, fs};

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

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Analyze {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Analyze {
                    table_name,
                    mut input,
                    index_metas,
                } = self;

                let schema = input.output_schema().clone();
                let mut builders = Vec::with_capacity(index_metas.len());
                let table = throw!(transaction
                    .table(cache.0, table_name.clone())
                    .cloned()
                    .ok_or(DatabaseError::TableNotFound));

                for index in table.indexes() {
                    builders.push((
                        index.id,
                        throw!(index.column_exprs(&table)),
                        throw!(HistogramBuilder::new(index, None)),
                    ));
                }

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);

                    for (_, exprs, builder) in builders.iter_mut() {
                        let values = throw!(Projection::projection(&tuple, exprs, &schema));

                        if values.len() == 1 {
                            throw!(builder.append(&values[0]));
                        } else {
                            throw!(builder.append(&Arc::new(DataValue::Tuple(Some(values)))));
                        }
                    }
                }
                drop(coroutine);
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
                throw!(fs::create_dir_all(&dir_path).map_err(DatabaseError::IO));

                for (index_id, _, builder) in builders {
                    let path: String = dir_path.join(index_id.to_string()).to_string_lossy().into();
                    let (histogram, sketch) = throw!(builder.build(DEFAULT_NUM_OF_BUCKETS));
                    let meta = StatisticsMeta::new(histogram, sketch);

                    throw!(meta.to_file(&path));
                    values.push(Arc::new(DataValue::Utf8 {
                        value: Some(path.clone()),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }));
                    throw!(transaction.save_table_meta(cache.1, &table_name, path, meta));
                }
                yield Ok(Tuple { id: None, values });
            },
        )
    }
}

impl fmt::Display for AnalyzeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let indexes = self.index_metas.iter().map(|index| &index.name).join(", ");

        write!(f, "Analyze {} -> [{}]", self.table_name, indexes)?;

        Ok(())
    }
}
