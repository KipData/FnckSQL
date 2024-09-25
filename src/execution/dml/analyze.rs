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
use std::collections::HashSet;
use std::fmt::Formatter;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;
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
                let dir_path = dirs::config_dir()
                    .expect("Your system does not have a Config directory!")
                    .join(DEFAULT_STATISTICS_META_PATH)
                    .join(table_name.as_str());
                // For DEBUG
                // println!("Statistics Path: {:#?}", dir_path);
                throw!(fs::create_dir_all(&dir_path).map_err(DatabaseError::IO));

                let mut active_index_paths = HashSet::new();

                for (index_id, _, builder) in builders {
                    let path = dir_path.join(index_id.to_string());
                    let temp_path = path.with_extension("tmp");
                    let path_str: String = path.to_string_lossy().into();
                    let (histogram, sketch) = throw!(builder.build(DEFAULT_NUM_OF_BUCKETS));
                    let meta = StatisticsMeta::new(histogram, sketch);

                    throw!(meta.to_file(&temp_path));
                    values.push(Arc::new(DataValue::Utf8 {
                        value: Some(path_str.clone()),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }));
                    throw!(transaction.save_table_meta(cache.1, &table_name, path_str, meta));
                    throw!(fs::rename(&temp_path, &path).map_err(DatabaseError::IO));

                    if let Some(file_name) = path.file_name() {
                        active_index_paths.insert(file_name.to_os_string());
                    }
                }

                // clean expired index
                for entry in throw!(fs::read_dir(dir_path).map_err(DatabaseError::IO)) {
                    let entry = throw!(entry.map_err(DatabaseError::IO));
                    let path = entry.path();

                    if let Some(file_name) = path.file_name() {
                        if !active_index_paths.remove(&file_name.to_os_string()) {
                            throw!(fs::remove_file(&path).map_err(DatabaseError::IO));
                        }
                    }
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

#[cfg(test)]
mod test {
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::execution::dml::analyze::{DEFAULT_NUM_OF_BUCKETS, DEFAULT_STATISTICS_META_PATH};
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use std::ffi::OsStr;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_statistics_meta() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        let _ = fnck_sql.run("create table t1 (a int primary key, b int)")?;
        let _ = fnck_sql.run("create index b_index on t1 (b)")?;
        let _ = fnck_sql.run("create index p_index on t1 (a, b)")?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            let _ = fnck_sql.run(format!("insert into t1 values({i}, {})", i % 20))?;
        }
        let _ = fnck_sql.run("analyze table t1")?;

        let dir_path = dirs::config_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_STATISTICS_META_PATH)
            .join("t1");

        let mut paths = fs::read_dir(&dir_path)?;

        let statistics_meta_pk_index = StatisticsMeta::from_file(paths.next().unwrap()?.path())?;

        assert_eq!(statistics_meta_pk_index.index_id(), 0);
        assert_eq!(statistics_meta_pk_index.histogram().values_len(), 101);

        let statistics_meta_b_index = StatisticsMeta::from_file(paths.next().unwrap()?.path())?;

        assert_eq!(statistics_meta_b_index.index_id(), 1);
        assert_eq!(statistics_meta_b_index.histogram().values_len(), 101);

        let statistics_meta_p_index = StatisticsMeta::from_file(paths.next().unwrap()?.path())?;

        assert_eq!(statistics_meta_p_index.index_id(), 2);
        assert_eq!(statistics_meta_p_index.histogram().values_len(), 101);

        Ok(())
    }

    #[test]
    fn test_clean_expired_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        let _ = fnck_sql.run("create table t1 (a int primary key, b int)")?;
        let _ = fnck_sql.run("create index b_index on t1 (b)")?;
        let _ = fnck_sql.run("create index p_index on t1 (a, b)")?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            let _ = fnck_sql.run(format!("insert into t1 values({i}, {i})"))?;
        }
        let _ = fnck_sql.run("analyze table t1")?;

        let dir_path = dirs::config_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_STATISTICS_META_PATH)
            .join("t1");

        let mut paths = fs::read_dir(&dir_path)?;

        assert_eq!(paths.next().unwrap()?.file_name(), OsStr::new("0"));
        assert_eq!(paths.next().unwrap()?.file_name(), OsStr::new("1"));
        assert_eq!(paths.next().unwrap()?.file_name(), OsStr::new("2"));
        assert!(paths.next().is_none());

        let _ = fnck_sql.run("alter table t1 drop column b")?;
        let _ = fnck_sql.run("analyze table t1")?;

        let mut paths = fs::read_dir(&dir_path)?;

        assert_eq!(paths.next().unwrap()?.file_name(), OsStr::new("0"));
        assert!(paths.next().is_none());

        Ok(())
    }
}
