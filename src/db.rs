use crate::binder::{command_type, Binder, BinderContext, CommandType};
use crate::errors::DatabaseError;
use crate::execution::{build_write, try_collect};
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::table::TableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::function::char_length::CharLength;
use crate::function::current_date::CurrentDate;
use crate::function::lower::Lower;
use crate::function::numbers::Numbers;
use crate::function::upper::Upper;
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::rocksdb::RocksStorage;
use crate::storage::{StatisticsMetaCache, Storage, TableCache, Transaction, ViewCache};
use crate::types::tuple::{SchemaRef, Tuple};
use crate::utils::lru::SharedLruCache;
use ahash::HashMap;
use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use sqlparser::ast::Statement;
use std::hash::RandomState;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub(crate) type ScalaFunctions = HashMap<FunctionSummary, Arc<dyn ScalarFunctionImpl>>;
pub(crate) type TableFunctions = HashMap<FunctionSummary, Arc<dyn TableFunctionImpl>>;

#[allow(dead_code)]
pub(crate) enum MetaDataLock {
    Read(ArcRwLockReadGuard<RawRwLock, ()>),
    Write(ArcRwLockWriteGuard<RawRwLock, ()>),
}

pub struct DataBaseBuilder {
    path: PathBuf,
    scala_functions: ScalaFunctions,
    table_functions: TableFunctions,
}

impl DataBaseBuilder {
    pub fn path(path: impl Into<PathBuf> + Send) -> Self {
        let mut builder = DataBaseBuilder {
            path: path.into(),
            scala_functions: Default::default(),
            table_functions: Default::default(),
        };
        builder = builder.register_scala_function(CharLength::new("char_length".to_lowercase()));
        builder =
            builder.register_scala_function(CharLength::new("character_length".to_lowercase()));
        builder = builder.register_scala_function(CurrentDate::new());
        builder = builder.register_scala_function(Lower::new());
        builder = builder.register_scala_function(Upper::new());
        builder = builder.register_table_function(Numbers::new());
        builder
    }

    pub fn register_scala_function(mut self, function: Arc<dyn ScalarFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.scala_functions.insert(summary, function);
        self
    }

    pub fn register_table_function(mut self, function: Arc<dyn TableFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.table_functions.insert(summary, function);
        self
    }

    pub fn build(self) -> Result<Database<RocksStorage>, DatabaseError> {
        let storage = RocksStorage::new(self.path)?;
        let meta_cache = Arc::new(SharedLruCache::new(256, 8, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(48, 4, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(12, 4, RandomState::new())?);

        Ok(Database {
            storage,
            scala_functions: Arc::new(self.scala_functions),
            table_functions: Arc::new(self.table_functions),
            mdl: Arc::new(RwLock::new(())),
            meta_cache,
            table_cache,
            view_cache,
        })
    }
}

pub struct Database<S: Storage> {
    pub(crate) storage: S,
    scala_functions: Arc<ScalaFunctions>,
    table_functions: Arc<TableFunctions>,
    mdl: Arc<RwLock<()>>,
    pub(crate) meta_cache: Arc<StatisticsMetaCache>,
    pub(crate) table_cache: Arc<TableCache>,
    pub(crate) view_cache: Arc<ViewCache>,
}

impl<S: Storage> Database<S> {
    /// Run SQL queries.
    pub fn run<T: AsRef<str>>(&self, sql: T) -> Result<(SchemaRef, Vec<Tuple>), DatabaseError> {
        // parse
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let stmt = &stmts[0];
        let _guard = if matches!(command_type(stmt)?, CommandType::DDL) {
            MetaDataLock::Write(self.mdl.write_arc())
        } else {
            MetaDataLock::Read(self.mdl.read_arc())
        };
        let mut transaction = self.storage.transaction()?;
        let mut plan = Self::build_plan(
            stmt,
            &self.table_cache,
            &self.view_cache,
            &self.meta_cache,
            &transaction,
            &self.scala_functions,
            &self.table_functions,
        )?;

        let schema = plan.output_schema().clone();
        let iterator = build_write(
            plan,
            (&self.table_cache, &self.view_cache, &self.meta_cache),
            &mut transaction,
        );
        let tuples = try_collect(iterator)?;

        transaction.commit()?;

        Ok((schema, tuples))
    }

    pub fn new_transaction(&self) -> Result<DBTransaction<S>, DatabaseError> {
        let guard = self.mdl.read_arc();
        let transaction = self.storage.transaction()?;

        Ok(DBTransaction {
            inner: transaction,
            scala_functions: self.scala_functions.clone(),
            table_functions: self.table_functions.clone(),
            _guard: guard,
            meta_cache: self.meta_cache.clone(),
            table_cache: self.table_cache.clone(),
            view_cache: self.view_cache.clone(),
        })
    }

    pub(crate) fn build_plan(
        stmt: &Statement,
        table_cache: &TableCache,
        view_cache: &ViewCache,
        meta_cache: &StatisticsMetaCache,
        transaction: &<S as Storage>::TransactionType<'_>,
        scala_functions: &ScalaFunctions,
        table_functions: &TableFunctions,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                transaction,
                scala_functions,
                table_functions,
                Arc::new(AtomicUsize::new(0)),
            ),
            None,
        );
        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(stmt)?;
        // println!("source_plan plan: {:#?}", source_plan);

        let best_plan = Self::default_optimizer(source_plan)
            .find_best(Some(&transaction.meta_loader(meta_cache)))?;
        // println!("best_plan plan: {:#?}", best_plan);

        Ok(best_plan)
    }

    pub(crate) fn default_optimizer(source_plan: LogicalPlan) -> HepOptimizer {
        HepOptimizer::new(source_plan)
            .batch(
                "Column Pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .batch(
                "Simplify Filter".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::SimplifyFilter,
                    NormalizationRuleImpl::ConstantCalculation,
                ],
            )
            .batch(
                "Predicate Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::PushPredicateThroughJoin,
                    NormalizationRuleImpl::PushPredicateIntoScan,
                ],
            )
            .batch(
                "Limit Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::LimitProjectTranspose,
                    NormalizationRuleImpl::PushLimitThroughJoin,
                    NormalizationRuleImpl::PushLimitIntoTableScan,
                ],
            )
            .batch(
                "Combine Operators".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::CollapseProject,
                    NormalizationRuleImpl::CollapseGroupByAgg,
                    NormalizationRuleImpl::CombineFilter,
                ],
            )
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::ExpressionRemapper,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
            .implementations(vec![
                // DQL
                ImplementationRuleImpl::SimpleAggregate,
                ImplementationRuleImpl::GroupByAggregate,
                ImplementationRuleImpl::Dummy,
                ImplementationRuleImpl::Filter,
                ImplementationRuleImpl::HashJoin,
                ImplementationRuleImpl::Limit,
                ImplementationRuleImpl::Projection,
                ImplementationRuleImpl::SeqScan,
                ImplementationRuleImpl::IndexScan,
                ImplementationRuleImpl::FunctionScan,
                ImplementationRuleImpl::Sort,
                ImplementationRuleImpl::Values,
                // DML
                ImplementationRuleImpl::Analyze,
                ImplementationRuleImpl::CopyFromFile,
                ImplementationRuleImpl::CopyToFile,
                ImplementationRuleImpl::Delete,
                ImplementationRuleImpl::Insert,
                ImplementationRuleImpl::Update,
                // DLL
                ImplementationRuleImpl::AddColumn,
                ImplementationRuleImpl::CreateTable,
                ImplementationRuleImpl::DropColumn,
                ImplementationRuleImpl::DropTable,
                ImplementationRuleImpl::Truncate,
            ])
    }
}

pub struct DBTransaction<'a, S: Storage + 'a> {
    inner: S::TransactionType<'a>,
    scala_functions: Arc<ScalaFunctions>,
    table_functions: Arc<TableFunctions>,
    _guard: ArcRwLockReadGuard<RawRwLock, ()>,
    pub(crate) meta_cache: Arc<StatisticsMetaCache>,
    pub(crate) table_cache: Arc<TableCache>,
    pub(crate) view_cache: Arc<ViewCache>,
}

impl<S: Storage> DBTransaction<'_, S> {
    pub fn run<T: AsRef<str>>(&mut self, sql: T) -> Result<(SchemaRef, Vec<Tuple>), DatabaseError> {
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let stmt = &stmts[0];
        if matches!(command_type(stmt)?, CommandType::DDL) {
            return Err(DatabaseError::UnsupportedStmt(
                "`DDL` is not allowed to execute within a transaction".to_string(),
            ));
        }
        let mut plan = Database::<S>::build_plan(
            stmt,
            &self.table_cache,
            &self.view_cache,
            &self.meta_cache,
            &self.inner,
            &self.scala_functions,
            &self.table_functions,
        )?;

        let schema = plan.output_schema().clone();
        let executor = build_write(
            plan,
            (&self.table_cache, &self.view_cache, &self.meta_cache),
            &mut self.inner,
        );

        Ok((schema, try_collect(executor)?))
    }

    pub fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::{DataBaseBuilder, DatabaseError};
    use crate::storage::{Storage, TableCache, Transaction};
    use crate::types::tuple::{create_table, Tuple};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use chrono::{Datelike, Local};
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) fn build_table<T: Transaction>(
        table_cache: &TableCache,
        transaction: &mut T,
    ) -> Result<(), DatabaseError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            ),
        ];
        let _ =
            transaction.create_table(table_cache, Arc::new("t1".to_string()), columns, false)?;

        Ok(())
    }

    #[test]
    fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build()?;
        let mut transaction = database.storage.transaction()?;

        build_table(&database.table_cache, &mut transaction)?;
        transaction.commit()?;

        let batch = database.run("select * from t1")?;

        println!("{:#?}", batch);
        Ok(())
    }

    /// use [CurrentDate](crate::function::current_date::CurrentDate) on this case
    #[test]
    fn test_udf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
        let (schema, tuples) = fnck_sql.run("select current_date()")?;
        println!("{}", create_table(&schema, &tuples));

        assert_eq!(
            schema,
            Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
                "current_date()".to_string(),
                true,
                ColumnDesc::new(LogicalType::Date, None, false, None).unwrap()
            ))])
        );
        assert_eq!(
            tuples,
            vec![Tuple {
                id: None,
                values: vec![DataValue::Date32(Some(Local::now().num_days_from_ce()))],
            }]
        );
        Ok(())
    }

    /// use [Numbers](crate::function::numbers::Numbers) on this case
    #[test]
    fn test_udtf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
        let (schema, tuples) = fnck_sql.run(
            "SELECT * FROM (select * from table(numbers(10)) a ORDER BY number LIMIT 5) OFFSET 3",
        )?;
        println!("{}", create_table(&schema, &tuples));

        let mut column = ColumnCatalog::new(
            "number".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        );
        let number_column_id = schema[0].id().unwrap();
        column.set_ref_table(Arc::new("a".to_string()), number_column_id, false);

        assert_eq!(schema, Arc::new(vec![ColumnRef::from(column)]));
        assert_eq!(
            tuples,
            vec![
                Tuple {
                    id: None,
                    values: vec![DataValue::Int32(Some(3))],
                },
                Tuple {
                    id: None,
                    values: vec![DataValue::Int32(Some(4))],
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        let _ = fnck_sql.run("create table t1 (a int primary key, b int)")?;

        let mut tx_1 = fnck_sql.new_transaction()?;
        let mut tx_2 = fnck_sql.new_transaction()?;

        let _ = tx_1.run("insert into t1 values(0, 0)")?;
        let _ = tx_1.run("insert into t1 values(1, 1)")?;

        let _ = tx_2.run("insert into t1 values(0, 0)")?;
        let _ = tx_2.run("insert into t1 values(3, 3)")?;

        let (_, tuples_1) = tx_1.run("select * from t1")?;
        let (_, tuples_2) = tx_2.run("select * from t1")?;

        assert_eq!(tuples_1.len(), 2);
        assert_eq!(tuples_2.len(), 2);

        assert_eq!(
            tuples_1[0].values,
            vec![DataValue::Int32(Some(0)), DataValue::Int32(Some(0))]
        );
        assert_eq!(
            tuples_1[1].values,
            vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(1))]
        );

        assert_eq!(
            tuples_2[0].values,
            vec![DataValue::Int32(Some(0)), DataValue::Int32(Some(0))]
        );
        assert_eq!(
            tuples_2[1].values,
            vec![DataValue::Int32(Some(3)), DataValue::Int32(Some(3))]
        );

        tx_1.commit()?;

        assert!(tx_2.commit().is_err());

        let mut tx_3 = fnck_sql.new_transaction()?;
        let res = tx_3.run("create table t2 (a int primary key, b int)");
        assert!(res.is_err());

        Ok(())
    }
}
