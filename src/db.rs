use ahash::HashMap;
use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use sqlparser::ast::Statement;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::binder::{command_type, Binder, BinderContext, CommandType};
use crate::errors::DatabaseError;
use crate::execution::volcano::{build_write, try_collect};
use crate::expression::function::{FunctionSummary, ScalarFunctionImpl};
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::kip::KipStorage;
use crate::storage::{Storage, Transaction};
use crate::types::tuple::{SchemaRef, Tuple};

pub(crate) type Functions = HashMap<FunctionSummary, Arc<dyn ScalarFunctionImpl>>;

#[allow(dead_code)]
pub(crate) enum MetaDataLock {
    Read(RwLockReadGuardArc<()>),
    Write(RwLockWriteGuardArc<()>),
}

pub struct DataBaseBuilder {
    path: PathBuf,
    functions: Functions,
}

impl DataBaseBuilder {
    pub fn path(path: impl Into<PathBuf> + Send) -> Self {
        DataBaseBuilder {
            path: path.into(),
            functions: Default::default(),
        }
    }

    pub fn register_function(mut self, function: Arc<dyn ScalarFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.functions.insert(summary, function);
        self
    }

    pub async fn build(self) -> Result<Database<KipStorage>, DatabaseError> {
        let storage = KipStorage::new(self.path).await?;

        Ok(Database {
            storage,
            functions: Arc::new(self.functions),
            mdl: Arc::new(RwLock::new(())),
        })
    }
}

pub struct Database<S: Storage> {
    pub storage: S,
    functions: Arc<Functions>,
    mdl: Arc<RwLock<()>>,
}

impl<S: Storage> Database<S> {
    /// Run SQL queries.
    pub async fn run<T: AsRef<str>>(
        &self,
        sql: T,
    ) -> Result<(SchemaRef, Vec<Tuple>), DatabaseError> {
        // parse
        let stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let stmt = &stmts[0];
        let _guard = if matches!(command_type(stmt)?, CommandType::DDL) {
            MetaDataLock::Write(self.mdl.write_arc().await)
        } else {
            MetaDataLock::Read(self.mdl.read_arc().await)
        };
        let transaction = self.storage.transaction().await?;
        let plan = Self::build_plan(stmt, &transaction, &self.functions)?;

        Self::run_volcano(transaction, plan).await
    }

    pub(crate) async fn run_volcano(
        mut transaction: <S as Storage>::TransactionType,
        mut plan: LogicalPlan,
    ) -> Result<(SchemaRef, Vec<Tuple>), DatabaseError> {
        let schema = plan.output_schema().clone();
        let mut stream = build_write(plan, &mut transaction);
        let tuples = try_collect(&mut stream).await?;

        drop(stream);
        transaction.commit().await?;

        Ok((schema, tuples))
    }

    pub async fn new_transaction(&self) -> Result<DBTransaction<S>, DatabaseError> {
        let guard = self.mdl.read_arc().await;
        let transaction = self.storage.transaction().await?;

        Ok(DBTransaction {
            inner: transaction,
            functions: self.functions.clone(),
            _guard: guard,
        })
    }

    pub(crate) fn build_plan(
        stmt: &Statement,
        transaction: &<S as Storage>::TransactionType,
        functions: &Functions,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut binder = Binder::new(
            BinderContext::new(transaction, functions, Arc::new(AtomicUsize::new(0))),
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

        let best_plan =
            Self::default_optimizer(source_plan).find_best(Some(&transaction.meta_loader()))?;
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
                "Combine Operators".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::CollapseProject,
                    NormalizationRuleImpl::CollapseGroupByAgg,
                    NormalizationRuleImpl::CombineFilter,
                ],
            )
            .batch(
                "Limit Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::LimitProjectTranspose,
                    NormalizationRuleImpl::PushLimitThroughJoin,
                    NormalizationRuleImpl::PushLimitIntoTableScan,
                    NormalizationRuleImpl::EliminateLimits,
                ],
            )
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ExpressionRemapper],
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

pub struct DBTransaction<S: Storage> {
    inner: S::TransactionType,
    functions: Arc<Functions>,
    _guard: RwLockReadGuardArc<()>,
}

impl<S: Storage> DBTransaction<S> {
    pub async fn run<T: AsRef<str>>(
        &mut self,
        sql: T,
    ) -> Result<(SchemaRef, Vec<Tuple>), DatabaseError> {
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
        let mut plan = Database::<S>::build_plan(stmt, &self.inner, &self.functions)?;

        let schema = plan.output_schema().clone();
        let mut stream = build_write(plan, &mut self.inner);

        Ok((schema, try_collect(&mut stream).await?))
    }

    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::{DataBaseBuilder, DatabaseError};
    use crate::expression::function::{FuncMonotonicity, FunctionSummary, ScalarFunctionImpl};
    use crate::expression::ScalarExpression;
    use crate::expression::{BinaryOperator, UnaryOperator};
    use crate::function;
    use crate::storage::{Storage, Transaction};
    use crate::types::tuple::{create_table, Tuple};
    use crate::types::value::{DataValue, ValueRef};
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn build_table(mut transaction: impl Transaction) -> Result<(), DatabaseError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
            ),
        ];
        let _ = transaction.create_table(Arc::new("t1".to_string()), columns, false)?;
        transaction.commit().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build().await?;
        let transaction = database.storage.transaction().await?;
        build_table(transaction).await?;

        let batch = database.run("select * from t1").await?;

        println!("{:#?}", batch);
        Ok(())
    }

    function!(TestFunction::test(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: ValueRef, v2: ValueRef| {
        let value = DataValue::binary_op(&v1, &v2, &BinaryOperator::Plus)?;
        DataValue::unary_op(&value, &UnaryOperator::Minus)
    });

    #[tokio::test]
    async fn test_udf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path())
            .register_function(TestFunction::new())
            .build()
            .await?;
        let _ = fnck_sql
            .run("CREATE TABLE test (id int primary key, c1 int, c2 int);")
            .await?;
        let _ = fnck_sql
            .run("INSERT INTO test VALUES (1, 2, 2), (0, 1, 1), (2, 1, 1), (3, 3, 3);")
            .await?;
        let (schema, tuples) = fnck_sql.run("select test(c1, 1) from test").await?;
        println!("{}", create_table(&schema, &tuples));

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build().await?;

        let _ = fnck_sql
            .run("create table t1 (a int primary key, b int)")
            .await?;

        let mut tx_1 = fnck_sql.new_transaction().await?;
        let mut tx_2 = fnck_sql.new_transaction().await?;

        let _ = tx_1.run("insert into t1 values(0, 0)").await?;
        let _ = tx_1.run("insert into t1 values(1, 1)").await?;

        let _ = tx_2.run("insert into t1 values(0, 0)").await?;
        let _ = tx_2.run("insert into t1 values(3, 3)").await?;

        let (_, tuples_1) = tx_1.run("select * from t1").await?;
        let (_, tuples_2) = tx_2.run("select * from t1").await?;

        assert_eq!(tuples_1.len(), 2);
        assert_eq!(tuples_2.len(), 2);

        assert_eq!(
            tuples_1[0].values,
            vec![
                Arc::new(DataValue::Int32(Some(0))),
                Arc::new(DataValue::Int32(Some(0)))
            ]
        );
        assert_eq!(
            tuples_1[1].values,
            vec![
                Arc::new(DataValue::Int32(Some(1))),
                Arc::new(DataValue::Int32(Some(1)))
            ]
        );

        assert_eq!(
            tuples_2[0].values,
            vec![
                Arc::new(DataValue::Int32(Some(0))),
                Arc::new(DataValue::Int32(Some(0)))
            ]
        );
        assert_eq!(
            tuples_2[1].values,
            vec![
                Arc::new(DataValue::Int32(Some(3))),
                Arc::new(DataValue::Int32(Some(3)))
            ]
        );

        tx_1.commit().await?;

        assert!(tx_2.commit().await.is_err());

        let mut tx_3 = fnck_sql.new_transaction().await?;
        let res = tx_3.run("create table t2 (a int primary key, b int)").await;
        assert!(res.is_err());

        Ok(())
    }
}
