use sqlparser::ast::Statement;
use std::path::PathBuf;

use crate::binder::{Binder, BinderContext};
use crate::errors::DatabaseError;
use crate::execution::volcano::{build_write, try_collect};
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::kip::KipStorage;
use crate::storage::{Storage, Transaction};
use crate::types::tuple::Tuple;

#[derive(Copy, Clone)]
pub enum QueryExecute {
    Volcano,
    Codegen,
}

pub struct Database<S: Storage> {
    pub storage: S,
}

impl Database<KipStorage> {
    /// Create a new Database instance With KipDB.
    pub async fn with_kipdb(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        let storage = KipStorage::new(path).await?;

        Ok(Database { storage })
    }
}

impl Database<KipStorage> {
    pub async fn run_on_query<S: AsRef<str>>(
        &self,
        sql: S,
        query_execute: QueryExecute,
    ) -> Result<Vec<Tuple>, DatabaseError> {
        match query_execute {
            QueryExecute::Volcano => self.run(sql).await,
            QueryExecute::Codegen => {
                #[cfg(feature = "codegen_execute")]
                {
                    use crate::execution::codegen::execute;
                    use std::sync::Arc;

                    let transaction = self.storage.transaction().await?;
                    let (plan, statement) = Self::build_plan(sql, &transaction)?;

                    if matches!(statement, Statement::Query(_)) {
                        let transaction = Arc::new(transaction);

                        let tuples = execute(plan, transaction.clone()).await?;
                        Arc::into_inner(transaction).unwrap().commit().await?;

                        Ok(tuples)
                    } else {
                        Self::run_volcano(transaction, plan).await
                    }
                }
                #[cfg(not(feature = "codegen_execute"))]
                {
                    unreachable!("open feature: `codegen_execute` plz")
                }
            }
        }
    }
}

impl<S: Storage> Database<S> {
    /// Create a new Database instance.
    pub fn new(storage: S) -> Result<Self, DatabaseError> {
        Ok(Database { storage })
    }

    /// Run SQL queries.
    pub async fn run<T: AsRef<str>>(&self, sql: T) -> Result<Vec<Tuple>, DatabaseError> {
        let transaction = self.storage.transaction().await?;
        let (plan, _) = Self::build_plan::<T, S::TransactionType>(sql, &transaction)?;

        Self::run_volcano(transaction, plan).await
    }

    pub(crate) async fn run_volcano(
        mut transaction: <S as Storage>::TransactionType,
        plan: LogicalPlan,
    ) -> Result<Vec<Tuple>, DatabaseError> {
        let mut stream = build_write(plan, &mut transaction);
        let tuples = try_collect(&mut stream).await?;

        drop(stream);
        transaction.commit().await?;

        Ok(tuples)
    }

    pub async fn new_transaction(&self) -> Result<DBTransaction<S>, DatabaseError> {
        let transaction = self.storage.transaction().await?;

        Ok(DBTransaction { inner: transaction })
    }

    pub fn build_plan<V: AsRef<str>, T: Transaction>(
        sql: V,
        transaction: &<S as Storage>::TransactionType,
    ) -> Result<(LogicalPlan, Statement), DatabaseError> {
        // parse
        let mut stmts = parse_sql(sql)?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        let mut binder = Binder::new(BinderContext::new(transaction));
        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);

        let best_plan =
            Self::default_optimizer(source_plan).find_best(Some(&transaction.meta_loader()))?;
        // println!("best_plan plan: {:#?}", best_plan);

        Ok((best_plan, stmts.remove(0)))
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
}

impl<S: Storage> DBTransaction<S> {
    pub async fn run<T: AsRef<str>>(&mut self, sql: T) -> Result<Vec<Tuple>, DatabaseError> {
        let (plan, _) = Database::<S>::build_plan::<T, S::TransactionType>(sql, &self.inner)?;
        let mut stream = build_write(plan, &mut self.inner);

        Ok(try_collect(&mut stream).await?)
    }

    pub async fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{Database, DatabaseError, QueryExecute};
    use crate::storage::{Storage, Transaction};
    use crate::types::tuple::{create_table, Tuple};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn build_table(mut transaction: impl Transaction) -> Result<(), DatabaseError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                None,
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
                None,
            ),
        ];
        let _ = transaction.create_table(Arc::new("t1".to_string()), columns, false)?;
        transaction.commit().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = Database::with_kipdb(temp_dir.path()).await?;
        let transaction = database.storage.transaction().await?;
        build_table(transaction).await?;

        let batch = database.run("select * from t1").await?;

        println!("{:#?}", batch);
        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = Database::with_kipdb(temp_dir.path()).await?;

        let mut tx_1 = fnck_sql.new_transaction().await?;
        let mut tx_2 = fnck_sql.new_transaction().await?;

        let _ = tx_1
            .run("create table t1 (a int primary key, b int)")
            .await?;
        let _ = tx_2
            .run("create table t1 (c int primary key, d int)")
            .await?;

        let _ = tx_1.run("insert into t1 values(0, 0)").await?;
        let _ = tx_1.run("insert into t1 values(1, 1)").await?;

        let _ = tx_2.run("insert into t1 values(2, 2)").await?;
        let _ = tx_2.run("insert into t1 values(3, 3)").await?;

        let tuples_1 = tx_1.run("select * from t1").await?;
        let tuples_2 = tx_2.run("select * from t1").await?;

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
                Arc::new(DataValue::Int32(Some(2))),
                Arc::new(DataValue::Int32(Some(2)))
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

        Ok(())
    }

    #[tokio::test]
    async fn test_crud_sql() -> Result<(), DatabaseError> {
        #[cfg(not(feature = "codegen_execute"))]
        {
            let _ = crate::db::test::_test_crud_sql(QueryExecute::Volcano).await?;
        }
        #[cfg(feature = "codegen_execute")]
        {
            let mut results_1 = _test_crud_sql(QueryExecute::Volcano).await?;
            let mut results_2 = _test_crud_sql(QueryExecute::Codegen).await?;

            assert_eq!(results_1.len(), results_2.len());

            for i in 0..results_1.len() {
                results_1[i].sort_by_key(|tuple: &Tuple| tuple.serialize_to());
                results_2[i].sort_by_key(|tuple: &Tuple| tuple.serialize_to());

                if results_1[i] != results_2[i] {
                    panic!(
                        "Index: {i} Tuples not match! \n Volcano: \n{}\n Codegen: \n{}",
                        create_table(&results_1[i]),
                        create_table(&results_2[i])
                    );
                }
            }
        }

        Ok(())
    }

    async fn _test_crud_sql(query_execute: QueryExecute) -> Result<Vec<Vec<Tuple>>, DatabaseError> {
        let mut results = Vec::new();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = Database::with_kipdb(temp_dir.path()).await?;

        let _ = fnck_sql.run_on_query("create table t1 (a int primary key, b int unique null, k int, z varchar unique null)", query_execute).await?;
        let _ = fnck_sql
            .run_on_query(
                "create table t2 (c int primary key, d int unsigned null, e datetime)",
                query_execute,
            )
            .await?;
        let _ = fnck_sql.run_on_query("insert into t1 (a, b, k, z) values (-99, 1, 1, 'k'), (-1, 2, 2, 'i'), (5, 3, 2, 'p'), (29, 4, 2, 'db')", query_execute).await?;
        let _ = fnck_sql.run_on_query("insert into t2 (d, c, e) values (2, 1, '2021-05-20 21:00:00'), (3, 4, '2023-09-10 00:00:00')", query_execute).await?;
        let _ = fnck_sql
            .run_on_query(
                "create table t3 (a int primary key, b decimal(4,2))",
                query_execute,
            )
            .await?;
        let _ = fnck_sql
            .run_on_query(
                "insert into t3 (a, b) values (1, 1111), (2, 2.01), (3, 3.00)",
                query_execute,
            )
            .await?;
        let _ = fnck_sql
            .run_on_query(
                "insert into t3 (a, b) values (4, 4444), (5, 5222), (6, 1.00)",
                query_execute,
            )
            .await?;

        println!("show tables:");
        let tuples_show_tables = fnck_sql.run_on_query("show tables", query_execute).await?;
        println!("{}", create_table(&tuples_show_tables));
        results.push(tuples_show_tables);

        println!("full t1:");
        let tuples_full_fields_t1 = fnck_sql
            .run_on_query("select * from t1", query_execute)
            .await?;
        println!("{}", create_table(&tuples_full_fields_t1));
        results.push(tuples_full_fields_t1);

        println!("full t2:");
        let tuples_full_fields_t2 = fnck_sql
            .run_on_query("select * from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_full_fields_t2));
        results.push(tuples_full_fields_t2);

        println!("projection_and_filter:");
        let tuples_projection_and_filter = fnck_sql
            .run_on_query("select a from t1 where b > 1", query_execute)
            .await?;
        println!("{}", create_table(&tuples_projection_and_filter));
        results.push(tuples_projection_and_filter);

        println!("projection_and_sort:");
        let tuples_projection_and_sort = fnck_sql
            .run_on_query("select * from t1 order by a, b", query_execute)
            .await?;
        println!("{}", create_table(&tuples_projection_and_sort));
        results.push(tuples_projection_and_sort);

        println!("like t1 1:");
        let tuples_like_1_t1 = fnck_sql
            .run_on_query("select * from t1 where z like '%k'", query_execute)
            .await?;
        println!("{}", create_table(&tuples_like_1_t1));
        results.push(tuples_like_1_t1);

        println!("like t1 2:");
        let tuples_like_2_t1 = fnck_sql
            .run_on_query("select * from t1 where z like '_b'", query_execute)
            .await?;
        println!("{}", create_table(&tuples_like_2_t1));
        results.push(tuples_like_2_t1);

        println!("not like t1:");
        let tuples_not_like_t1 = fnck_sql
            .run_on_query("select * from t1 where z not like '_b'", query_execute)
            .await?;
        println!("{}", create_table(&tuples_not_like_t1));
        results.push(tuples_not_like_t1);

        println!("in t1:");
        let tuples_in_t1 = fnck_sql
            .run_on_query("select * from t1 where a in (5, 29)", query_execute)
            .await?;
        println!("{}", create_table(&tuples_in_t1));
        results.push(tuples_in_t1);

        println!("not in t1:");
        let tuples_not_in_t1 = fnck_sql
            .run_on_query("select * from t1 where a not in (5, 29)", query_execute)
            .await?;
        println!("{}", create_table(&tuples_not_in_t1));
        results.push(tuples_not_in_t1);

        println!("limit:");
        let tuples_limit = fnck_sql
            .run_on_query("select * from t1 limit 1 offset 1", query_execute)
            .await?;
        println!("{}", create_table(&tuples_limit));
        results.push(tuples_limit);

        println!("inner join:");
        let tuples_inner_join = fnck_sql
            .run_on_query("select * from t1 inner join t2 on a = c", query_execute)
            .await?;
        println!("{}", create_table(&tuples_inner_join));
        results.push(tuples_inner_join);

        println!("left join:");
        let tuples_left_join = fnck_sql
            .run_on_query("select * from t1 left join t2 on a = c", query_execute)
            .await?;
        println!("{}", create_table(&tuples_left_join));
        results.push(tuples_left_join);

        println!("right join:");
        let tuples_right_join = fnck_sql
            .run_on_query("select * from t1 right join t2 on a = c", query_execute)
            .await?;
        println!("{}", create_table(&tuples_right_join));
        results.push(tuples_right_join);

        println!("full join:");
        let tuples_full_join = fnck_sql
            .run_on_query("select * from t1 full join t2 on a = c", query_execute)
            .await?;
        println!("{}", create_table(&tuples_full_join));
        results.push(tuples_full_join);

        println!("count agg:");
        let tuples_count_agg = fnck_sql
            .run_on_query("select count(d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_count_agg));
        results.push(tuples_count_agg);

        println!("count wildcard agg:");
        let tuples_count_wildcard_agg = fnck_sql
            .run_on_query("select count(*) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_count_wildcard_agg));
        results.push(tuples_count_wildcard_agg);

        println!("count distinct agg:");
        let tuples_count_distinct_agg = fnck_sql
            .run_on_query("select count(distinct d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_count_distinct_agg));
        results.push(tuples_count_distinct_agg);

        println!("sum agg:");
        let tuples_sum_agg = fnck_sql
            .run_on_query("select sum(d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_sum_agg));
        results.push(tuples_sum_agg);

        println!("sum distinct agg:");
        let tuples_sum_distinct_agg = fnck_sql
            .run_on_query("select sum(distinct d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_sum_distinct_agg));
        results.push(tuples_sum_distinct_agg);

        println!("avg agg:");
        let tuples_avg_agg = fnck_sql
            .run_on_query("select avg(d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_avg_agg));
        results.push(tuples_avg_agg);

        println!("min_max agg:");
        let tuples_min_max_agg = fnck_sql
            .run_on_query("select min(d), max(d) from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_min_max_agg));
        results.push(tuples_min_max_agg);

        println!("group agg:");
        let tuples_group_agg = fnck_sql
            .run_on_query(
                "select c, max(d) from t2 group by c having c = 1",
                query_execute,
            )
            .await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("alias:");
        let tuples_group_agg = fnck_sql
            .run_on_query("select c as o from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_group_agg));
        results.push(tuples_group_agg);

        println!("alias agg:");
        let tuples_group_agg = fnck_sql
            .run_on_query(
                "select c, max(d) as max_d from t2 group by c having c = 1",
                query_execute,
            )
            .await?;
        println!("{}", create_table(&tuples_group_agg));
        results.push(tuples_group_agg);

        println!("time max:");
        let tuples_time_max = fnck_sql
            .run_on_query("select max(e) as max_time from t2", query_execute)
            .await?;
        println!("{}", create_table(&tuples_time_max));
        results.push(tuples_time_max);

        println!("time where:");
        let tuples_time_where_t2 = fnck_sql
            .run_on_query(
                "select (c + 1) from t2 where e > '2021-05-20'",
                query_execute,
            )
            .await?;
        println!("{}", create_table(&tuples_time_where_t2));
        results.push(tuples_time_where_t2);

        assert!(fnck_sql
            .run_on_query("select max(d) from t2 group by c", query_execute)
            .await
            .is_err());

        println!("distinct t1:");
        let tuples_distinct_t1 = fnck_sql
            .run_on_query("select distinct b, k from t1", query_execute)
            .await?;
        println!("{}", create_table(&tuples_distinct_t1));
        results.push(tuples_distinct_t1);

        println!("update t1 with filter:");
        let _ = fnck_sql
            .run_on_query("update t1 set b = 0 where b = 1", query_execute)
            .await?;
        println!("after t1:");

        let update_after_full_t1 = fnck_sql
            .run_on_query("select * from t1", query_execute)
            .await?;
        println!("{}", create_table(&update_after_full_t1));
        results.push(update_after_full_t1);

        println!("insert overwrite t1:");
        let _ = fnck_sql
            .run_on_query(
                "insert overwrite t1 (a, b, k) values (-99, 1, 0)",
                query_execute,
            )
            .await?;
        println!("after t1:");
        let insert_overwrite_after_full_t1 = fnck_sql
            .run_on_query("select * from t1", query_execute)
            .await?;
        println!("{}", create_table(&insert_overwrite_after_full_t1));
        results.push(insert_overwrite_after_full_t1);

        assert!(fnck_sql
            .run_on_query(
                "insert overwrite t1 (a, b, k) values (-1, 1, 0)",
                query_execute
            )
            .await
            .is_err());

        println!("delete t1 with filter:");
        let _ = fnck_sql
            .run_on_query("delete from t1 where b = 0", query_execute)
            .await?;
        println!("after t1:");
        let delete_after_full_t1 = fnck_sql
            .run_on_query("select * from t1", query_execute)
            .await?;
        println!("{}", create_table(&delete_after_full_t1));
        results.push(delete_after_full_t1);

        println!("trun_on_querycate t1:");
        let _ = fnck_sql.run_on_query("truncate t1", query_execute).await?;

        println!("drop t1:");
        let _ = fnck_sql
            .run_on_query("drop table t1", query_execute)
            .await?;

        println!("decimal:");
        let tuples_decimal = fnck_sql
            .run_on_query("select * from t3", query_execute)
            .await?;
        println!("{}", create_table(&tuples_decimal));
        results.push(tuples_decimal);

        Ok(results)
    }
}
