use sqlparser::parser::ParserError;
use std::cell::RefCell;
use std::path::PathBuf;

use crate::binder::{BindError, Binder, BinderContext};
use crate::execution::executor::{build, try_collect};
use crate::execution::ExecutorError;
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::RuleImpl;
use crate::optimizer::OptimizerError;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::kip::KipStorage;
use crate::storage::{Storage, StorageError, Transaction};
use crate::types::tuple::Tuple;

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

impl<S: Storage> Database<S> {
    /// Create a new Database instance.
    pub fn new(storage: S) -> Result<Self, DatabaseError> {
        Ok(Database { storage })
    }

    /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        let transaction = self.storage.transaction().await?;

        // parse
        let stmts = parse_sql(sql)?;

        if stmts.is_empty() {
            return Ok(vec![]);
        }

        let binder = Binder::new(BinderContext::new(&transaction));

        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);

        let best_plan = Self::default_optimizer(source_plan).find_best()?;
        // println!("best_plan plan: {:#?}", best_plan);

        let transaction = RefCell::new(transaction);
        let mut stream = build(best_plan, &transaction);
        let tuples = try_collect(&mut stream).await?;

        transaction.into_inner().commit().await?;

        Ok(tuples)
    }

    fn default_optimizer(source_plan: LogicalPlan) -> HepOptimizer {
        HepOptimizer::new(source_plan)
            .batch(
                "Simplify Filter".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![RuleImpl::SimplifyFilter],
            )
            .batch(
                "Predicate Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::PushPredicateThroughJoin,
                    RuleImpl::PushPredicateIntoScan,
                ],
            )
            .batch(
                "Column Pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::PushProjectThroughChild,
                    RuleImpl::PushProjectIntoScan,
                ],
            )
            .batch(
                "Limit Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::LimitProjectTranspose,
                    RuleImpl::PushLimitThroughJoin,
                    RuleImpl::PushLimitIntoTableScan,
                    RuleImpl::EliminateLimits,
                ],
            )
            .batch(
                "Combine Operators".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![RuleImpl::CollapseProject, RuleImpl::CombineFilter],
            )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("parse error: {0}")]
    Parse(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bind error: {0}")]
    Bind(
        #[source]
        #[from]
        BindError,
    ),
    #[error("Storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        StorageError,
    ),
    #[error("executor error: {0}")]
    ExecutorError(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("optimizer error: {0}")]
    OptimizerError(
        #[source]
        #[from]
        OptimizerError,
    ),
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{Database, DatabaseError};
    use crate::storage::{Storage, StorageError, Transaction};
    use crate::types::tuple::create_table;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn build_table(mut transaction: impl Transaction) -> Result<(), StorageError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false),
                None,
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false),
                None,
            ),
        ];
        let _ = transaction.create_table(Arc::new("t1".to_string()), columns)?;
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
    async fn test_crud_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;

        let _ = kipsql.run("create table t1 (a int primary key, b int unique null, k int, z varchar unique null)").await?;
        let _ = kipsql
            .run("create table t2 (c int primary key, d int unsigned null, e datetime)")
            .await?;
        let _ = kipsql.run("insert into t1 (a, b, k, z) values (-99, 1, 1, 'k'), (-1, 2, 2, 'i'), (5, 3, 2, 'p')").await?;
        let _ = kipsql.run("insert into t2 (d, c, e) values (2, 1, '2021-05-20 21:00:00'), (3, 4, '2023-09-10 00:00:00')").await?;
        let _ = kipsql
            .run("create table t3 (a int primary key, b decimal(4,2))")
            .await?;
        let _ = kipsql
            .run("insert into t3 (a, b) values (1, 1111), (2, 2.01), (3, 3.00)")
            .await?;
        let _ = kipsql
            .run("insert into t3 (a, b) values (4, 4444), (5, 5222), (6, 1.00)")
            .await?;

        println!("show tables:");
        let tuples_show_tables = kipsql.run("show tables").await?;
        println!("{}", create_table(&tuples_show_tables));

        println!("full t1:");
        let tuples_full_fields_t1 = kipsql.run("select * from t1").await?;
        println!("{}", create_table(&tuples_full_fields_t1));

        println!("full t2:");
        let tuples_full_fields_t2 = kipsql.run("select * from t2").await?;
        println!("{}", create_table(&tuples_full_fields_t2));

        println!("projection_and_filter:");
        let tuples_projection_and_filter = kipsql.run("select a from t1 where b > 1").await?;
        println!("{}", create_table(&tuples_projection_and_filter));

        println!("projection_and_sort:");
        let tuples_projection_and_sort = kipsql.run("select * from t1 order by a, b").await?;
        println!("{}", create_table(&tuples_projection_and_sort));

        println!("limit:");
        let tuples_limit = kipsql.run("select * from t1 limit 1 offset 1").await?;
        println!("{}", create_table(&tuples_limit));

        println!("inner join:");
        let tuples_inner_join = kipsql
            .run("select * from t1 inner join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_inner_join));

        println!("left join:");
        let tuples_left_join = kipsql.run("select * from t1 left join t2 on a = c").await?;
        println!("{}", create_table(&tuples_left_join));

        println!("right join:");
        let tuples_right_join = kipsql
            .run("select * from t1 right join t2 on a = c")
            .await?;
        println!("{}", create_table(&tuples_right_join));

        println!("full join:");
        let tuples_full_join = kipsql.run("select * from t1 full join t2 on a = c").await?;
        println!("{}", create_table(&tuples_full_join));

        println!("count agg:");
        let tuples_count_agg = kipsql.run("select count(d) from t2").await?;
        println!("{}", create_table(&tuples_count_agg));

        println!("count wildcard agg:");
        let tuples_count_wildcard_agg = kipsql.run("select count(*) from t2").await?;
        println!("{}", create_table(&tuples_count_wildcard_agg));

        println!("count distinct agg:");
        let tuples_count_distinct_agg = kipsql.run("select count(distinct d) from t2").await?;
        println!("{}", create_table(&tuples_count_distinct_agg));

        println!("sum agg:");
        let tuples_sum_agg = kipsql.run("select sum(d) from t2").await?;
        println!("{}", create_table(&tuples_sum_agg));

        println!("sum distinct agg:");
        let tuples_sum_distinct_agg = kipsql.run("select sum(distinct d) from t2").await?;
        println!("{}", create_table(&tuples_sum_distinct_agg));

        println!("avg agg:");
        let tuples_avg_agg = kipsql.run("select avg(d) from t2").await?;
        println!("{}", create_table(&tuples_avg_agg));

        println!("min_max agg:");
        let tuples_min_max_agg = kipsql.run("select min(d), max(d) from t2").await?;
        println!("{}", create_table(&tuples_min_max_agg));

        println!("group agg:");
        let tuples_group_agg = kipsql
            .run("select c, max(d) from t2 group by c having c = 1")
            .await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("alias:");
        let tuples_group_agg = kipsql.run("select c as o from t2").await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("alias agg:");
        let tuples_group_agg = kipsql
            .run("select c, max(d) as max_d from t2 group by c having c = 1")
            .await?;
        println!("{}", create_table(&tuples_group_agg));

        println!("time max:");
        let tuples_time_max = kipsql.run("select max(e) as max_time from t2").await?;
        println!("{}", create_table(&tuples_time_max));

        println!("time where:");
        let tuples_time_where_t2 = kipsql
            .run("select (c + 1) from t2 where e > '2021-05-20'")
            .await?;
        println!("{}", create_table(&tuples_time_where_t2));

        assert!(kipsql
            .run("select max(d) from t2 group by c")
            .await
            .is_err());

        println!("distinct t1:");
        let tuples_distinct_t1 = kipsql.run("select distinct b, k from t1").await?;
        println!("{}", create_table(&tuples_distinct_t1));

        println!("update t1 with filter:");
        let _ = kipsql.run("update t1 set b = 0 where b = 1").await?;
        println!("after t1:");
        let update_after_full_t1 = kipsql.run("select * from t1").await?;
        println!("{}", create_table(&update_after_full_t1));

        println!("insert overwrite t1:");
        let _ = kipsql
            .run("insert overwrite t1 (a, b, k) values (-99, 1, 0)")
            .await?;
        println!("after t1:");
        let insert_overwrite_after_full_t1 = kipsql.run("select * from t1").await?;
        println!("{}", create_table(&insert_overwrite_after_full_t1));

        assert!(kipsql
            .run("insert overwrite t1 (a, b, k) values (-1, 1, 0)")
            .await
            .is_err());

        println!("delete t1 with filter:");
        let _ = kipsql.run("delete from t1 where b = 0").await?;
        println!("after t1:");
        let delete_after_full_t1 = kipsql.run("select * from t1").await?;
        println!("{}", create_table(&delete_after_full_t1));

        println!("truncate t1:");
        let _ = kipsql.run("truncate t1").await?;

        println!("drop t1:");
        let _ = kipsql.run("drop table t1").await?;

        println!("decimal:");
        let tuples_decimal = kipsql.run("select * from t3").await?;
        println!("{}", create_table(&tuples_decimal));

        Ok(())
    }
}
