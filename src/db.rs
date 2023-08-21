use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder, BinderContext};
use crate::execution_tp::ExecutorError;
use crate::execution_ap::physical_plan::physical_plan_mapping::PhysicalPlanMapping;
use crate::execution_tp::executor::{Executor, try_collect};
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::RuleImpl;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage_tp::memory::MemStorage;
use crate::storage_tp::{Storage, StorageError};
use crate::types::tuple::Tuple;

#[derive(Debug)]
pub struct Database {
    pub storage: MemStorage,
}

impl Default for Database {
    fn default() -> Self {
        Self::new_on_mem()
    }
}

impl Database {
    /// Create a new Database instance.
    pub fn new_on_mem() -> Self {
        let storage = MemStorage::new();
        Database { storage }
    }

    /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>, ExecutorError> {
        // parse
        let stmts = parse_sql(sql)?;
        // bind
        let catalog = self.storage.get_catalog();

        let binder = Binder::new(BinderContext::new(catalog.clone()));

        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);

        let best_plan = Self::default_optimizer(source_plan)
            .find_best();
        // println!("best_plan plan: {:#?}", best_plan);

        let physical_plan = PhysicalPlanMapping::build_plan(best_plan)?;
        // println!("physical_plan: {:#?}", physical_plan);

        let executor = Executor::new(self.storage.clone());

        let mut stream = executor.build(physical_plan);

        Ok(try_collect(&mut stream).await?)
    }

    fn default_optimizer(source_plan: LogicalPlan) -> HepOptimizer {
        HepOptimizer::new(source_plan)
            .batch(
                "Predicate pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::PushPredicateThroughJoin
                ]
            )
            .batch(
                "Limit pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::LimitProjectTranspose,
                    RuleImpl::PushLimitThroughJoin,
                    RuleImpl::PushLimitIntoTableScan,
                    RuleImpl::EliminateLimits,
                ],
            )
            .batch(
                "Column pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::PushProjectThroughChild,
                    RuleImpl::PushProjectIntoScan
                ]
            )
            .batch(
                "Combine operators".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    RuleImpl::CollapseProject,
                    RuleImpl::CombineFilter
                ]
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
        #[backtrace]
        StorageError,
    ),
    #[error("Arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        #[backtrace]
        ArrowError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::Database;
    use crate::execution_tp::ExecutorError;
    use crate::storage_tp::{Storage, StorageError};
    use crate::types::{LogicalType, TableId};
    use crate::types::tuple::create_table;

    fn build_table(storage: &impl Storage) -> Result<TableId, StorageError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true)
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false)
            ),
        ];

        Ok(storage.create_table("t1".to_string(), columns)?)
    }

    #[test]
    fn test_run_sql() -> Result<(), ExecutorError> {
        let database = Database::new_on_mem();

        let _ = build_table(&database.storage)?;

        tokio_test::block_on(async move {
            let batch = database.run("select * from t1").await?;
            println!("{:#?}", batch);

            Ok(())
        })
    }

    #[test]
    fn test_crud_sql() -> Result<(), ExecutorError> {
        let kipsql = Database::new_on_mem();

        tokio_test::block_on(async move {
            let _ = kipsql.run("create table t1 (a int, b int)").await?;
            let _ = kipsql.run("create table t2 (c int, d int null)").await?;
            let _ = kipsql.run("insert into t1 (b, a) values (1, 1), (3, 3), (5, 4)").await?;
            let _ = kipsql.run("insert into t2 (d, c) values (1, 2), (2, 3), (null, 6)").await?;

            println!("full t1:");
            let tuples_full_fields_t1 = kipsql.run("select * from t1").await?;
            println!("{}", create_table(&tuples_full_fields_t1));

            println!("full t2:");
            let tuples_full_fields_t2 = kipsql.run("select * from t2").await?;
            println!("{}", create_table(&tuples_full_fields_t2));

            // println!("projection_and_filter:");
            // let tuples_projection_and_filter = kipsql.run("select a from t1 where a <= b").await?;
            // println!("{:#?}", tuples_projection_and_filter);
            //
            // println!("projection_and_sort:");
            // let tuples_projection_and_sort = kipsql.run("select a from t1 order by a").await?;
            // println!("{:#?}", tuples_projection_and_sort);
            //
            // println!("limit:");
            // let tuples_limit=kipsql.run("select * from t1 limit 1 offset 1").await?;
            // println!("{:#?}", tuples_limit);
            //
            // println!("inner join:");
            // let tuples_inner_join = kipsql.run("select * from t1 inner join t2 on a = c").await?;
            // println!("{:#?}", tuples_inner_join);
            //
            // println!("left join:");
            // let tuples_left_join = kipsql.run("select * from t1 left join t2 on a = c").await?;
            // println!("{:#?}", tuples_left_join);
            //
            // println!("right join:");
            // let tuples_right_join = kipsql.run("select * from t1 right join t2 on a = c and a > 1").await?;
            // println!("{:#?}", tuples_right_join);
            //
            // println!("full join:");
            // let tuples_full_join = kipsql.run("select d, b from t1 full join t2 on a = c and a > 1").await?;
            // println!("{:#?}", tuples_full_join);

            Ok(())
        })
    }
}
