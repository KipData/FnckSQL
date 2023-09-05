use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder, BinderContext};
use crate::execution::ExecutorError;
use crate::execution::executor::{Executor, try_collect};
use crate::execution::physical_plan::MappingError;
use crate::execution::physical_plan::physical_plan_mapping::PhysicalPlanMapping;
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizer;
use crate::optimizer::rule::RuleImpl;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::memory::MemStorage;
use crate::storage::StorageError;
use crate::types::tuple::Tuple;

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
    pub async fn run(&self, sql: &str) -> Result<Vec<Tuple>, DatabaseError> {
        // parse
        let stmts = parse_sql(sql)?;

        let binder = Binder::new(BinderContext::new(self.storage.clone()));

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
    #[error("mapping error: {0}")]
    MappingError(
        #[source]
        #[from]
        MappingError
    ),
    #[error("executor error: {0}")]
    ExecutorError(
        #[source]
        #[from]
        ExecutorError
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableName};
    use crate::db::{Database, DatabaseError};
    use crate::storage::{Storage, StorageError};
    use crate::types::LogicalType;
    use crate::types::tuple::create_table;

    fn build_table(storage: &impl Storage) -> Result<TableName, StorageError> {
        let columns = vec![
            Arc::new(
                ColumnCatalog::new(
                    "c1".to_string(),
                    false,
                    ColumnDesc::new(LogicalType::Integer, true)
                )
            ),
            Arc::new(
                ColumnCatalog::new(
                    "c2".to_string(),
                    false,
                    ColumnDesc::new(LogicalType::Boolean, false)
                )
            ),
        ];

        Ok(storage.create_table(Arc::new("t1".to_string()), columns)?)
    }

    #[test]
    fn test_run_sql() -> Result<(), DatabaseError> {
        let database = Database::new_on_mem();

        let _ = build_table(&database.storage)?;

        tokio_test::block_on(async move {
            let batch = database.run("select * from t1").await?;
            println!("{:#?}", batch);

            Ok(())
        })
    }

    #[test]
    fn test_crud_sql() -> Result<(), DatabaseError> {
        let kipsql = Database::new_on_mem();

        tokio_test::block_on(async move {
            let _ = kipsql.run("create table t1 (a int, b int)").await?;
            let _ = kipsql.run("create table t2 (c int, d int null)").await?;
            let _ = kipsql.run("insert into t1 (a, b) values (1, 1), (5, 3), (5, 2)").await?;
            let _ = kipsql.run("insert into t2 (d, c) values (2, 1), (3, 1), (null, 6)").await?;

            println!("full t1:");
            let tuples_full_fields_t1 = kipsql.run("select * from t1").await?;
            println!("{}", create_table(&tuples_full_fields_t1));

            println!("full t2:");
            let tuples_full_fields_t2 = kipsql.run("select * from t2").await?;
            println!("{}", create_table(&tuples_full_fields_t2));

            println!("projection_and_filter:");
            let tuples_projection_and_filter = kipsql.run("select a from t1 where a <= 1").await?;
            println!("{}", create_table(&tuples_projection_and_filter));

            println!("projection_and_sort:");
            let tuples_projection_and_sort = kipsql.run("select * from t1 order by a, b").await?;
            println!("{}", create_table(&tuples_projection_and_sort));

            println!("limit:");
            let tuples_limit = kipsql.run("select * from t1 limit 1 offset 1").await?;
            println!("{}", create_table(&tuples_limit));

            println!("inner join:");
            let tuples_inner_join = kipsql.run("select * from t1 inner join t2 on a = c").await?;
            println!("{}", create_table(&tuples_inner_join));

            println!("left join:");
            let tuples_left_join = kipsql.run("select * from t1 left join t2 on a = c").await?;
            println!("{}", create_table(&tuples_left_join));

            println!("right join:");
            let tuples_right_join = kipsql.run("select * from t1 right join t2 on a = c").await?;
            println!("{}", create_table(&tuples_right_join));

            println!("full join:");
            let tuples_full_join = kipsql.run("select * from t1 full join t2 on a = c").await?;
            println!("{}", create_table(&tuples_full_join));

            println!("update t1 and filter:");
            let _ = kipsql.run("update t1 set a = 0 where b > 1").await?;
            println!("after t1:");
            let update_after_full_t1 = kipsql.run("select * from t1").await?;
            println!("{}", create_table(&update_after_full_t1));


            Ok(())
        })
    }
}
