use anyhow::Result;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder, BinderContext};
use crate::execution_v1::physical_plan::physical_plan_builder::PhysicalPlanBuilder;
use crate::execution_v1::volcano_executor::VolcanoExecutor;
use crate::parser::parse_sql;
use crate::storage::memory::InMemoryStorage;
use crate::storage::{Storage, StorageError, StorageImpl};

#[derive(Debug)]
pub struct Database {
    pub storage: InMemoryStorage,
}

impl Default for Database {
    fn default() -> Self {
        Self::new_on_mem()
    }
}

impl Database {
    /// Create a new Database instance.
    pub fn new_on_mem() -> Self {
        let storage = InMemoryStorage::new();
        Database { storage }
    }

    /// Run SQL queries.
    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>> {
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
        let logical_plan = binder.bind(&stmts[0])?;
        // println!("logic plan: {:#?}", logical_plan);

        let mut builder = PhysicalPlanBuilder::new();
        let operator = builder.build_plan(&logical_plan)?;
        // println!("operator: {:#?}", operator);

        let storage = StorageImpl::InMemoryStorage(self.storage.clone());
        let executor = VolcanoExecutor::new(storage);

        let mut stream = executor.build(operator);

        Ok(VolcanoExecutor::try_collect(&mut stream).await?)
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
    use std::sync::Arc;
    use arrow::array::{BooleanArray, Int32Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::Database;
    use crate::storage::{Storage, StorageError};
    use crate::types::{LogicalType, TableId};

    fn build_table(storage: &impl Storage) -> Result<TableId, StorageError> {
        let schema = Arc::new(Schema::new(
            vec![
                ColumnCatalog::new(
                    "c1".to_string(),
                    false,
                    ColumnDesc::new(LogicalType::Integer, true)
                ).to_field(),
                ColumnCatalog::new(
                    "c2".to_string(),
                    false,
                    ColumnDesc::new(LogicalType::Boolean, false)
                ).to_field(),
            ]
        ));
        let batch_1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(BooleanArray::from(vec![true, true, false, true, false]))
            ]
        ).unwrap();

        Ok(storage.create_table("t1", vec![batch_1])?)
    }

    #[test]
    fn test_run_sql() -> anyhow::Result<()> {
        let mut database = Database::new_on_mem();

        let i = build_table(&database.storage)?;

        tokio_test::block_on(async move {
            let batch = database.run("select * from t1").await?;
            println!("{:#?}", batch);

            Ok(())
        })
    }

    #[test]
    fn test_crud_sql() -> anyhow::Result<()> {
        let kipsql = Database::new_on_mem();

        tokio_test::block_on(async move {
            let _ = kipsql.run("create table t1 (a int, b int)").await?;
            let _ = kipsql.run("create table t2 (c int, d int)").await?;
            let _ = kipsql.run("insert into t1 (b, a) values (1, 1), (3, 3), (5, 4)").await?;
            let _ = kipsql.run("insert into t2 (d, c) values (1, 2), (2, 3), (5, 6)").await?;

            println!("full t1:");
            let vec_batch_full_fields_t1 = kipsql.run("select * from t1").await?;
            print_batches(&vec_batch_full_fields_t1)?;

            println!("full t2:");
            let vec_batch_full_fields_t2 = kipsql.run("select * from t2").await?;
            print_batches(&vec_batch_full_fields_t2)?;

            println!("projection_and_filter:");
            let vec_batch_projection_a = kipsql.run("select a from t1 where a <= b").await?;
            print_batches(&vec_batch_projection_a)?;

            println!("projection_and_sort:");
            let vec_batch_projection_a = kipsql.run("select a from t1 order by a").await?;
            print_batches(&vec_batch_projection_a)?;

            println!("limit:");
            let vec_batch_limit=kipsql.run("select * from t1 limit 1 offset 1").await?;
            print_batches(&vec_batch_limit)?;

            println!("inner join:");
            let vec_batch_inner_join = kipsql.run("select * from t1 inner join t2 on a = c").await?;
            print_batches(&vec_batch_inner_join)?;

            println!("left join:");
            let vec_batch_left_join = kipsql.run("select * from t1 left join t2 on a = c").await?;
            print_batches(&vec_batch_left_join)?;

            println!("right join:");
            let vec_batch_right_join = kipsql.run("select * from t1 right join t2 on a = c and a > 1").await?;
            print_batches(&vec_batch_right_join)?;

            println!("full join:");
            let vec_batch_full_join = kipsql.run("select d, b from t1 full join t2 on a = c and a > 1").await?;
            print_batches(&vec_batch_full_join)?;

            println!("aggregate sum");
            let vec_batch_sum_a = kipsql.run("select sum(a) from t1").await?;
            print_batches(&vec_batch_sum_a)?;

            Ok(())
        })
    }
}
