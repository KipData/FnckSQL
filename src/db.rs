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
        println!("logic plan: {:#?}", logical_plan);

        let mut builder = PhysicalPlanBuilder::new();
        let operator = builder.build_plan(&logical_plan)?;
        println!("operator: {:#?}", operator);

        let storage = StorageImpl::InMemoryStorage(self.storage.clone());
        let executor = VolcanoExecutor::new(storage);

        let mut stream = executor.build(operator);

        Ok(VolcanoExecutor::try_collect(&mut stream).await?)

        // // let physical_planner = PhysicalPlaner::default();
        // // let executor_builder = ExecutorBuilder::new(self.env.clone());
        //
        // // let physical_plan = physical_planner.plan(logical_plan)?;
        // // let executor = executor_builder.build(physical_plan)?;
        // // futures::executor::block_on(executor).unwrap();
        //
        // /// THE FOLLOWING CODE IS FOR TESTING ONLY
        // /// THE FINAL CODE WILL BE IN executor MODULE
        // if let LogicalPlan::CreateTable(plan) = logical_plan {
        //     let mut columns = Vec::new();
        //     plan.columns.iter().for_each(|c| {
        //         columns.push(ColumnCatalog::new(c.0.clone(), c.1, c.2.clone()));
        //     });
        //     let table_name = plan.table_name.clone();
        //     // columns->batch record
        //     let mut data = Vec::new();
        //
        //     columns.iter().for_each(|c| {
        //         let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![c.to_field()])));
        //         data.push(batch);
        //     });
        //
        //     self.storage
        //         .create_table(IdGenerator::build(), table_name.as_str(), data)?;
        // }
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
    use arrow::compute::concat_batches;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use itertools::Itertools;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::Database;
    use crate::execution_v1::ExecutorError;
    use crate::storage::{Storage, StorageError};
    use crate::storage::memory::InMemoryStorage;
    use crate::types::{IdGenerator, LogicalType, TableIdx};

    fn build_table(storage: &impl Storage) -> Result<TableIdx, StorageError> {
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
        let database = Database::new_on_mem();

        tokio_test::block_on(async move {
            let _ = database.run("create table t1 (a int, b boolean)").await?;
            let _ = database.run("insert into t1 values (1, true), (2, false)").await?;
            let vec_batch = database.run("select * from t1 where a = 1 or b = false").await?;

            let table = database.storage
                .get_catalog()
                .get_table(0).unwrap().clone();
            print_batches(&vec_batch)?;

            Ok(())
        })
    }
}
