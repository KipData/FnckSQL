use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder, BinderContext};
use crate::catalog::ColumnCatalog;
use crate::parser::parse_sql;
use crate::planner::LogicalPlan;
use crate::storage::memory::InMemoryStorage;
use crate::storage::{Storage, StorageError};
use crate::types::IdGenerator;

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
    pub fn run(&mut self, sql: &str) -> Result<()> {
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
        println!("logic plan     {:?}", logical_plan);

        // let physical_planner = PhysicalPlaner::default();
        // let executor_builder = ExecutorBuilder::new(self.env.clone());

        // let physical_plan = physical_planner.plan(logical_plan)?;
        // let executor = executor_builder.build(physical_plan)?;
        // futures::executor::block_on(executor).unwrap();

        /// THE FOLLOWING CODE IS FOR TESTING ONLY
        /// THE FINAL CODE WILL BE IN executor MODULE
        if let LogicalPlan::CreateTable(plan) = logical_plan {
            let mut columns = Vec::new();
            plan.columns.iter().for_each(|c| {
                columns.push(ColumnCatalog::new(c.0.clone(), c.1.clone()));
            });
            let table_name = plan.table_name.clone();
            // columns->batch record
            let mut data = Vec::new();

            columns.iter().for_each(|c| {
                let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![c.to_field()])));
                data.push(batch);
            });

            self.storage
                .create_table(IdGenerator::build(), table_name.as_str(), data)?;
        }

        Ok(())
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
