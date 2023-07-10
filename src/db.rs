use crate::binder::{Binder, BinderContext};
use crate::catalog::{CatalogRef, Column, Root};
use crate::parser::parse_sql;
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::LogicalPlan;

use crate::storage::{InMemoryStorage, Storage};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Database {
    pub storage: InMemoryStorage,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new Database instance.
    pub fn new() -> Self {
        let storage = InMemoryStorage::new();
        Database { storage }
    }

    /// Run SQL queries.
    pub fn run(&mut self, sql: &str) -> Result<()> {
        // parse
        let stmts = parse_sql(sql)?;
        // bind
        let catalog = self.storage.catalog();
        let mut binder = Binder::new(BinderContext::new(catalog.clone()));

        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let logical_plan = binder.bind(&stmts[0])?;

        println!("{:?}", logical_plan);

        //let physical_planner = PhysicalPlaner::default();
        //let executor_builder = ExecutorBuilder::new(self.env.clone());

        //let physical_plan = physical_planner.plan(logical_plan)?;
        //let executor = executor_builder.build(physical_plan)?;
        //futures::executor::block_on(executor).unwrap();

        /// THE FOLLOWING CODE IS FOR TESTING ONLY
        /// THE FINAL CODE WILL BE IN executor MODULE
        if let LogicalPlan::CreateTable(plan) = logical_plan {
            let mut colums = Vec::new();
            plan.columns.iter().for_each(|c| {
                colums.push(Column::new(c.0.clone(), c.1.clone()));
            });
            let mut table_name = plan.table_name.clone();
            self.storage
                .create_table(&table_name.to_string(), &colums.clone())?;
        }

        Ok(())
    }
}
