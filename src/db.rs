use crate::binder::{Binder, BinderContext};
use crate::catalog::{CatalogRef, Column, Root};
use crate::parser::parse_sql;
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::LogicalPlan;

use crate::storage::{InMemoryStorage, Storage};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Database {
    pub catalog: Root,
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
        let catalog = storage.catalog().clone();

        Database { catalog, storage }
    }

    /// Run SQL queries.
    pub fn run(&mut self, sql: &str) -> Result<()> {
        // parse
        let stmts = parse_sql(sql)?;
        // bind

        let mut binder = Binder::new(BinderContext::new(self.catalog.clone()));

        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let logical_plan = binder.bind(&stmts[0])?;

        println!("{:?}", logical_plan);

        //let logical_planner = LogicalPlaner::default();
        //let physical_planner = PhysicalPlaner::default();
        //let executor_builder = ExecutorBuilder::new(self.env.clone());

        //let logical_plan = logical_planner.plan(stmt)?;
        //let physical_plan = physical_planner.plan(logical_plan)?;
        //let executor = executor_builder.build(physical_plan)?;
        //futures::executor::block_on(executor).unwrap();

        if let LogicalPlan::CreateTable(plan) = logical_plan {
            let mut colums = Vec::new();
            plan.columns.iter().for_each(|c| {
                colums.push(Column::new(c.0.clone(), c.1.clone()));
            });
            let mut table_name = plan.table_name.clone();
            self.catalog
                .add_table(table_name.to_string(), colums.clone())?;
            self.storage
                .create_table(&table_name.to_string(), &colums.clone())?;
        }

        Ok(())
    }
}
