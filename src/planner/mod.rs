pub mod display;
pub mod logical_create_table_plan;
pub mod logical_plan_builder;
pub mod logical_select_plan;
pub mod operator;
pub mod logical_insert_plan;

use crate::planner::logical_insert_plan::LogicalInsertPlan;
use self::{
    logical_create_table_plan::LogicalCreateTablePlan, logical_select_plan::LogicalSelectPlan,
};

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Select(LogicalSelectPlan),
    CreateTable(LogicalCreateTablePlan),
    Insert(LogicalInsertPlan)
}
pub enum LogicalPlanError {}
