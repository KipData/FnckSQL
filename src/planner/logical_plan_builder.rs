use crate::binder::{Binder, BinderContext};
use crate::catalog::Root;
use crate::parser;
use anyhow::Result;
use std::sync::Arc;

use crate::planner::logical_select_plan::LogicalSelectPlan;
use crate::planner::LogicalPlan;

#[derive(Clone)]
pub struct PlanBuilder {}

impl PlanBuilder {
    pub fn new() -> Self {
        PlanBuilder {}
    }
}
