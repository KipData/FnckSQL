use std::sync::Arc;

use crate::planner::operator::scan::ScanOperator;
use crate::planner::{logical_select_plan::LogicalSelectPlan, operator::Operator, LogicalPlan};

use super::{
    physical_filter::PhysicalFilter, physical_limit::PhysicalLimit,
    physical_projection::PhysicalProjection, physical_sort::PhysicalSort, PhysicalOperator,
};

use anyhow::anyhow;
use anyhow::Result;

pub struct PhysicalPlanBuilder {
    plan_id: u32,
}

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        PhysicalPlanBuilder { plan_id: 0 }
    }

    fn next_plan_id(&mut self) -> u32 {
        let id = self.plan_id;
        self.plan_id += 1;
        id
    }

    pub fn build_plan(&mut self, plan: &LogicalPlan) -> Result<PhysicalOperator> {
        match plan {
            LogicalPlan::Select(select) => self.build_select_logical_plan(select),
            LogicalPlan::CreateTable(_) => todo!(),
        }
    }

    fn build_select_logical_plan(&mut self, plan: &LogicalSelectPlan) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Project(_) => {
                let input = self.build_select_logical_plan(plan.child(0)?)?;
                Ok(PhysicalOperator::Prjection(PhysicalProjection {
                    plan_id: self.next_plan_id(),
                    input: Arc::new(input),
                }))
            }
            Operator::Scan(scan) => self.build_physical_scan(scan),
            Operator::Sort(sort) => {
                let input = self.build_select_logical_plan(plan.child(0)?)?;
                Ok(PhysicalOperator::Sort(PhysicalSort {
                    plan_id: self.next_plan_id(),
                    input: Arc::new(input),
                    order_by: sort.sort_fields.clone(),
                    limit: sort.limit,
                }))
            }
            Operator::Limit(limit) => {
                let input = self.build_select_logical_plan(plan.child(0)?)?;

                Ok(PhysicalOperator::Limit(PhysicalLimit {
                    plan_id: self.next_plan_id(),
                    input: Arc::new(input),
                    limit: limit.count,
                    offset: limit.offset,
                }))
            }
            Operator::Filter(filter) => {
                let input = self.build_select_logical_plan(plan.child(0)?)?;
                Ok(PhysicalOperator::Filter(PhysicalFilter {
                    plan_id: self.next_plan_id(),
                    input: Arc::new(input),
                    predicates: filter.predicate.clone(),
                }))
            }
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn build_physical_scan(&mut self, _scan: &ScanOperator) -> Result<PhysicalOperator> {
        //    let ScanOperator {
        //        table_ref_id,
        //        columns,
        //        sort_fields,
        //        predicates,
        //        limit,
        //    } = scan;

        // Get table impl use `table_ref_id`.
        todo!()
    }
}
