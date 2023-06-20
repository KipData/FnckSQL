use crate::{
    executor::physical::physical_project::PhysicalProject,
    planner::{logical_select_plan::LogicalSelectPlan, operator::Operator, LogicalPlan},
};

use super::{
    physical_filter::PhysicalFilter, physical_limit::PhysicalLimit,
    physical_scan::PhysicalTableScan, physical_sort::PhysicalSort, PhysicalPlan,
};
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

    // todo: all physical plan need add statistic.
    // todo:
    pub fn build_plan(&mut self, plan: &LogicalPlan) -> Result<PhysicalPlan> {
        match plan {
            LogicalPlan::Select(select) => self.build_select(select),
            LogicalPlan::CreateTable(_) => todo!(),
        }
    }

    fn build_select(&mut self, select_plan: &LogicalSelectPlan) -> Result<PhysicalPlan> {
        match select_plan.operator.as_ref() {
            Operator::Project(_) => {
                let input = self.build_select(select_plan.child(0)?)?;
                Ok(PhysicalPlan::Prjection(PhysicalProject {
                    plan_id: self.next_plan_id(),
                    input: Box::new(input),
                }))
            }
            Operator::Scan(scan) => Ok(PhysicalPlan::TableScan(PhysicalTableScan {
                plan_id: self.next_plan_id(),
                operator: scan.clone(),
            })),
            Operator::Sort(sort) => {
                let input = self.build_select(select_plan.child(0)?)?;
                Ok(PhysicalPlan::Sort(PhysicalSort {
                    plan_id: self.next_plan_id(),
                    input: Box::new(input),
                    order_by: sort.sort_fields.clone(),
                    limit: sort.limit,
                }))
            }
            Operator::Limit(limit) => {
                let input = self.build_select(select_plan.child(0)?)?;

                Ok(PhysicalPlan::Limit(PhysicalLimit {
                    plan_id: self.next_plan_id(),
                    input: Box::new(input),
                    limit: limit.count,
                    offset: limit.offset,
                }))
            }
            Operator::Filter(filter) => {
                let input = self.build_select(select_plan.child(0)?)?;
                Ok(PhysicalPlan::Filter(PhysicalFilter {
                    plan_id: self.next_plan_id(),
                    input: Box::new(input),
                    predicates: filter.predicate.clone(),
                }))
            }
            _ => Err(anyhow::Error::msg(format!(
                "Unsupported physical plan: {:?}",
                select_plan.operator
            ))),
        }
    }
}
