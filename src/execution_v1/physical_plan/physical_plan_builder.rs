use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::planner::logical_create_table_plan::LogicalCreateTablePlan;
use crate::planner::logical_select_plan::LogicalSelectPlan;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
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
            LogicalPlan::CreateTable(create_table) => {
                self.build_create_table_logic_plan(create_table)
            }
        }
    }

    fn build_create_table_logic_plan(
        &mut self,
        plan: &LogicalCreateTablePlan,
    ) -> Result<PhysicalOperator> {
        Ok(PhysicalOperator::CreateTable(PhysicalCreateTable {
            table_name: plan.table_name.to_string(),
            columns: plan.columns.clone(),
        }))
    }

    fn build_select_logical_plan(&mut self, plan: &LogicalSelectPlan) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Project(op) => {
                let input = self.build_select_logical_plan(plan.child(0)?)?;
                Ok(PhysicalOperator::Projection(PhysicalProjection {
                    plan_id: self.next_plan_id(),
                    exprs: op.columns.clone(),
                    input: Box::new(input),
                }))
            }
            Operator::Scan(scan) => Ok(self.build_physical_scan(scan.clone())),
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn build_physical_scan(&mut self, base: ScanOperator) -> PhysicalOperator {
        PhysicalOperator::TableScan(PhysicalTableScan { plan_id: self.next_plan_id(), base })
    }
}