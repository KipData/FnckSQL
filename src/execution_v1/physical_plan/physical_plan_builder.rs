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
use crate::execution_v1::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_v1::physical_plan::physical_values::PhysicalValues;
use crate::planner::logical_insert_plan::LogicalInsertPlan;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::values::ValuesOperator;

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
            LogicalPlan::Select(select) =>
                self.build_select_logical_plan(select),
            LogicalPlan::CreateTable(create_table) =>
                Ok(self.build_create_table_logical_plan(create_table)),
            LogicalPlan::Insert(insert) =>
                self.build_insert_logical_plan(insert)
        }
    }

    fn build_insert_logical_plan(
        &mut self,
        plan: &LogicalInsertPlan,
    ) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Insert(op) => self.bind_insert(plan, op),
            Operator::Values(op) => Ok(Self::bind_values(op)),
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn bind_values(op: &ValuesOperator) -> PhysicalOperator {
        PhysicalOperator::Values(PhysicalValues { base: op.clone() })
    }

    fn bind_insert(&mut self, plan: &LogicalInsertPlan, op: &InsertOperator) -> Result<PhysicalOperator> {
        let input = self.build_insert_logical_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Insert(PhysicalInsert {
            table_name: op.table.clone(),
            input: Box::new(input),
        }))
    }

    fn build_create_table_logical_plan(
        &mut self,
        plan: &LogicalCreateTablePlan,
    ) -> PhysicalOperator {
        PhysicalOperator::CreateTable(
            PhysicalCreateTable {
                op: plan.operator.clone(),
            }
        )
    }

    fn build_select_logical_plan(&mut self, plan: &LogicalSelectPlan) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Project(op) => self.build_physical_select_projection(plan, op),
            Operator::Scan(scan) => Ok(self.build_physical_scan(scan.clone())),
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn build_physical_select_projection(&mut self, plan: &LogicalSelectPlan, op: &ProjectOperator) -> Result<PhysicalOperator> {
        let input = self.build_select_logical_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Projection(PhysicalProjection {
            plan_id: self.next_plan_id(),
            exprs: op.columns.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_scan(&mut self, base: ScanOperator) -> PhysicalOperator {
        PhysicalOperator::TableScan(PhysicalTableScan { plan_id: self.next_plan_id(), base })
    }
}