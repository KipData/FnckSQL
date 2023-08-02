use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use anyhow::anyhow;
use anyhow::Result;
use crate::execution_v1::physical_plan::physical_filter::PhysicalFilter;
use crate::execution_v1::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_v1::physical_plan::physical_sort::PhysicalSort;
use crate::execution_v1::physical_plan::physical_values::PhysicalValues;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::sort::SortOperator;
use crate::planner::operator::values::ValuesOperator;

pub struct PhysicalPlanBuilder { }

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        PhysicalPlanBuilder { }
    }

    pub fn build_plan(&mut self, plan: &LogicalPlan) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Project(op) => self.build_physical_select_projection(plan, op),
            Operator::Scan(scan) => Ok(self.build_physical_scan(scan.clone())),
            Operator::Filter(op) => self.build_physical_filter(plan, op),
            Operator::CreateTable(op) => Ok(self.build_physical_create_table(op)),
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

    fn bind_insert(&mut self, plan: &LogicalPlan, op: &InsertOperator) -> Result<PhysicalOperator> {
        let input = self.build_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Insert(PhysicalInsert {
            table_name: op.table.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_create_table(
        &mut self,
        op: &CreateTableOperator,
    ) -> PhysicalOperator {
        PhysicalOperator::CreateTable(
            PhysicalCreateTable {
                op: op.clone(),
            }
        )
    }

    fn build_select_logical_plan(&mut self, plan: &LogicalPlan) -> Result<PhysicalOperator> {
        match plan.operator.as_ref() {
            Operator::Project(op) => self.build_physical_select_projection(plan, op),
            Operator::Scan(scan) => Ok(self.build_physical_scan(scan.clone())),
            Operator::Filter(op) => self.build_physical_filter(plan, op),
            Operator::Sort(op) => self.build_physical_sort(plan, op),
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn build_physical_select_projection(&mut self, plan: &LogicalPlan, op: &ProjectOperator) -> Result<PhysicalOperator> {
        let input = self.build_select_logical_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Projection(PhysicalProjection {
            exprs: op.columns.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_scan(&mut self, base: ScanOperator) -> PhysicalOperator {
        PhysicalOperator::TableScan(PhysicalTableScan { base })
    }

    fn build_physical_filter(&mut self, plan: &LogicalPlan, base: &FilterOperator) -> Result<PhysicalOperator> {
        let input = self.build_select_logical_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Filter(PhysicalFilter {
            predicate: base.predicate.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_sort(&mut self, plan: &LogicalPlan, base: &SortOperator) -> Result<PhysicalOperator> {
        let input = self.build_select_logical_plan(plan.child(0)?)?;

        Ok(PhysicalOperator::Sort(PhysicalSort {
            op: base.clone(),
            input: Box::new(input),
        }))
    }
}