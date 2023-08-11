use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::execution_v1::physical_plan::PhysicalPlan;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use anyhow::anyhow;
use anyhow::Result;
use crate::execution_v1::physical_plan::physical_filter::PhysicalFilter;
use crate::execution_v1::physical_plan::physical_hash_join::PhysicalHashJoin;
use crate::execution_v1::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_v1::physical_plan::physical_limit::PhysicalLimit;
use crate::execution_v1::physical_plan::physical_sort::PhysicalSort;
use crate::execution_v1::physical_plan::physical_values::PhysicalValues;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::{JoinOperator, JoinType};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::sort::SortOperator;
use crate::planner::operator::values::ValuesOperator;

pub struct PhysicalPlanMapping;

impl PhysicalPlanMapping {
    pub fn build_plan(mut plan: LogicalPlan) -> Result<PhysicalPlan> {
        let plan = match plan.operator {
            Operator::Project(op) => {
                let child = plan.childrens.remove(0);

                Self::build_physical_select_projection(child, op)?
            },
            Operator::Scan(scan) => {
                Self::build_physical_scan(scan.clone())
            },
            Operator::Filter(op) => {
                let child = plan.childrens.remove(0);

                Self::build_physical_filter(child, op)?
            },
            Operator::CreateTable(op) => {
                Self::build_physical_create_table(op)
            },
            Operator::Insert(op) => {
                let child = plan.childrens.remove(0);

                Self::build_insert(child, op)?
            },
            Operator::Values(op) => {
                Self::build_values(op)
            },
            Operator::Sort(op) => {
                let child = plan.childrens.remove(0);

                Self::build_physical_sort(child, op)?
            },
            Operator::Limit(op) => {
                let child = plan.childrens.remove(0);

                Self::build_physical_limit(child, op)?
            },
            Operator::Join(op) => {
                let left_child = plan.childrens.remove(0);
                let right_child = plan.childrens.remove(0);

                Self::build_physical_join(left_child, right_child, op)?
            }
            _ => return Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        };

        Ok(plan)
    }

    fn build_values(op: ValuesOperator) -> PhysicalPlan {
        PhysicalPlan::Values(PhysicalValues { op })
    }

    fn build_insert(child: LogicalPlan, op: InsertOperator) -> Result<PhysicalPlan> {
        let input = Self::build_plan(child)?;

        Ok(PhysicalPlan::Insert(PhysicalInsert {
            table_name: op.table,
            input: Box::new(input),
        }))
    }

    fn build_physical_create_table(op: CreateTableOperator) -> PhysicalPlan {
        PhysicalPlan::CreateTable(
            PhysicalCreateTable {
                op,
            }
        )
    }

    fn build_physical_select_projection(child: LogicalPlan, op: ProjectOperator) -> Result<PhysicalPlan> {
        let input = Self::build_plan(child)?;

        Ok(PhysicalPlan::Projection(PhysicalProjection {
            exprs: op.columns,
            input: Box::new(input),
        }))
    }

    fn build_physical_scan(op: ScanOperator) -> PhysicalPlan {
        PhysicalPlan::TableScan(PhysicalTableScan { op })
    }

    fn build_physical_filter(child: LogicalPlan, op: FilterOperator) -> Result<PhysicalPlan> {
        let input = Self::build_plan(child)?;

        Ok(PhysicalPlan::Filter(PhysicalFilter {
            predicate: op.predicate,
            input: Box::new(input),
        }))
    }

    fn build_physical_sort(child: LogicalPlan, op: SortOperator) -> Result<PhysicalPlan> {
        let input = Self::build_plan(child)?;

        Ok(PhysicalPlan::Sort(PhysicalSort {
            op,
            input: Box::new(input),
        }))
    }

    fn build_physical_limit(child: LogicalPlan, op: LimitOperator) -> Result<PhysicalPlan> {
        let input =Self::build_plan(child)?;

        Ok(PhysicalPlan::Limit(PhysicalLimit{
            op,
            input: Box::new(input),
        }))
    }

    fn build_physical_join(left_child: LogicalPlan, right_child: LogicalPlan, op: JoinOperator) -> Result<PhysicalPlan> {
        let left_input = Box::new(Self::build_plan(left_child)?);
        let right_input = Box::new(Self::build_plan(right_child)?);

        if op.join_type == JoinType::Cross {
            todo!()
        } else {
            Ok(PhysicalPlan::HashJoin(PhysicalHashJoin {
                op,
                left_input,
                right_input,
            }))
        }
    }
}