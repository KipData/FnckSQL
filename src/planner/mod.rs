pub mod operator;

use crate::catalog::TableName;
use crate::planner::operator::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan {
    pub operator: Operator,
    pub childrens: Vec<LogicalPlan>,
}

impl LogicalPlan {
    pub fn child(&self, index: usize) -> Option<&LogicalPlan> {
        self.childrens.get(index)
    }

    pub fn referenced_table(&self) -> Vec<TableName> {
        fn collect_table(plan: &LogicalPlan, results: &mut Vec<TableName>) {
            if let Operator::Scan(op) = &plan.operator {
                results.push(op.table_name.clone());
            }
            for child in &plan.childrens {
                collect_table(child, results);
            }
        }

        let mut tables = Vec::new();
        collect_table(self, &mut tables);
        tables
    }
}
