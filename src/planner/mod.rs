pub mod operator;

use crate::catalog::TableName;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::types::tuple::SchemaRef;
use itertools::Itertools;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct LogicalPlan {
    pub(crate) operator: Operator,
    pub(crate) childrens: Vec<LogicalPlan>,
    pub(crate) physical_option: Option<PhysicalOption>,

    pub(crate) _output_schema_ref: Option<SchemaRef>,
}

impl LogicalPlan {
    pub fn new(operator: Operator, childrens: Vec<LogicalPlan>) -> Self {
        Self {
            operator,
            childrens,
            physical_option: None,
            _output_schema_ref: None,
        }
    }

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

    pub fn output_schema(&mut self) -> &SchemaRef {
        self._output_schema_ref
            .get_or_insert_with(|| match &self.operator {
                Operator::Filter(_) | Operator::Sort(_) | Operator::Limit(_) => {
                    self.childrens[0].output_schema().clone()
                }
                Operator::Aggregate(op) => {
                    let out_columns = op
                        .agg_calls
                        .iter()
                        .chain(op.groupby_exprs.iter())
                        .map(|expr| expr.output_column())
                        .collect_vec();
                    Arc::new(out_columns)
                }
                Operator::Join(_) => {
                    let out_columns = self
                        .childrens
                        .iter_mut()
                        .flat_map(|children| Vec::clone(children.output_schema()))
                        .collect_vec();
                    Arc::new(out_columns)
                }
                Operator::Project(op) => {
                    let out_columns = op
                        .exprs
                        .iter()
                        .map(|expr| expr.output_column())
                        .collect_vec();
                    Arc::new(out_columns)
                }
                Operator::Scan(op) => {
                    let out_columns = op
                        .columns
                        .iter()
                        .map(|(_, column)| column.clone())
                        .collect_vec();
                    Arc::new(out_columns)
                }
                Operator::Values(op) => op.schema_ref.clone(),
                Operator::Dummy
                | Operator::Show
                | Operator::Explain
                | Operator::Describe(_)
                | Operator::Insert(_)
                | Operator::Update(_)
                | Operator::Delete(_)
                | Operator::Analyze(_)
                | Operator::AddColumn(_)
                | Operator::DropColumn(_)
                | Operator::CreateTable(_)
                | Operator::DropTable(_)
                | Operator::Truncate(_)
                | Operator::CopyFromFile(_)
                | Operator::CopyToFile(_) => Arc::new(vec![]),
            })
    }

    pub fn explain(&self, indentation: usize) -> String {
        let mut result = format!("{:indent$}{}", "", self.operator, indent = indentation);

        if let Some(physical_option) = &self.physical_option {
            result.push_str(&format!(" [{}]", physical_option));
        }

        for child in &self.childrens {
            result.push('\n');
            result.push_str(&child.explain(indentation + 2));
        }

        result
    }
}
