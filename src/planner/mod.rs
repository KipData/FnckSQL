pub mod operator;

use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::planner::operator::join::JoinType;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::types::tuple::{Schema, SchemaRef};
use itertools::Itertools;
use fnck_sql_serde_macros::ReferenceSerialization;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum SchemaOutput {
    Schema(Schema),
    SchemaRef(SchemaRef),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct LogicalPlan {
    pub(crate) operator: Operator,
    pub(crate) childrens: Vec<LogicalPlan>,
    pub(crate) physical_option: Option<PhysicalOption>,

    pub(crate) _output_schema_ref: Option<SchemaRef>,
}

impl SchemaOutput {
    pub(crate) fn columns(&self) -> impl Iterator<Item = &ColumnRef> {
        match self {
            SchemaOutput::Schema(schema) => schema.iter(),
            SchemaOutput::SchemaRef(schema_ref) => schema_ref.iter(),
        }
    }
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
            if let Operator::TableScan(op) = &plan.operator {
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

    pub(crate) fn _output_schema_direct(
        operator: &Operator,
        childrens: &[LogicalPlan],
    ) -> SchemaOutput {
        match operator {
            Operator::Filter(_) | Operator::Sort(_) | Operator::Limit(_) => {
                childrens[0].output_schema_direct()
            }
            Operator::Aggregate(op) => SchemaOutput::Schema(
                op.agg_calls
                    .iter()
                    .chain(op.groupby_exprs.iter())
                    .map(|expr| expr.output_column())
                    .collect_vec(),
            ),
            Operator::Join(op) => {
                if matches!(op.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    return childrens[0].output_schema_direct();
                }
                let mut columns = Vec::new();

                for plan in childrens.iter() {
                    for column in plan.output_schema_direct().columns() {
                        columns.push(column.clone());
                    }
                }
                SchemaOutput::Schema(columns)
            }
            Operator::Project(op) => SchemaOutput::Schema(
                op.exprs
                    .iter()
                    .map(|expr| expr.output_column())
                    .collect_vec(),
            ),
            Operator::TableScan(op) => SchemaOutput::Schema(
                op.columns
                    .iter()
                    .map(|(_, column)| column.clone())
                    .collect_vec(),
            ),
            Operator::FunctionScan(op) => {
                SchemaOutput::SchemaRef(op.table_function.output_schema().clone())
            }
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            }) => SchemaOutput::SchemaRef(schema_ref.clone()),
            Operator::Dummy => SchemaOutput::Schema(vec![]),
            Operator::Show => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("TABLE".to_string()),
            )]),
            Operator::Explain => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("PLAN".to_string()),
            )]),
            Operator::Describe(_) => SchemaOutput::Schema(vec![
                ColumnRef::from(ColumnCatalog::new_dummy("FIELD".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("TYPE".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("LEN".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("NULL".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("Key".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("DEFAULT".to_string())),
            ]),
            Operator::Insert(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("INSERTED".to_string()),
            )]),
            Operator::Update(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("UPDATED".to_string()),
            )]),
            Operator::Delete(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DELETED".to_string()),
            )]),
            Operator::Analyze(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("STATISTICS_META_PATH".to_string()),
            )]),
            Operator::AddColumn(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("ADD COLUMN SUCCESS".to_string()),
            )]),
            Operator::DropColumn(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP COLUMN SUCCESS".to_string()),
            )]),
            Operator::CreateTable(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE TABLE SUCCESS".to_string()),
            )]),
            Operator::CreateIndex(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE INDEX SUCCESS".to_string()),
            )]),
            Operator::CreateView(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE VIEW SUCCESS".to_string()),
            )]),
            Operator::DropTable(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP TABLE SUCCESS".to_string()),
            )]),
            Operator::Truncate(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("TRUNCATE TABLE SUCCESS".to_string()),
            )]),
            Operator::CopyFromFile(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("COPY FROM SOURCE".to_string()),
            )]),
            Operator::CopyToFile(_) => todo!(),
        }
    }

    pub(crate) fn output_schema_direct(&self) -> SchemaOutput {
        Self::_output_schema_direct(&self.operator, &self.childrens)
    }

    pub fn output_schema(&mut self) -> &SchemaRef {
        self._output_schema_ref.get_or_insert_with(|| {
            match Self::_output_schema_direct(&self.operator, &self.childrens) {
                SchemaOutput::Schema(schema) => Arc::new(schema),
                SchemaOutput::SchemaRef(schema_ref) => schema_ref.clone(),
            }
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
