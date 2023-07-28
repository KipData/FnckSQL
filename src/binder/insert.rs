use sqlparser::ast::{Expr, Ident, ObjectName};
use anyhow::Result;
use itertools::Itertools;
use crate::binder::{Binder, lower_case_name, split_name};
use crate::expression::ScalarExpression;
use crate::planner::logical_insert_plan::LogicalInsertPlan;
use crate::planner::operator::insert::InsertOperator;

impl Binder {
    pub(crate) fn bind_insert(
        &mut self,
        name: ObjectName,
        idents: &[Ident],
        rows: &Vec<Vec<Expr>>
    ) -> Result<LogicalInsertPlan> {
        let name = lower_case_name(&name);
        let (_, table_name) = split_name(&name)?;

        if let Some(table) = self.context.catalog.get_table_by_name(table_name) {
            let mut col_idxs = Vec::new();

            for ident in idents {
                let col_name = &ident.value;
                if let Some(col_idx) = table.get_column_id_by_name(col_name) {
                    col_idxs.push(col_idx.clone());
                } else {
                    return Err(anyhow::Error::msg(format!(
                        "not found column {} on table {}",
                        col_name,
                        table_name
                    )))
                }
            }
            if col_idxs.is_empty() {
                col_idxs = (0..table.columns_len()).collect_vec()
            }

            // 行转列
            let mut cols: Vec<Vec<ScalarExpression>> = vec![Vec::new(); rows[0].len()];

            for row in rows {
                for (i, expr) in row.into_iter().enumerate() {
                    cols[i].push(self.bind_expr(expr)?);
                }
            }

            Ok(LogicalInsertPlan {
                operator: InsertOperator {
                    table: table_name.to_string(),
                    col_idxs,
                    cols,
                },
            })
        } else {
            Err(anyhow::Error::msg(format!(
                "not found table {}",
                table_name
            )))
        }
    }


}