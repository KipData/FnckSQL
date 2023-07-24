use std::sync::Arc;
use arrow::array::{ArrayRef, BooleanArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use crate::execution_v1::ExecutorError;
use crate::expression::array_compute::binary_op;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;

impl ScalarExpression {
    pub fn eval_column(&self, batch: &RecordBatch) -> Result<ArrayRef, ExecutorError> {
        match &self {
            ScalarExpression::Constant(val) =>
                Ok(val.to_array_of_size(batch.num_rows())),
            ScalarExpression::ColumnRef(col) => {
                // FIXME: 此处ColumnRef理应被优化器下推至TableScan中,
                // 并且无法实现eval_column，因为ColumnCatalog并无对应的index
                Ok(batch.column(0).clone())
            }
            ScalarExpression::InputRef{ index, .. } =>
                Ok(batch.column(*index).clone()),
            ScalarExpression::Alias{ expr, .. } =>
                expr.eval_column(batch),
            ScalarExpression::TypeCast{ expr, ty, .. } =>
                Ok(cast(&expr.eval_column(batch)?, &DataType::from(ty.clone()))?),
            ScalarExpression::Binary{ left_expr, right_expr, op, .. } => {
                let left = left_expr.eval_column(batch)?;
                let right = right_expr.eval_column(batch)?;
                binary_op(&left, &right, op)
            }
            ScalarExpression::IsNull{ expr } =>
                Ok(DataValue::bool_array(batch.num_rows(), &Some(expr.nullable()))),
            ScalarExpression::Unary{ .. } => todo!(),
            ScalarExpression::AggCall{ .. } => todo!()
        }
    }

    pub fn eval_field(&self, batch: &RecordBatch) -> Field {
        match self {
            ScalarExpression::Constant(val) =>
                Field::new(format!("{}", val).as_str(), val.datatype(), true),
            ScalarExpression::ColumnRef(col) => col.to_field(),
            ScalarExpression::InputRef { index, .. } =>
                batch.schema().field(*index).clone(),
            ScalarExpression::Alias { alias, expr, .. } => {
                let logic_type = expr.return_type().unwrap();
                Field::new(alias, logic_type.into(), true)
            }
            ScalarExpression::TypeCast { expr, ty, .. } => {
                let inner_field = expr.eval_field(batch);
                let data_type = DataType::from(ty.clone());
                let new_name = format!("{}({})", data_type, inner_field.name());
                Field::new(new_name.as_str(), data_type, true)
            }
            ScalarExpression::AggCall { args, kind, ty } => {
                let inner_name = args[0].eval_field(batch).name().clone();
                let new_name = format!("{:?}({})", kind, inner_name);
                Field::new(new_name.as_str(), ty.clone().into(), true)
            }
            ScalarExpression::Binary { left_expr, right_expr, op, ty } => {
                let left = left_expr.eval_field(batch);
                let right = right_expr.eval_field(batch);
                let new_name = format!("{}{:?}{}", left.name(), op, right.name());
                let data_type = DataType::from(ty.clone());
                Field::new(new_name.as_str(), data_type, true)
            }
            ScalarExpression::IsNull { expr } => {
                let data_type = DataType::from(expr.return_type().unwrap());
                let new_name = format!("{}", data_type);
                Field::new(new_name.as_str(), data_type, true)
            }
            ScalarExpression::Unary { .. } => todo!()
        }
    }
}