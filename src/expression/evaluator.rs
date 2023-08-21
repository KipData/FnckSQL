use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use crate::catalog::ColumnCatalog;
use crate::execution_ap::ExecutorError;
use crate::expression::array_compute::{binary_op, binary_op_tp};
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

impl ScalarExpression {
    pub fn eval_column(&self, batch: &RecordBatch) -> Result<ArrayRef, ExecutorError> {
        match &self {
            ScalarExpression::Constant(val) =>
                Ok(val.to_array_of_size(batch.num_rows())),
            ScalarExpression::ColumnRef(col) => {
                let index = batch.schema().index_of(&col.name)?;
                Ok(batch.column(index).clone())
            },
            ScalarExpression::InputRef{ index, .. } =>
                Ok(batch.column(*index % batch.num_columns()).clone()),
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
                let logic_type = expr.return_type();
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
                let data_type = DataType::from(expr.return_type());
                let new_name = format!("{}", data_type);
                Field::new(new_name.as_str(), data_type, true)
            }
            ScalarExpression::Unary { .. } => todo!()
        }
    }

    pub fn eval_column_tp(&self, columns: &Vec<ColumnCatalog>, tuple: &Tuple) -> Result<DataValue, ExecutorError> {
        match &self {
            ScalarExpression::Constant(val) =>
                Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let index = columns
                    .binary_search_by(|tul_col| tul_col.name.cmp(&col.name))
                    .unwrap();

                Ok(tuple.values[index].clone())
            },
            ScalarExpression::InputRef{ index, .. } =>
                Ok(tuple.values[*index].clone()),
            ScalarExpression::Alias{ expr, .. } =>
                expr.eval_column_tp(columns, tuple),
            ScalarExpression::TypeCast{ expr, ty, .. } => {
                let value = expr.eval_column_tp(columns, tuple)?;

                Ok(value.cast(ty))
            }
            ScalarExpression::Binary{ left_expr, right_expr, op, .. } => {
                let left = left_expr.eval_column_tp(columns, tuple)?;
                let right = right_expr.eval_column_tp(columns, tuple)?;
                Ok(binary_op_tp(&left, &right, op))
            }
            ScalarExpression::IsNull{ expr } => {
                Ok(DataValue::Boolean(Some(expr.nullable())))
            }
            ScalarExpression::Unary{ .. } => todo!(),
            ScalarExpression::AggCall{ .. } => todo!()
        }
    }
}