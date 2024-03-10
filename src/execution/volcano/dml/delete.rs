use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use futures_async_stream::try_stream;
use std::collections::HashMap;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<T: Transaction> WriteExecutor<T> for Delete {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    value_rows: Vec<Vec<ValueRef>>,
    index_ty: IndexType,
}

impl Delete {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Delete {
            table_name,
            mut input,
        } = self;
        let schema = input.output_schema().clone();
        let table = transaction
            .table(table_name.clone())
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;
        let mut tuple_ids = Vec::new();
        let mut indexes: HashMap<IndexId, Value> = HashMap::new();

        #[for_await]
        for tuple in build_read(input, transaction) {
            let tuple: Tuple = tuple?;

            for index_meta in table.indexes() {
                if let Some(Value {
                    exprs, value_rows, ..
                }) = indexes.get_mut(&index_meta.id)
                {
                    value_rows.push(Projection::projection(&tuple, exprs, &schema)?);
                } else {
                    let exprs = index_meta.column_exprs(&table)?;
                    let values = Projection::projection(&tuple, &exprs, &schema)?;

                    indexes.insert(
                        index_meta.id,
                        Value {
                            exprs,
                            value_rows: vec![values],
                            index_ty: index_meta.ty,
                        },
                    );
                }
            }
            tuple_ids.push(tuple.id.unwrap());
        }
        for (
            i,
            (
                index_id,
                Value {
                    value_rows,
                    index_ty,
                    ..
                },
            ),
        ) in indexes.into_iter().enumerate()
        {
            for values in value_rows {
                transaction.del_index(
                    &table_name,
                    &Index::new(index_id, &values, index_ty),
                    Some(&tuple_ids[i]),
                )?;
            }
        }
        for tuple_id in tuple_ids {
            transaction.delete(&table_name, tuple_id)?;
        }
    }
}
