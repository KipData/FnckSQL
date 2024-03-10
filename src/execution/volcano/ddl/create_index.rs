use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::ColumnId;
use futures_async_stream::try_stream;

pub struct CreateIndex {
    op: CreateIndexOperator,
    input: LogicalPlan,
}

impl From<(CreateIndexOperator, LogicalPlan)> for CreateIndex {
    fn from((op, input): (CreateIndexOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> WriteExecutor<T> for CreateIndex {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl CreateIndex {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    async fn _execute<T: Transaction>(mut self, transaction: &mut T) {
        let CreateIndexOperator {
            table_name,
            index_name,
            columns,
            if_not_exists,
            ty,
        } = self.op;
        let (column_ids, column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
            .into_iter()
            .filter_map(|column| {
                column
                    .id()
                    .map(|id| (id, ScalarExpression::ColumnRef(column)))
            })
            .unzip();
        let schema = self.input.output_schema().clone();
        let index_id = match transaction.add_index_meta(&table_name, index_name, column_ids, ty) {
            Ok(index_id) => index_id,
            Err(DatabaseError::DuplicateIndex(index_name)) => {
                return if if_not_exists {
                    Ok(())
                } else {
                    Err(DatabaseError::DuplicateIndex(index_name))
                }
            }
            err => err?,
        };
        let mut index_values = Vec::new();

        #[for_await]
        for tuple in build_read(self.input, transaction) {
            let mut tuple: Tuple = tuple?;

            let tuple_id = if let Some(tuple_id) = tuple.id.take() {
                tuple_id
            } else {
                continue;
            };
            index_values.push((
                tuple_id,
                Projection::projection(&tuple, &column_exprs, &schema)?,
            ));
        }
        for (tuple_id, values) in index_values {
            let index = Index::new(index_id, &values, ty);
            transaction.add_index(table_name.as_str(), index, &tuple_id)?;
        }
    }
}
