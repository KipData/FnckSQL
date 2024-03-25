use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Insert {
    table_name: TableName,
    input: LogicalPlan,
    is_overwrite: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input,
            is_overwrite,
        }
    }
}

impl<T: Transaction> WriteExecutor<T> for Insert {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Insert {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Insert {
            table_name,
            mut input,
            is_overwrite,
        } = self;
        let mut tuples = Vec::new();
        let schema = input.output_schema().clone();

        let pk_index = schema
            .iter()
            .find(|col| col.desc.is_primary)
            .map(|col| col.id())
            .ok_or_else(|| DatabaseError::NotNull)?;

        if let Some(table_catalog) = transaction.table(table_name.clone()).cloned() {
            let types = table_catalog.types();
            #[for_await]
            for tuple in build_read(input, transaction) {
                let Tuple { values, .. } = tuple?;

                let mut tuple_map = HashMap::new();
                for (i, value) in values.into_iter().enumerate() {
                    tuple_map.insert(schema[i].id(), value);
                }
                let tuple_id = tuple_map
                    .get(&pk_index)
                    .cloned()
                    .ok_or(DatabaseError::NotNull)?;
                let mut values = Vec::with_capacity(table_catalog.columns_len());

                for col in table_catalog.columns() {
                    let value = {
                        let mut value = tuple_map.remove(&col.id());

                        if value.is_none() {
                            value = col.default_value()?;
                        }
                        value.unwrap_or_else(|| Arc::new(DataValue::none(col.datatype())))
                    };
                    if value.is_null() && !col.nullable {
                        return Err(DatabaseError::NotNull);
                    }
                    values.push(value)
                }
                tuples.push(Tuple {
                    id: Some(tuple_id),
                    values,
                });
            }
            for index_meta in table_catalog.indexes() {
                let exprs = index_meta.column_exprs(&table_catalog)?;

                for tuple in tuples.iter() {
                    let values = Projection::projection(tuple, &exprs, &schema)?;
                    let index = Index::new(index_meta.id, &values, index_meta.ty);
                    transaction.add_index(&table_name, index, tuple.id.as_ref().unwrap())?;
                }
            }
            for tuple in tuples {
                transaction.append(&table_name, tuple, &types, is_overwrite)?;
            }
        }
    }
}
