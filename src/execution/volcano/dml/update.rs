use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::types;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::collections::HashMap;

pub struct Update {
    table_name: TableName,
    input: LogicalPlan,
    values: LogicalPlan,
}

impl From<(UpdateOperator, LogicalPlan, LogicalPlan)> for Update {
    fn from(
        (UpdateOperator { table_name }, input, values): (UpdateOperator, LogicalPlan, LogicalPlan),
    ) -> Self {
        Update {
            table_name,
            input,
            values,
        }
    }
}

impl<T: Transaction> WriteExecutor<T> for Update {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Update {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Update {
            table_name,
            mut input,
            mut values,
        } = self;
        let values_schema = values.output_schema().clone();
        let input_schema = input.output_schema().clone();
        let types = types(&input_schema);

        if let Some(table_catalog) = transaction.table(table_name.clone()).cloned() {
            let mut value_map = HashMap::new();
            let mut tuples = Vec::new();

            // only once
            #[for_await]
            for tuple in build_read(values, transaction) {
                let Tuple { values, .. } = tuple?;
                for i in 0..values.len() {
                    value_map.insert(values_schema[i].id(), values[i].clone());
                }
            }
            #[for_await]
            for tuple in build_read(input, transaction) {
                let tuple: Tuple = tuple?;

                tuples.push(tuple);
            }
            let mut index_metas = Vec::new();
            for index_meta in table_catalog.indexes() {
                let exprs = index_meta.column_exprs(&table_catalog)?;

                for tuple in tuples.iter() {
                    let values = Projection::projection(tuple, &exprs, &input_schema)?;
                    let index = Index::new(index_meta.id, &values, index_meta.ty);
                    transaction.del_index(&table_name, &index, Some(tuple.id.as_ref().unwrap()))?;
                }
                index_metas.push((index_meta, exprs));
            }
            for mut tuple in tuples {
                let mut is_overwrite = true;

                for (i, column) in input_schema.iter().enumerate() {
                    if let Some(value) = value_map.get(&column.id()) {
                        if column.desc.is_primary {
                            let old_key = tuple.id.replace(value.clone()).unwrap();

                            transaction.delete(&table_name, old_key)?;
                            is_overwrite = false;
                        }
                        tuple.values[i] = value.clone();
                    }
                }
                for (index_meta, exprs) in index_metas.iter() {
                    let values = Projection::projection(&tuple, exprs, &input_schema)?;
                    let index = Index::new(index_meta.id, &values, index_meta.ty);
                    transaction.add_index(&table_name, index, tuple.id.as_ref().unwrap())?;
                }

                transaction.append(&table_name, tuple, &types, is_overwrite)?;
            }
        }
    }
}
