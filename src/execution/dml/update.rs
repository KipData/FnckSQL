use crate::catalog::TableName;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::types;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

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

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Update {
    fn execute_mut(self, transaction: &'a mut T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
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
                    let mut coroutine = build_read(values, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let Tuple { values, .. } = throw!(tuple);
                        for i in 0..values.len() {
                            value_map.insert(values_schema[i].id(), values[i].clone());
                        }
                    }
                    drop(coroutine);
                    let mut coroutine = build_read(input, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let tuple: Tuple = throw!(tuple);

                        tuples.push(tuple);
                    }
                    drop(coroutine);
                    let mut index_metas = Vec::new();
                    for index_meta in table_catalog.indexes() {
                        let exprs = throw!(index_meta.column_exprs(&table_catalog));

                        for tuple in tuples.iter() {
                            let values =
                                throw!(Projection::projection(tuple, &exprs, &input_schema));
                            let index = Index::new(index_meta.id, &values, index_meta.ty);
                            throw!(transaction.del_index(
                                &table_name,
                                &index,
                                Some(tuple.id.as_ref().unwrap())
                            ));
                        }
                        index_metas.push((index_meta, exprs));
                    }
                    for mut tuple in tuples {
                        let mut is_overwrite = true;

                        for (i, column) in input_schema.iter().enumerate() {
                            if let Some(value) = value_map.get(&column.id()) {
                                if column.desc.is_primary {
                                    let old_key = tuple.id.replace(value.clone()).unwrap();

                                    throw!(transaction.delete(&table_name, old_key));
                                    is_overwrite = false;
                                }
                                tuple.values[i] = value.clone();
                            }
                        }
                        for (index_meta, exprs) in index_metas.iter() {
                            let values =
                                throw!(Projection::projection(&tuple, exprs, &input_schema));
                            let index = Index::new(index_meta.id, &values, index_meta.ty);
                            throw!(transaction.add_index(
                                &table_name,
                                index,
                                tuple.id.as_ref().unwrap()
                            ));
                        }

                        throw!(transaction.append(&table_name, tuple, &types, is_overwrite));
                    }
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
