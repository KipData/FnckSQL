use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Delete {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Delete {
                    table_name,
                    mut input,
                } = self;

                let schema = input.output_schema().clone();
                let table = throw!(throw!(transaction.table(cache.0, table_name.clone()))
                    .cloned()
                    .ok_or(DatabaseError::TableNotFound));
                let mut tuple_ids = Vec::new();
                let mut indexes: HashMap<IndexId, Value> = HashMap::new();

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    for index_meta in table.indexes() {
                        if let Some(Value {
                            exprs, value_rows, ..
                        }) = indexes.get_mut(&index_meta.id)
                        {
                            value_rows.push(throw!(Projection::projection(&tuple, exprs, &schema)));
                        } else {
                            let exprs = throw!(index_meta.column_exprs(&table));
                            let values = throw!(Projection::projection(&tuple, &exprs, &schema));

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
                    if let Some(tuple_id) = tuple.id {
                        tuple_ids.push(tuple_id);
                    }
                }
                drop(coroutine);
                for (
                    index_id,
                    Value {
                        value_rows,
                        index_ty,
                        ..
                    },
                ) in indexes
                {
                    for (i, values) in value_rows.into_iter().enumerate() {
                        let Some(value) = DataValue::values_to_tuple(values) else {
                            continue;
                        };

                        throw!(transaction.del_index(
                            &table_name,
                            &Index::new(index_id, &value, index_ty),
                            Some(&tuple_ids[i]),
                        ));
                    }
                }
                for tuple_id in tuple_ids {
                    throw!(transaction.remove_tuple(&table_name, &tuple_id));
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    value_rows: Vec<Vec<DataValue>>,
    index_ty: IndexType,
}
