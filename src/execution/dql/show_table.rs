use crate::catalog::TableMeta;
use crate::execution::{Executor, ReadExecutor};
use crate::storage::Transaction;
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

pub struct ShowTables;

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ShowTables {
    fn execute(self, transaction: &'a T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let metas = throw!(transaction.table_metas());

                for TableMeta { table_name } in metas {
                    let values = vec![Arc::new(DataValue::Utf8 {
                        value: Some(table_name.to_string()),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    })];

                    yield Ok(Tuple { id: None, values });
                }
            },
        )
    }
}
