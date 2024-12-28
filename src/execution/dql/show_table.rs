use crate::catalog::TableMeta;
use crate::execution::{Executor, ReadExecutor};
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct ShowTables;

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ShowTables {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let metas = throw!(unsafe { &mut (*transaction) }.table_metas());

                for TableMeta { table_name } in metas {
                    let values = vec![DataValue::Utf8 {
                        value: table_name.to_string(),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }];

                    yield Ok(Tuple::new(None, values));
                }
            },
        )
    }
}
