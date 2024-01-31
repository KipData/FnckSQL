use crate::execution::codegen::{CodeGenerator, KipTransactionPtr};
use crate::execution::ExecutorError;
use crate::impl_from_lua;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::tuple::Tuple;
use mlua::prelude::{LuaResult, LuaValue};
use mlua::{FromLua, Lua, UserData, UserDataMethods, Value};
use std::sync::Arc;
use tokio::sync::mpsc;

const DEFAULT_CHANNEL_BUF: usize = 5;

pub(crate) struct IndexScan {
    id: i64,
    op: Option<ScanOperator>,
}

impl From<(ScanOperator, i64)> for IndexScan {
    fn from((op, id): (ScanOperator, i64)) -> Self {
        IndexScan { id, op: Some(op) }
    }
}

pub(crate) struct KipChannelIndexNext(mpsc::Receiver<Tuple>);

impl KipChannelIndexNext {
    pub(crate) fn new(transaction: &KipTransactionPtr, op: ScanOperator) -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_BUF);

        let ScanOperator {
            table_name,
            projection_columns: columns,
            limit,
            index_by,
            ..
        } = op;
        let inner = KipTransactionPtr(Arc::clone(&transaction.0));

        if let Some((index_meta, binaries)) = index_by {
            tokio::spawn(async move {
                let mut iter = inner
                    .0
                    .read_by_index(table_name, limit, columns, index_meta, binaries)
                    .unwrap();

                while let Some(tuple) = iter.next_tuple().unwrap() {
                    if tx.send(tuple).await.is_err() {
                        break;
                    }
                }
            });
        } else {
            unreachable!("`index_by` cannot be None")
        }

        KipChannelIndexNext(rx)
    }
}

impl UserData for KipChannelIndexNext {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method_mut("next", |_, next, ()| async move { Ok(next.0.recv().await) });
    }
}

impl_from_lua!(KipChannelIndexNext);

impl CodeGenerator for IndexScan {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(op) = self.op.take() {
            let env = format!("scan_op_{}", self.id);
            lua.globals().set(env.as_str(), op)?;

            script.push_str(
                format!(
                    r#"
            local index_scan_{} = transaction:new_index_scan({})
            local index = -1

            for tuple in function() return index_scan_{}:next() end do
                index = index + 1
            "#,
                    self.id, env, self.id
                )
                .as_str(),
            )
        }

        Ok(())
    }

    fn consume(&mut self, _: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        script.push_str(
            r#"
                table.insert(results, tuple)
                ::continue::
            end
            "#,
        );

        Ok(())
    }
}
