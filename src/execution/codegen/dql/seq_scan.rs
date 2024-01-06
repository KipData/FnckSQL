use crate::execution::codegen::{CodeGenerator, KipTransactionPtr};
use crate::execution::ExecutorError;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::tuple::Tuple;
use mlua::prelude::{LuaResult, LuaValue};
use mlua::{FromLua, Lua, UserData, UserDataMethods, Value};
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::impl_from_lua;

const DEFAULT_CHANNEL_BUF: usize = 5;

pub(crate) struct SeqScan {
    id: i64,
    op: Option<ScanOperator>,
}

impl From<(ScanOperator, i64)> for SeqScan {
    fn from((op, id): (ScanOperator, i64)) -> Self {
        SeqScan { id, op: Some(op) }
    }
}

pub(crate) struct KipChannelSeqNext(mpsc::Receiver<Tuple>);

impl KipChannelSeqNext {
    pub(crate) fn new(transaction: &KipTransactionPtr, op: ScanOperator) -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_BUF);

        let ScanOperator {
            table_name,
            columns,
            limit,
            ..
        } = op;
        let inner = KipTransactionPtr(Arc::clone(&transaction.0));

        tokio::spawn(async move {
            let mut iter = inner.0.read(table_name, limit, columns).unwrap();

            while let Some(tuple) = iter.next_tuple().unwrap() {
                if tx.send(tuple).await.is_err() {
                    break;
                }
            }
        });

        KipChannelSeqNext(rx)
    }
}

impl UserData for KipChannelSeqNext {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method_mut(
            "next",
            |_, next, ()| async move { Ok(next.0.recv().await) },
        );
    }
}

impl UserData for ScanOperator {}

impl_from_lua!(KipChannelSeqNext);
impl_from_lua!(ScanOperator);

impl CodeGenerator for SeqScan {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(op) = self.op.take() {
            let env = format!("scan_op_{}", self.id);
            lua.globals().set(env.as_str(), op)?;

            script.push_str(format!(
                r#"
                local index = -1
                local results = {{}}
                local seq_scan = transaction:new_seq_scan({})

                for tuple in function() return seq_scan:next() end do
                    index = index + 1
            "#, env).as_str())
        }

        Ok(())
    }

    fn consume(&mut self, _: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        script.push_str(
            r#"
                    table.insert(results, tuple)
                    ::continue::
                end

                return results
            "#,
        );

        Ok(())
    }
}
