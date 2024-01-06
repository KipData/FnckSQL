mod dql;

#[macro_use]
pub(crate) mod marcos;

use crate::execution::codegen::dql::seq_scan::{KipChannelSeqNext, SeqScan};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::kip::KipTransaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use mlua::prelude::*;
use mlua::{UserData, UserDataMethods, UserDataRef, Value};
use std::sync::Arc;
use crate::execution::codegen::dql::filter::Filter;
use crate::execution::codegen::dql::limit::Limit;
use crate::execution::codegen::dql::projection::Projection;
use crate::planner::operator::scan::ScanOperator;

pub trait CodeGenerator {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError>;

    fn consume(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError>;
}

impl UserData for Tuple {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method_mut(
            "projection",
            |_, tuple, exprs: Vec<UserDataRef<ScalarExpression>>| async move {
                let mut columns = Vec::with_capacity(exprs.len());
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs.iter() {
                    values.push(expr.eval(&tuple).unwrap());
                    columns.push(expr.output_column());
                }

                tuple.columns = columns;
                tuple.values = values;

                Ok(())
            },
        );
    }
}

impl UserData for ScalarExpression {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method(
            "eval",
            |_, expr, tuple: UserDataRef<Tuple>| async move {
                Ok(ValuePtr(expr.eval(&tuple).unwrap()))
            },
        );
        methods.add_async_method(
            "is_filtering",
            |_, expr, tuple: UserDataRef<Tuple>| async move {
                Ok(!matches!(expr.eval(&tuple).unwrap().as_ref(), DataValue::Boolean(Some(true))))
            },
        );
    }
}

impl UserData for ValuePtr {}

#[derive(Debug)]
pub(crate) struct ValuePtr(Arc<DataValue>);

pub(crate) struct KipTransactionPtr(Arc<KipTransaction>);

impl UserData for KipTransactionPtr {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method(
            "new_seq_scan",
            |_, transaction, op: ScanOperator| async move {
                Ok(KipChannelSeqNext::new(transaction, op))
            },
        );
    }
}

impl_from_lua!(Tuple);
impl_from_lua!(ScalarExpression);
impl_from_lua!(ValuePtr);
impl_from_lua!(KipTransactionPtr);

pub async fn execute(
    plan: LogicalPlan,
    transaction: KipTransaction,
) -> Result<Vec<Tuple>, ExecutorError> {
    let lua = Lua::new();
    let mut script = String::new();

    lua.globals()
        .set("transaction", KipTransactionPtr(Arc::new(transaction)))?;

    build_script(0, plan, &lua, &mut script, Box::new(|_, _| Ok(())))?;
    println!("Lua Script: \n{}", script);

    Ok(lua.load(script).eval_async().await?)
}

macro_rules! build_script_with_consume {
    ($op_id: expr,$executor:expr, $childrens:expr, $lua:expr, $script:expr, $consume:expr) => {
        build_script($op_id + 1, $childrens.remove(0), $lua, $script, Box::new(move |lua, script| {
            $executor.consume(lua, script)?;
            $consume(lua, script)?;

            Ok(())
        }))?;
    };
}

pub fn build_script(
    op_id: i64,
    plan: LogicalPlan,
    lua: &Lua,
    script: &mut String,
    consume: Box<dyn FnOnce(&Lua, &mut String) -> Result<(), ExecutorError>>
) -> Result<(), ExecutorError> {
    let LogicalPlan {
        operator,
        mut childrens,
    } = plan;

    match operator {
        Operator::Scan(op) => {
            let mut seq_scan = SeqScan::from((op, op_id));

            seq_scan.produce(lua, script)?;
            consume(lua, script)?;
            seq_scan.consume(lua, script)?;
        }
        Operator::Project(op) => {
            let mut projection = Projection::from((op, op_id));

            projection.produce(lua, script)?;
            build_script_with_consume!(op_id, projection, childrens, lua, script, consume);
        }
        Operator::Filter(op) => {
            let mut filter = Filter::from((op, op_id));

            filter.produce(lua, script)?;
            build_script_with_consume!(op_id, filter, childrens, lua, script, consume);
        }
        Operator::Limit(op) => {
            let mut limit = Limit::from((op, op_id));

            limit.produce(lua, script)?;
            build_script_with_consume!(op_id, limit, childrens, lua, script, consume);
        }
        _ => unreachable!(),
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;
    use crate::binder::{Binder, BinderContext};
    use crate::db::{Database, DatabaseError};
    use crate::execution::codegen::execute;
    use crate::parser::parse_sql;
    use crate::storage::kip::KipStorage;
    use crate::storage::Storage;
    use crate::types::tuple::create_table;

    #[tokio::test]
    async fn test_scan() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let database = Database::with_kipdb(temp_dir.path()).await?;

        database.run("create table t1 (c1 int primary key, c2 int)").await?;
        database.run("insert into t1 values(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)").await?;

        let transaction = database.storage.transaction().await?;

        // parse
        let stmts = parse_sql("select c1, c1 + c2 from t1 where c1 > 0 limit 3 offset 1")?;
        let binder = Binder::new(BinderContext::new(&transaction));
        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(&stmts[0])?;
        // println!("source_plan plan: {:#?}", source_plan);

        let best_plan = Database::<KipStorage>::default_optimizer(source_plan).find_best()?;

        let tuples = execute(best_plan, transaction).await?;

        println!("{}", create_table(&tuples));
        Ok(())
    }
}
