mod dql;

#[macro_use]
pub(crate) mod marcos;

use crate::execution::codegen::dql::aggregate::simple_agg::SimpleAgg;
use crate::execution::codegen::dql::filter::Filter;
use crate::execution::codegen::dql::join::hash_join::HashJoin;
use crate::execution::codegen::dql::limit::Limit;
use crate::execution::codegen::dql::projection::Projection;
use crate::execution::codegen::dql::seq_scan::{KipChannelSeqNext, SeqScan};
use crate::execution::codegen::dql::sort::Sort;
use crate::execution::volcano::dql::sort::sort;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::kip::KipTransaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use mlua::prelude::*;
use mlua::{UserData, UserDataMethods, UserDataRef, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

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
        methods.add_async_method("eval", |_, expr, tuple: UserDataRef<Tuple>| async move {
            Ok(ValuePtr(expr.eval(&tuple).unwrap()))
        });
        methods.add_async_method(
            "is_filtering",
            |_, expr, tuple: UserDataRef<Tuple>| async move {
                Ok(!matches!(
                    expr.eval(&tuple).unwrap().as_ref(),
                    DataValue::Boolean(Some(true))
                ))
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

    let func_sort =
        lua.create_function(|_, (sort_fields, tuples): (Vec<SortField>, Vec<Tuple>)| {
            Ok(sort(&sort_fields, tuples).unwrap())
        })?;

    lua.globals()
        .set("transaction", KipTransactionPtr(Arc::new(transaction)))?;
    lua.globals().set("sort", func_sort)?;

    script.push_str(
        r#"
            local results = {}
    "#,
    );
    build_script(0, plan, &lua, &mut script, Box::new(|_, _| Ok(())))?;
    script.push_str(
        r#"
            return results"#,
    );

    println!("Lua Script: \n{}", script);

    Ok(lua.load(script).eval_async().await?)
}

macro_rules! consumption {
    ($child_op_id:expr,$executor:expr, $childrens:expr, $lua:expr, $script:expr, $consume:expr) => {
        build_script(
            $child_op_id,
            $childrens.remove(0),
            $lua,
            $script,
            Box::new(move |lua, script| {
                $executor.consume(lua, script)?;
                $consume(lua, script)?;

                Ok(())
            }),
        )?;
    };
}

macro_rules! materialize {
    ($child_op_id:expr, $executor:expr, $childrens:expr, $lua:expr, $script:expr, $consume:expr) => {
        build_script(
            $child_op_id,
            $childrens.remove(0),
            $lua,
            $script,
            Box::new(move |_, _| Ok(())),
        )?;

        $executor.produce($lua, $script)?;
        $consume($lua, $script)?;
        $executor.consume($lua, $script)?;
    };
}

static OP_COUNTER: AtomicI64 = AtomicI64::new(0);

pub fn build_script(
    op_id: i64,
    plan: LogicalPlan,
    lua: &Lua,
    script: &mut String,
    consume: Box<dyn FnOnce(&Lua, &mut String) -> Result<(), ExecutorError>>,
) -> Result<(), ExecutorError> {
    let LogicalPlan {
        operator,
        mut childrens,
    } = plan;

    let func_op_id = || OP_COUNTER.fetch_add(1, Ordering::SeqCst);

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
            consumption!(func_op_id(), projection, childrens, lua, script, consume);
        }
        Operator::Filter(op) => {
            let mut filter = Filter::from((op, op_id));

            filter.produce(lua, script)?;
            consumption!(func_op_id(), filter, childrens, lua, script, consume);
        }
        Operator::Limit(op) => {
            let mut limit = Limit::from((op, op_id));

            limit.produce(lua, script)?;
            consumption!(func_op_id(), limit, childrens, lua, script, consume);
        }
        Operator::Aggregate(op) => {
            let mut simple_agg = SimpleAgg::from((op, op_id));

            materialize!(func_op_id(), simple_agg, childrens, lua, script, consume);
        }
        Operator::Sort(op) => {
            let mut sort = Sort::from((op, op_id));

            materialize!(func_op_id(), sort, childrens, lua, script, consume);
        }
        Operator::Join(op) => {
            let env = format!("hash_join_{}", op_id);

            script.push_str(
                format!(
                    r#"
            local join_temp_{op_id} = {{}}
            "#
                )
                .as_str(),
            );

            let insert_into_left = format!(
                r#"
                {}:left_build(tuple)
                goto continue
                "#,
                env
            );
            build_script(
                func_op_id(),
                childrens.remove(0),
                lua,
                script,
                Box::new(move |_, script| {
                    script.push_str(insert_into_left.as_str());

                    Ok(())
                }),
            )?;

            let insert_into_right = format!(
                r#"
                for _, tuple in ipairs({}:right_probe(tuple)) do
                    table.insert(join_temp_{op_id}, tuple)
                end
                goto continue
                "#,
                env
            );
            build_script(
                func_op_id(),
                childrens.remove(0),
                lua,
                script,
                Box::new(move |_, script| {
                    script.push_str(insert_into_right.as_str());

                    Ok(())
                }),
            )?;

            let mut join = HashJoin::from((op, op_id, env));

            join.produce(lua, script)?;
            consume(lua, script)?;
            join.consume(lua, script)?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::binder::{Binder, BinderContext};
    use crate::db::{Database, DatabaseError};
    use crate::execution::codegen::execute;
    use crate::parser::parse_sql;
    use crate::storage::kip::KipStorage;
    use crate::storage::Storage;
    use crate::types::tuple::create_table;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_scan() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let database = Database::with_kipdb(temp_dir.path()).await?;

        database
            .run("create table t1 (c1 int primary key, c2 int)")
            .await?;
        database
            .run("insert into t1 values(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)")
            .await?;
        database
            .run("create table t2 (c3 int primary key, c4 int)")
            .await?;
        database
            .run("insert into t2 values(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)")
            .await?;

        let transaction = database.storage.transaction().await?;

        // parse
        let stmts = parse_sql(
            "select sum(t2.c3) from t1 left join t2 on t1.c1 = t2.c3 and t1.c1 > 3 where t1.c1 > 0",
        )?;
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
        // println!("{:#?}", best_plan);

        let tuples = execute(best_plan, transaction).await?;

        println!("{}", create_table(&tuples));
        Ok(())
    }
}
