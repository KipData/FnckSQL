use std::mem;
use itertools::Itertools;
use mlua::prelude::{LuaValue, LuaResult};
use mlua::{Lua, UserData, UserDataMethods, FromLua, Value};
use crate::catalog::ColumnRef;
use crate::execution::codegen::CodeGenerator;
use crate::execution::ExecutorError;
use crate::execution::volcano::dql::aggregate::{Accumulator, create_accumulators};
use crate::expression::ScalarExpression;
use crate::impl_from_lua;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;

pub struct SimpleAgg {
    id: i64,
    agg_calls: Option<Vec<ScalarExpression>>,
    is_produced: bool,
}

impl From<(AggregateOperator, i64)> for SimpleAgg {
    fn from((AggregateOperator { agg_calls, .. }, id): (AggregateOperator, i64)) -> Self {
        SimpleAgg {
            id,
            agg_calls: Some(agg_calls),
            is_produced: false,
        }
    }
}

pub(crate) struct AggAccumulators {
    agg_calls: Vec<ScalarExpression>,
    accs: Vec<Box<dyn Accumulator>>,
    columns: Vec<ColumnRef>
}

impl AggAccumulators {
    pub(crate) fn new(agg_calls: Vec<ScalarExpression>) -> Self {
        let accs = create_accumulators(&agg_calls);
        let columns = agg_calls
            .iter()
            .map(|expr| expr.output_column())
            .collect_vec();

        AggAccumulators {
            agg_calls,
            accs,
            columns,
        }
    }
}

impl UserData for AggAccumulators {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method_mut(
            "update",
            |_, agg_accumulators, tuple: Tuple| async move {
                if agg_accumulators.accs.is_empty() {
                    return Ok(());
                }

                let values: Vec<ValueRef> = agg_accumulators
                    .agg_calls
                    .iter()
                    .map(|expr| match expr {
                        ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple),
                        _ => unreachable!(),
                    })
                    .try_collect().unwrap();

                for (acc, value) in agg_accumulators.accs.iter_mut().zip_eq(values.iter()) {
                    acc.update_value(value).unwrap();
                }

                Ok(())
            },
        );
        methods.add_async_method_mut(
            "to_tuple",
            |_, agg_accumulators, ()| async move {
                let columns = mem::replace(&mut agg_accumulators.columns, vec![]);
                let values: Vec<ValueRef> = agg_accumulators
                    .accs
                    .drain(..)
                    .into_iter()
                    .map(|acc| acc.evaluate())
                    .try_collect().unwrap();

                Ok(Tuple {
                    id: None,
                    columns,
                    values,
                })
            },
        );
    }
}

impl_from_lua!(AggAccumulators);

impl CodeGenerator for SimpleAgg {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(agg_calls) = self.agg_calls.take() {
            let env = format!("simple_agg_{}", self.id);
            lua.globals().set(env.as_str(), AggAccumulators::new(agg_calls))?;

            script.push_str(format!(r#"
            for _, tuple in ipairs(results) do
                {}:update(tuple)
            end

            results = {{}}

            for index, tuple in ipairs({{{}:to_tuple()}}) do
                index = index - 1
            "#, &env, &env).as_str());

            self.is_produced = true;
        }

        Ok(())
    }

    fn consume(&mut self, _: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if mem::replace(&mut self.is_produced, false) {
            script.push_str(r#"
                table.insert(results, tuple)
                ::continue::
            end
            "#);
        }

        Ok(())
    }
}