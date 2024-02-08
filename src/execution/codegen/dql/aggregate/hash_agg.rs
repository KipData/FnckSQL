use crate::execution::codegen::CodeGenerator;
use crate::execution::volcano::dql::aggregate::hash_agg::HashAggStatus;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::impl_from_lua;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::types::tuple::Tuple;
use mlua::prelude::{LuaResult, LuaValue};
use mlua::{FromLua, Lua, UserData, UserDataMethods, Value};
use std::mem;

pub struct HashAgg {
    id: i64,
    agg_calls: Option<Vec<ScalarExpression>>,
    groupby_exprs: Option<Vec<ScalarExpression>>,
    is_produced: bool,
}

impl From<(AggregateOperator, i64)> for HashAgg {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            id,
        ): (AggregateOperator, i64),
    ) -> Self {
        HashAgg {
            id,
            agg_calls: Some(agg_calls),
            groupby_exprs: Some(groupby_exprs),
            is_produced: false,
        }
    }
}

impl UserData for HashAggStatus {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("update", |_, agg_status, tuple: Tuple| {
            agg_status.update(tuple).unwrap();

            Ok(())
        });
        methods.add_method_mut("to_tuples", |_, agg_status, ()| {
            Ok(agg_status.as_tuples().unwrap())
        });
    }
}

impl_from_lua!(HashAggStatus);

impl CodeGenerator for HashAgg {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let (Some(agg_calls), Some(groupby_exprs)) =
            (self.agg_calls.take(), self.groupby_exprs.take())
        {
            let env = format!("hash_agg_{}", self.id);
            lua.globals()
                .set(env.as_str(), HashAggStatus::new(agg_calls, groupby_exprs))?;

            script.push_str(
                format!(
                    r#"
            for _, tuple in ipairs(results) do
                {}:update(tuple)
            end

            results = {{}}

            for index, tuple in ipairs({}:to_tuples()) do
                index = index - 1
            "#,
                    env, env
                )
                .as_str(),
            );

            self.is_produced = true;
        }

        Ok(())
    }

    fn consume(&mut self, _: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if mem::replace(&mut self.is_produced, false) {
            script.push_str(
                r#"
                table.insert(results, tuple)
                ::continue::
            end
            "#,
            );
        }

        Ok(())
    }
}
