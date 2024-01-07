use crate::execution::codegen::CodeGenerator;
use crate::execution::volcano::dql::join::hash_join::JoinStatus;
use crate::execution::ExecutorError;
use crate::impl_from_lua;
use crate::planner::operator::join::JoinOperator;
use crate::types::tuple::Tuple;
use mlua::prelude::{LuaResult, LuaValue};
use mlua::{FromLua, Lua, UserData, UserDataMethods, Value};
use std::mem;

pub struct HashJoin {
    pub(crate) id: i64,
    join_status: Option<JoinStatus>,
    is_produced: bool,

    env_left_input: String,
    env_right_input: String,
}

impl From<(JoinOperator, i64, String, String)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type }, id, env_left_input, env_right_input): (
            JoinOperator,
            i64,
            String,
            String,
        ),
    ) -> Self {
        HashJoin {
            id,
            join_status: Some(JoinStatus::new(on, join_type)),
            is_produced: false,
            env_left_input,
            env_right_input,
        }
    }
}

impl UserData for JoinStatus {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("left_build", |_, join_status, tuple: Tuple| {
            join_status.left_build(tuple).unwrap();

            Ok(())
        });
        methods.add_method_mut("right_probe", |_, join_status, tuple: Tuple| {
            Ok(join_status.right_probe(tuple).unwrap())
        });
        methods.add_method_mut("drop_build", |_, join_status, ()| {
            Ok(join_status.build_drop())
        });
    }
}

impl_from_lua!(JoinStatus);

impl CodeGenerator for HashJoin {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(join_status) = self.join_status.take() {
            let env = format!("hash_join_{}", self.id);
            lua.globals().set(env.as_str(), join_status)?;

            script.push_str(
                format!(
                    r#"
            for _, tuple in ipairs({}) do
                {env}:left_build(tuple)
            end

            local temp = {{}}
            {} = {{}}

            for _, tuple in ipairs({}) do
                for _, tuple in ipairs({env}:right_probe(tuple)) do
                    table.insert(temp, tuple)
                end
            end

            {} = {{}}

            for _, tuple in ipairs({env}:drop_build()) do
                table.insert(temp, tuple)
            end

            for index, tuple in ipairs(temp) do
                index = index - 1
            "#,
                    self.env_left_input,
                    self.env_left_input,
                    self.env_right_input,
                    self.env_right_input
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
