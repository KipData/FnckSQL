use std::mem;
use mlua::prelude::{LuaValue, LuaResult};
use mlua::{Lua, UserData, FromLua, Value};
use crate::execution::codegen::CodeGenerator;
use crate::execution::ExecutorError;
use crate::impl_from_lua;
use crate::planner::operator::sort::{SortField, SortOperator};

pub struct Sort {
    id: i64,
    sort_fields: Option<Vec<SortField>>,
    is_produced: bool,
}

impl UserData for SortField {}

impl_from_lua!(SortField);

impl From<(SortOperator, i64)> for Sort {
    fn from((SortOperator { sort_fields, .. }, id): (SortOperator, i64)) -> Self {
        Sort {
            id,
            sort_fields: Some(sort_fields),
            is_produced: false,
        }
    }
}

impl CodeGenerator for Sort {
    fn produce(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(sort_fields) = self.sort_fields.take() {
            let env = format!("sort_fields_{}", self.id);
            lua.globals().set(env.as_str(), sort_fields)?;

            script.push_str(format!(r#"
            local temp = sort({}, results)
            results = {{}}

            for index, tuple in ipairs(temp) do
                index = index - 1
            "#, &env).as_str());

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