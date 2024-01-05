use mlua::Lua;
use crate::execution::codegen::CodeGenerator;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;

pub struct Filter {
    id: i64,
    predicate: Option<ScalarExpression>,
}

impl From<(FilterOperator, i64)> for Filter {
    fn from((FilterOperator { predicate, .. }, id): (FilterOperator, i64)) -> Self {
        Filter {
            id,
            predicate: Some(predicate)
        }
    }
}

impl CodeGenerator for Filter {
    fn produce(&mut self, _: &Lua, _: &mut String) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn consume(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(predicate) = self.predicate.take() {
            let env = format!("predicate_{}", self.id);
            lua.globals().set(env.as_str(), predicate)?;

            script.push_str(format!(r#"
                    if {}:is_filtering(tuple) then
                        goto continue
                    end
            "#, env).as_str())
        }

        Ok(())
    }
}