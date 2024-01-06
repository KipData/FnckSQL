use mlua::Lua;
use crate::execution::codegen::CodeGenerator;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;

pub struct Projection {
    id: i64,
    exprs: Option<Vec<ScalarExpression>>,
}

impl From<(ProjectOperator, i64)> for Projection {
    fn from((ProjectOperator { exprs }, id): (ProjectOperator, i64)) -> Self {
        Projection {
            id,
            exprs: Some(exprs)
        }
    }
}

impl CodeGenerator for Projection {
    fn produce(&mut self, _: &Lua, _: &mut String) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn consume(&mut self, lua: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        if let Some(exprs) = self.exprs.take() {
            let env = format!("project_exprs_{}", self.id);
            lua.globals().set(env.as_str(), exprs)?;

            script.push_str(format!(r#"
                tuple:projection({})
            "#, env).as_str(),
            )
        }

        Ok(())
    }
}