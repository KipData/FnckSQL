use crate::execution::codegen::CodeGenerator;
use crate::execution::ExecutorError;
use crate::planner::operator::limit::LimitOperator;
use mlua::Lua;

pub struct Limit {
    id: i64,
    offset: Option<usize>,
    limit: Option<usize>,
}

impl From<(LimitOperator, i64)> for Limit {
    fn from((LimitOperator { offset, limit }, id): (LimitOperator, i64)) -> Self {
        Limit { offset, limit, id }
    }
}

impl CodeGenerator for Limit {
    fn produce(&mut self, _: &Lua, _: &mut String) -> Result<(), ExecutorError> {
        Ok(())
    }

    fn consume(&mut self, _: &Lua, script: &mut String) -> Result<(), ExecutorError> {
        let Limit { offset, limit, .. } = self;

        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let offset_val = offset.unwrap_or(0);
        let offset_limit = offset_val + limit.unwrap_or(1) - 1;

        script.push_str(
            format!(
                r#"
                if index < {} then
                    goto continue
                end
                if index > {} then
                    break
                end
            "#,
                offset_val, offset_limit
            )
            .as_str(),
        );

        Ok(())
    }
}
