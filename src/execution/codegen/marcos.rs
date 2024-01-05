#[macro_export]
macro_rules! impl_from_lua {
    ($ty:ty) => {
        impl<'lua> FromLua<'lua> for $ty {
            fn from_lua(value: LuaValue<'lua>, _: &'lua Lua) -> LuaResult<Self> {
                match value {
                    Value::UserData(ud) => Ok(ud.take::<Self>()?),
                    _ => unreachable!(),
                }
            }
        }
    };
}