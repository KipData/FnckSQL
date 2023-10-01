
/// # Examples
///
/// ```
///struct MyStruct {
///   c1: i32,
///   c2: String,
///}
///
///implement_from_tuple!(
///     MyStruct, (
///         c1: i32 => |inner: &mut MyStruct, value| {
///             if let DataValue::Int32(Some(val)) = value {
///                 inner.c1 = val;
///             }
///         },
///         c2: String => |inner: &mut MyStruct, value| {
///             if let DataValue::Utf8(Some(val)) = value {
///                 inner.c2 = val;
///             }
///         }
///     )
/// );
/// ```
macro_rules! implement_from_tuple {
    ($struct_name:ident, ($($field_name:ident : $field_type:ty => $closure:expr),+)) => {
        use kip_sql::types::tuple::Tuple;
        use kip_sql::types::LogicalType;
        use kip_sql::types::value::DataValue;

        impl From<Tuple> for $struct_name {
            fn from(tuple: Tuple) -> Self {
                fn try_get<T: 'static>(tuple: &Tuple, field_name: &str) -> Option<DataValue> {
                    let ty = LogicalType::type_trans::<T>()?;
                    let (idx, _) = tuple.columns
                        .iter()
                        .enumerate()
                        .find(|(_, col)| &col.name == field_name)?;

                    DataValue::clone(&tuple.values[idx])
                        .cast(&ty)
                        .ok()
                }

                let mut struct_instance = $struct_name::default();
                $(
                    if let Some(value) = try_get::<$field_type>(&tuple, stringify!($field_name)) {
                        $closure(
                            &mut struct_instance,
                            value
                        );
                    }
                )+
                struct_instance
            }
        }
    };
}

#[cfg(test)]
mod test {
    use kip_sql::mock::build_tuple;

    #[derive(Default, Debug, PartialEq)]
    struct MyStruct {
        c1: i32,
        c2: String,
    }

    implement_from_tuple!(
        MyStruct, (
            c1: i32 => |inner: &mut MyStruct, value| {
                if let DataValue::Int32(Some(val)) = value {
                    inner.c1 = val;
                }
            },
            c2: String => |inner: &mut MyStruct, value| {
                if let DataValue::Utf8(Some(val)) = value {
                    inner.c2 = val;
                }
            }
        )
    );

    #[test]
    fn test_from_tuple() {
        let my_struct = MyStruct::from(build_tuple());

        println!("{:?}", my_struct);

        assert_eq!(my_struct.c1, 9);
        assert_eq!(my_struct.c2, "LOL");
    }
}