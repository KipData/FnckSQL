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
#[macro_export]
macro_rules! implement_from_tuple {
    ($struct_name:ident, ($($field_name:ident : $field_type:ty => $closure:expr),+)) => {
        impl From<Tuple> for $struct_name {
            fn from(tuple: Tuple) -> Self {
                fn try_get<T: 'static>(tuple: &Tuple, field_name: &str) -> Option<DataValue> {
                    let ty = LogicalType::type_trans::<T>()?;
                    let (idx, _) = tuple.schema_ref
                        .iter()
                        .enumerate()
                        .find(|(_, col)| col.name() == field_name)?;

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
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;

    fn build_tuple() -> Tuple {
        let columns = Arc::new(vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar(None), false, false, None),
            )),
        ]);
        let values = vec![
            Arc::new(DataValue::Int32(Some(9))),
            Arc::new(DataValue::Utf8(Some("LOL".to_string()))),
        ];

        Tuple {
            id: None,
            schema_ref: columns,
            values,
        }
    }

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
