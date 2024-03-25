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
        impl From<(&SchemaRef, Tuple)> for $struct_name {
            fn from((schema, tuple): (&SchemaRef, Tuple)) -> Self {
                fn try_get<T: 'static>(tuple: &Tuple, schema: &SchemaRef, field_name: &str) -> Option<DataValue> {
                    let ty = LogicalType::type_trans::<T>()?;
                    let (idx, _) = schema
                        .iter()
                        .enumerate()
                        .find(|(_, col)| col.name() == field_name)?;

                    DataValue::clone(&tuple.values[idx])
                        .cast(&ty)
                        .ok()
                }

                let mut struct_instance = $struct_name::default();
                $(
                    if let Some(value) = try_get::<$field_type>(&tuple, schema, stringify!($field_name)) {
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

/// # Examples
///
/// ```
/// function!(MyFunction::sum(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: ValueRef, v2: ValueRef| {
///     DataValue::binary_op(&v1, &v2, &BinaryOperator::Plus)
/// });
///
/// let fnck_sql = DataBaseBuilder::path("./example")
///     .register_function(TestFunction::new())
///     .build()
///     .await?;
/// ```
#[macro_export]
macro_rules! function {
    ($struct_name:ident::$function_name:ident($($arg_ty:expr),*) -> $return_ty:expr => $closure:expr) => {
        #[derive(Debug, Serialize, Deserialize)]
        pub(crate) struct $struct_name {
            summary: FunctionSummary
        }

        impl $struct_name {
            pub(crate) fn new() -> Arc<Self> {
                let function_name = stringify!($function_name).to_lowercase();

                let mut arg_types = Vec::new();
                $({
                    arg_types.push($arg_ty);
                })*

                Arc::new(Self {
                    summary: FunctionSummary {
                        name: function_name,
                        arg_types
                    }
                })
            }
        }

        #[typetag::serde]
        impl ScalarFunctionImpl for $struct_name {
            fn eval(&self, args: &[ScalarExpression], tuple: &Tuple, schema: &[ColumnRef]) -> Result<DataValue, DatabaseError> {
                let mut _index = 0;

                $closure($({
                    let mut value = args[_index].eval(tuple, schema)?;
                    _index += 1;

                    if value.logical_type() != $arg_ty {
                        value = Arc::new(DataValue::clone(&value).cast(&$arg_ty)?);
                    }
                    value
                }, )*)
            }

            fn monotonicity(&self) -> Option<FuncMonotonicity> {
                todo!()
            }

            fn return_type(&self) -> &LogicalType {
                &$return_ty
            }

            fn summary(&self) -> &FunctionSummary {
                &self.summary
            }
        }
    };
}

#[cfg(test)]
mod test {
    use crate::catalog::ColumnRef;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::expression::function::{FuncMonotonicity, FunctionSummary, ScalarFunctionImpl};
    use crate::expression::BinaryOperator;
    use crate::expression::ScalarExpression;
    use crate::types::tuple::{SchemaRef, Tuple};
    use crate::types::value::{DataValue, ValueRef};
    use crate::types::LogicalType;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Arc;

    fn build_tuple() -> (Tuple, SchemaRef) {
        let schema_ref = Arc::new(vec![
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

        (Tuple { id: None, values }, schema_ref)
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
        let (tuple, schema_ref) = build_tuple();
        let my_struct = MyStruct::from((&schema_ref, tuple));

        println!("{:?}", my_struct);

        assert_eq!(my_struct.c1, 9);
        assert_eq!(my_struct.c2, "LOL");
    }

    function!(MyFunction::sum(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => (|v1: ValueRef, v2: ValueRef| {
        DataValue::binary_op(&v1, &v2, &BinaryOperator::Plus)
    }));

    #[test]
    fn test_function() -> Result<(), DatabaseError> {
        let function = MyFunction::new();
        let sum = function.eval(
            &[
                ScalarExpression::Constant(Arc::new(DataValue::Int8(Some(1)))),
                ScalarExpression::Constant(Arc::new(DataValue::Utf8(Some("1".to_string())))),
            ],
            &Tuple {
                id: None,
                values: vec![],
            },
            &vec![],
        )?;

        println!("{:?}", function);

        assert_eq!(
            function.summary,
            FunctionSummary {
                name: "sum".to_string(),
                arg_types: vec![LogicalType::Integer, LogicalType::Integer],
            }
        );
        assert_eq!(sum, DataValue::Int32(Some(2)));
        Ok(())
    }
}
