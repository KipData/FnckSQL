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
/// scala_function!(MyFunction::sum(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: ValueRef, v2: ValueRef| {
///     DataValue::binary_op(&v1, &v2, &BinaryOperator::Plus)
/// });
///
/// let fnck_sql = DataBaseBuilder::path("./example")
///     .register_scala_function(TestFunction::new())
///     .build()
///     ?;
/// ```
#[macro_export]
macro_rules! scala_function {
    ($struct_name:ident::$function_name:ident($($arg_ty:expr),*) -> $return_ty:expr => $closure:expr) => {
        #[derive(Debug, Serialize, Deserialize)]
        pub(crate) struct $struct_name {
            summary: FunctionSummary
        }

        impl $struct_name {
            #[allow(unused_mut)]
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
            #[allow(unused_variables, clippy::redundant_closure_call)]
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

/// # Examples
///
/// ```
/// table_function!(MyTableFunction::test_numbers(LogicalType::Integer) -> [c1: LogicalType::Integer, c2: LogicalType::Integer] => (|v1: ValueRef| {
///     let num = v1.i32().unwrap();
///
///     Ok(Box::new((0..num)
///         .into_iter()
///         .map(|i| Ok(Tuple {
///             id: None,
///             values: vec![
///                 Arc::new(DataValue::Int32(Some(i))),
///                 Arc::new(DataValue::Int32(Some(i))),
///             ]
///         }))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
///     }));
///
///     let fnck_sql = DataBaseBuilder::path("./example")
///         .register_table_function(MyTableFunction::new())
///         .build()
///     ?;
/// ```
#[macro_export]
macro_rules! table_function {
    ($struct_name:ident::$function_name:ident($($arg_ty:expr),*) -> [$($output_name:ident: $output_ty:expr),*] => $closure:expr) => {
        lazy_static! {
            static ref $function_name: TableCatalog = {
                let mut columns = Vec::new();

                $({
                    columns.push(ColumnCatalog::new(stringify!($output_name).to_lowercase(), true, ColumnDesc::new($output_ty, false, false, None)));
                })*
                TableCatalog::new(Arc::new(stringify!($function_name).to_lowercase()), columns).unwrap()
            };
        }

        #[derive(Debug, Serialize, Deserialize)]
        pub(crate) struct $struct_name {
            summary: FunctionSummary
        }

        impl $struct_name {
            #[allow(unused_mut)]
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
        impl TableFunctionImpl for $struct_name {
            #[allow(unused_variables, clippy::redundant_closure_call)]
            fn eval(&self, args: &[ScalarExpression]) -> Result<Box<dyn Iterator<Item=Result<Tuple, DatabaseError>>>, DatabaseError> {
                let mut _index = 0;
                let tuple = Tuple {
                    id: None,
                    values: Vec::new(),
                };

                $closure($({
                    let mut value = args[_index].eval(&tuple, &[])?;
                    _index += 1;

                    if value.logical_type() != $arg_ty {
                        value = Arc::new(DataValue::clone(&value).cast(&$arg_ty)?);
                    }
                    value
                }, )*)
            }

            fn output_schema(&self) -> &SchemaRef {
                $function_name.schema_ref()
            }

            fn summary(&self) -> &FunctionSummary {
                &self.summary
            }

            fn table(&self) -> &'static TableCatalog {
                &$function_name
            }
        }
    };
}

#[cfg(test)]
mod test {
    use crate::catalog::ColumnRef;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::expression::function::scala::{FuncMonotonicity, ScalarFunctionImpl};
    use crate::expression::function::FunctionSummary;
    use crate::expression::BinaryOperator;
    use crate::expression::ScalarExpression;
    use crate::types::evaluator::EvaluatorFactory;
    use crate::types::tuple::{SchemaRef, Tuple};
    use crate::types::value::{DataValue, Utf8Type, ValueRef};
    use crate::types::LogicalType;
    use crate::catalog::TableCatalog;
    use crate::expression::function::table::TableFunctionImpl;
    use lazy_static::lazy_static;
    use serde::Deserialize;
    use serde::Serialize;
    use sqlparser::ast::CharLengthUnits;
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
                ColumnDesc::new(
                    LogicalType::Varchar(None, CharLengthUnits::Characters),
                    false,
                    false,
                    None,
                ),
            )),
        ]);
        let values = vec![
            Arc::new(DataValue::Int32(Some(9))),
            Arc::new(DataValue::Utf8 {
                value: Some("LOL".to_string()),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }),
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
                if let DataValue::Utf8 { value: Some(val), .. } = value {
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

    scala_function!(MyScalaFunction::sum(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => (|v1: ValueRef, v2: ValueRef| {
        let plus_evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?;

        Ok(plus_evaluator.0.binary_eval(&v1, &v2))
    }));

    table_function!(MyTableFunction::test_numbers(LogicalType::Integer) -> [c1: LogicalType::Integer, c2: LogicalType::Integer] => (|v1: ValueRef| {
        let num = v1.i32().unwrap();

        Ok(Box::new((0..num)
            .into_iter()
            .map(|i| Ok(Tuple {
                id: None,
                values: vec![
                    Arc::new(DataValue::Int32(Some(i))),
                    Arc::new(DataValue::Int32(Some(i))),
                ]
            }))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
    }));

    #[test]
    fn test_scala_function() -> Result<(), DatabaseError> {
        let function = MyScalaFunction::new();
        let sum = function.eval(
            &[
                ScalarExpression::Constant(Arc::new(DataValue::Int8(Some(1)))),
                ScalarExpression::Constant(Arc::new(DataValue::Utf8 {
                    value: Some("1".to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                })),
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

    #[test]
    fn test_table_function() -> Result<(), DatabaseError> {
        let function = MyTableFunction::new();
        let mut numbers = function.eval(&[ScalarExpression::Constant(Arc::new(
            DataValue::Int8(Some(2)),
        ))])?;

        println!("{:?}", function);

        assert_eq!(
            function.summary,
            FunctionSummary {
                name: "numbers".to_string(),
                arg_types: vec![LogicalType::Integer],
            }
        );
        assert_eq!(
            numbers.next().unwrap().unwrap(),
            Tuple {
                id: None,
                values: vec![
                    Arc::new(DataValue::Int32(Some(0))),
                    Arc::new(DataValue::Int32(Some(0))),
                ]
            }
        );
        assert_eq!(
            numbers.next().unwrap().unwrap(),
            Tuple {
                id: None,
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(1))),
                ]
            }
        );
        assert!(numbers.next().is_none());

        assert_eq!(
            function.output_schema(),
            &Arc::new(vec![
                Arc::new(ColumnCatalog::new(
                    "c1".to_string(),
                    true,
                    ColumnDesc::new(LogicalType::Integer, false, false, None)
                )),
                Arc::new(ColumnCatalog::new(
                    "c2".to_string(),
                    true,
                    ColumnDesc::new(LogicalType::Integer, false, false, None)
                ))
            ])
        );

        Ok(())
    }
}
