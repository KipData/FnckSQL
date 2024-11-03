fn main() {}

#[cfg(test)]
mod test {
    use fnck_sql::catalog::column::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation};
    use fnck_sql::errors::DatabaseError;
    use fnck_sql::expression::function::scala::ScalarFunctionImpl;
    use fnck_sql::expression::function::table::TableFunctionImpl;
    use fnck_sql::expression::function::FunctionSummary;
    use fnck_sql::expression::BinaryOperator;
    use fnck_sql::expression::ScalarExpression;
    use fnck_sql::types::evaluator::EvaluatorFactory;
    use fnck_sql::types::tuple::{SchemaRef, Tuple};
    use fnck_sql::types::value::ValueRef;
    use fnck_sql::types::value::{DataValue, Utf8Type};
    use fnck_sql::types::LogicalType;
    use fnck_sql::{implement_from_tuple, scala_function, table_function};
    use sqlparser::ast::CharLengthUnits;
    use std::sync::Arc;

    fn build_tuple() -> (Tuple, SchemaRef) {
        let schema_ref = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(None, CharLengthUnits::Characters),
                    false,
                    false,
                    None,
                )
                .unwrap(),
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
                name: "test_numbers".to_string(),
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

        let function_schema = function.output_schema();
        let table_name = Arc::new("test_numbers".to_string());
        let mut c1 = ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, false, false, None)?,
        );
        c1.summary_mut().relation = ColumnRelation::Table {
            column_id: function_schema[0].id().unwrap(),
            table_name: table_name.clone(),
            is_temp: false,
        };
        let mut c2 = ColumnCatalog::new(
            "c2".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, false, false, None)?,
        );
        c2.summary_mut().relation = ColumnRelation::Table {
            column_id: function_schema[1].id().unwrap(),
            table_name: table_name.clone(),
            is_temp: false,
        };

        assert_eq!(
            function_schema,
            &Arc::new(vec![ColumnRef::from(c1), ColumnRef::from(c2)])
        );

        Ok(())
    }
}
