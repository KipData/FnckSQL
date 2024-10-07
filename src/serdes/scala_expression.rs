use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::agg::AggKind;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
use crate::serdes::{ReferenceSerialization, ReferenceTables, Serialization};
use crate::storage::{TableCache, Transaction};
use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use sqlparser::ast::TrimWhereField;
use std::io::{Read, Write};
use std::sync::Arc;

impl ReferenceSerialization for ScalarExpression {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            ScalarExpression::Constant(value) => {
                writer.write_all(&[0u8])?;

                let logical_type = value.logical_type();

                logical_type.encode(writer)?;
                value.is_null().encode(writer)?;

                if value.is_null() {
                    return Ok(());
                }
                if logical_type.raw_len().is_none() {
                    let mut bytes = Vec::new();
                    (value.to_raw(&mut bytes)? as u32).encode(writer)?;
                    writer.write_all(&bytes)?;
                } else {
                    let _ = value.to_raw(writer)?;
                }
            }
            ScalarExpression::ColumnRef(column) => {
                writer.write_all(&[1u8])?;

                column.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::Alias { expr, alias } => {
                writer.write_all(&[2u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                alias.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::TypeCast { expr, ty } => {
                writer.write_all(&[3u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
            }
            ScalarExpression::IsNull { expr, negated } => {
                writer.write_all(&[4u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                negated.encode(writer)?;
            }
            ScalarExpression::Unary {
                op,
                expr,
                ty,
                evaluator,
            } => {
                writer.write_all(&[5u8])?;

                op.encode(writer)?;
                expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
                evaluator.encode(writer)?;
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ty,
                evaluator,
            } => {
                writer.write_all(&[6u8])?;

                op.encode(writer)?;
                left_expr.encode(writer, is_direct, reference_tables)?;
                right_expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
                evaluator.encode(writer)?;
            }
            ScalarExpression::AggCall {
                distinct,
                kind,
                args,
                ty,
            } => {
                writer.write_all(&[7u8])?;

                distinct.encode(writer)?;
                kind.encode(writer)?;
                (args.len() as u32).encode(writer)?;
                for arg in args.iter() {
                    arg.encode(writer, is_direct, reference_tables)?
                }
                ty.encode(writer)?;
            }
            ScalarExpression::In {
                negated,
                expr,
                args,
            } => {
                writer.write_all(&[8u8])?;

                negated.encode(writer)?;
                expr.encode(writer, is_direct, reference_tables)?;
                (args.len() as u32).encode(writer)?;
                for arg in args.iter() {
                    arg.encode(writer, is_direct, reference_tables)?
                }
            }
            ScalarExpression::Between {
                negated,
                expr,
                left_expr,
                right_expr,
            } => {
                writer.write_all(&[9u8])?;

                negated.encode(writer)?;
                expr.encode(writer, is_direct, reference_tables)?;
                left_expr.encode(writer, is_direct, reference_tables)?;
                right_expr.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                writer.write_all(&[10u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                for_expr.encode(writer, is_direct, reference_tables)?;
                from_expr.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::Position { expr, in_expr } => {
                writer.write_all(&[11u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                in_expr.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                writer.write_all(&[12u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                trim_what_expr.encode(writer, is_direct, reference_tables)?;
                trim_where.encode(writer)?;
            }
            ScalarExpression::Empty => writer.write_all(&[13u8])?,
            ScalarExpression::Reference { expr, pos } => {
                writer.write_all(&[14u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
                (*pos as u32).encode(writer)?;
            }
            ScalarExpression::Tuple(exprs) => {
                writer.write_all(&[15u8])?;

                (exprs.len() as u32).encode(writer)?;
                for expr in exprs.iter() {
                    expr.encode(writer, is_direct, reference_tables)?
                }
            }
            ScalarExpression::ScalaFunction(function) => {
                writer.write_all(&[16u8])?;

                function.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::TableFunction(function) => {
                writer.write_all(&[17u8])?;

                function.encode(writer, is_direct, reference_tables)?;
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ty,
            } => {
                writer.write_all(&[18u8])?;

                condition.encode(writer, is_direct, reference_tables)?;
                left_expr.encode(writer, is_direct, reference_tables)?;
                right_expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ty,
            } => {
                writer.write_all(&[19u8])?;

                left_expr.encode(writer, is_direct, reference_tables)?;
                right_expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ty,
            } => {
                writer.write_all(&[20u8])?;

                left_expr.encode(writer, is_direct, reference_tables)?;
                right_expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
            }
            ScalarExpression::Coalesce { exprs, ty } => {
                writer.write_all(&[21u8])?;

                (exprs.len() as u32).encode(writer)?;
                for expr in exprs.iter() {
                    expr.encode(writer, is_direct, reference_tables)?
                }
                ty.encode(writer)?;
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ty,
            } => {
                writer.write_all(&[22u8])?;

                operand_expr.encode(writer, is_direct, reference_tables)?;
                (expr_pairs.len() as u32).encode(writer)?;
                for (left_expr, right_expr) in expr_pairs.iter() {
                    left_expr.encode(writer, is_direct, reference_tables)?;
                    right_expr.encode(writer, is_direct, reference_tables)?;
                }
                else_expr.encode(writer, is_direct, reference_tables)?;
                ty.encode(writer)?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => {
                let logical_type = LogicalType::decode(reader)?;
                let is_null = bool::decode(reader)?;

                let value = if is_null {
                    DataValue::none(&logical_type)
                } else {
                    let value_len = match logical_type.raw_len() {
                        None => u32::decode(reader)? as usize,
                        Some(len) => len,
                    };
                    let mut buf = vec![0u8; value_len];
                    reader.read_exact(&mut buf)?;

                    DataValue::from_raw(&buf, &logical_type)
                };

                ScalarExpression::Constant(Arc::new(value))
            }
            1 => {
                let column_ref = ColumnRef::decode(reader, drive, reference_tables)?;

                ScalarExpression::ColumnRef(column_ref)
            }
            2 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let alias = AliasType::decode(reader, drive, reference_tables)?;

                ScalarExpression::Alias { expr, alias }
            }
            3 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::TypeCast { expr, ty }
            }
            4 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let negated = bool::decode(reader)?;

                ScalarExpression::IsNull { negated, expr }
            }
            5 => {
                let op = UnaryOperator::decode(reader)?;
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;
                let evaluator = Option::<UnaryEvaluatorBox>::decode(reader)?;

                ScalarExpression::Unary {
                    op,
                    expr,
                    evaluator,
                    ty,
                }
            }
            6 => {
                let op = BinaryOperator::decode(reader)?;
                let left_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let right_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;
                let evaluator = Option::<BinaryEvaluatorBox>::decode(reader)?;

                ScalarExpression::Binary {
                    op,
                    left_expr,
                    right_expr,
                    evaluator,
                    ty,
                }
            }
            7 => {
                let distinct = bool::decode(reader)?;
                let kind = AggKind::decode(reader)?;
                let args_len = u32::decode(reader)? as usize;

                let mut args = Vec::with_capacity(args_len);
                for _ in 0..args_len {
                    args.push(ScalarExpression::decode(reader, drive, reference_tables)?);
                }
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::AggCall {
                    distinct,
                    kind,
                    args,
                    ty,
                }
            }
            8 => {
                let negated = bool::decode(reader)?;
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let args_len = u32::decode(reader)? as usize;

                let mut args = Vec::with_capacity(args_len);
                for _ in 0..args_len {
                    args.push(ScalarExpression::decode(reader, drive, reference_tables)?);
                }

                ScalarExpression::In {
                    negated,
                    expr,
                    args,
                }
            }
            9 => {
                let negated = bool::decode(reader)?;
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let left_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let right_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;

                ScalarExpression::Between {
                    negated,
                    expr,
                    left_expr,
                    right_expr,
                }
            }
            10 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let for_expr =
                    Option::<Box<ScalarExpression>>::decode(reader, drive, reference_tables)?;
                let from_expr =
                    Option::<Box<ScalarExpression>>::decode(reader, drive, reference_tables)?;

                ScalarExpression::SubString {
                    expr,
                    for_expr,
                    from_expr,
                }
            }
            11 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let in_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;

                ScalarExpression::Position { expr, in_expr }
            }
            12 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let trim_what_expr =
                    Option::<Box<ScalarExpression>>::decode(reader, drive, reference_tables)?;
                let trim_where = Option::<TrimWhereField>::decode(reader)?;

                ScalarExpression::Trim {
                    expr,
                    trim_what_expr,
                    trim_where,
                }
            }
            13 => ScalarExpression::Empty,
            14 => {
                let expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let pos = u32::decode(reader)? as usize;

                ScalarExpression::Reference { expr, pos }
            }
            15 => {
                let exprs_len = u32::decode(reader)? as usize;

                let mut exprs = Vec::with_capacity(exprs_len);
                for _ in 0..exprs_len {
                    exprs.push(ScalarExpression::decode(reader, drive, reference_tables)?);
                }

                ScalarExpression::Tuple(exprs)
            }
            16 => {
                let function = ScalarFunction::decode(reader, drive, reference_tables)?;

                ScalarExpression::ScalaFunction(function)
            }
            17 => {
                let function = TableFunction::decode(reader, drive, reference_tables)?;

                ScalarExpression::TableFunction(function)
            }
            18 => {
                let condition = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let left_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let right_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::If {
                    condition,
                    left_expr,
                    right_expr,
                    ty,
                }
            }
            19 => {
                let left_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let right_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::IfNull {
                    left_expr,
                    right_expr,
                    ty,
                }
            }
            20 => {
                let left_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let right_expr = Box::<ScalarExpression>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::NullIf {
                    left_expr,
                    right_expr,
                    ty,
                }
            }
            21 => {
                let exprs_len = u32::decode(reader)? as usize;

                let mut exprs = Vec::with_capacity(exprs_len);
                for _ in 0..exprs_len {
                    exprs.push(ScalarExpression::decode(reader, drive, reference_tables)?);
                }
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::Coalesce { exprs, ty }
            }
            22 => {
                let operand_expr =
                    Option::<Box<ScalarExpression>>::decode(reader, drive, reference_tables)?;

                let pairs_len = u32::decode(reader)? as usize;

                let mut expr_pairs = Vec::with_capacity(pairs_len);
                for _ in 0..pairs_len {
                    let left_expr = ScalarExpression::decode(reader, drive, reference_tables)?;
                    let right_expr = ScalarExpression::decode(reader, drive, reference_tables)?;

                    expr_pairs.push((left_expr, right_expr));
                }
                let else_expr =
                    Option::<Box<ScalarExpression>>::decode(reader, drive, reference_tables)?;
                let ty = LogicalType::decode(reader)?;

                ScalarExpression::CaseWhen {
                    operand_expr,
                    expr_pairs,
                    else_expr,
                    ty,
                }
            }
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRelation, ColumnSummary};
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::agg::AggKind;
    use crate::expression::function::scala::ScalarFunction;
    use crate::expression::function::table::TableFunction;
    use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::function::current_date::CurrentDate;
    use crate::function::numbers::Numbers;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::{Storage, TableCache};
    use crate::types::evaluator::boolean::BooleanNotUnaryEvaluator;
    use crate::types::evaluator::int32::Int32PlusBinaryEvaluator;
    use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::LogicalType;
    use crate::utils::lru::ShardingLruCache;
    use sqlparser::ast::{CharLengthUnits, TrimWhereField};
    use std::hash::RandomState;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            expr: ScalarExpression,
            drive: Option<(&RocksTransaction, &TableCache)>,
            reference_tables: &mut ReferenceTables,
        ) -> Result<(), DatabaseError> {
            expr.encode(cursor, false, reference_tables)?;

            cursor.seek(SeekFrom::Start(0))?;
            assert_eq!(
                ScalarExpression::decode(cursor, drive, reference_tables)?,
                expr
            );
            cursor.seek(SeekFrom::Start(0))?;

            Ok(())
        }

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(ShardingLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(Arc::new(DataValue::Int32(None))),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(42)))),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(Arc::new(DataValue::Utf8 {
                value: Some("hello".to_string()),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            })),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ColumnRef(Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: 2,
                        table_name: Arc::new("t1".to_string()),
                    },
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Integer, false, false, None).unwrap(),
            })),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ColumnRef(Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    name: "c4".to_string(),
                    relation: ColumnRelation::None,
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Boolean, false, false, None).unwrap(),
            })),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Name("Hello".to_string()),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Expr(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TypeCast {
                expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IsNull {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(ScalarExpression::Empty),
                evaluator: Some(UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator))),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(ScalarExpression::Empty),
                evaluator: None,
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                evaluator: Some(BinaryEvaluatorBox(Arc::new(Int32PlusBinaryEvaluator))),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                evaluator: None,
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::AggCall {
                distinct: true,
                kind: AggKind::Avg,
                args: vec![ScalarExpression::Empty],
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::In {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
                args: vec![ScalarExpression::Empty],
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Between {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: Some(Box::new(ScalarExpression::Empty)),
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: None,
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: None,
                from_expr: None,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Position {
                expr: Box::new(ScalarExpression::Empty),
                in_expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: Some(Box::new(ScalarExpression::Empty)),
                trim_where: Some(TrimWhereField::Both),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: None,
                trim_where: Some(TrimWhereField::Both),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: None,
                trim_where: None,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Empty,
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Tuple(vec![ScalarExpression::Empty]),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![ScalarExpression::Empty],
                inner: CurrentDate::new(),
            }),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TableFunction(TableFunction {
                args: vec![ScalarExpression::Empty],
                inner: Numbers::new(),
            }),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::If {
                condition: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IfNull {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::NullIf {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Coalesce {
                exprs: vec![ScalarExpression::Empty],
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: Some(Box::new(ScalarExpression::Empty)),
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: None,
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;

        Ok(())
    }
}
