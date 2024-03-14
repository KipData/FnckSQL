use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::ScalarFunction;
use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
use crate::types::value::{DataValue, ValueRef};
use crate::types::{ColumnId, LogicalType};
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
enum Replace {
    Binary(ReplaceBinary),
    Unary(ReplaceUnary),
}

#[derive(Debug)]
struct ReplaceBinary {
    column_expr: ScalarExpression,
    val_expr: ScalarExpression,
    op: BinaryOperator,
    ty: LogicalType,
    is_column_left: bool,
}

#[derive(Debug)]
struct ReplaceUnary {
    child_expr: ScalarExpression,
    op: UnaryOperator,
    ty: LogicalType,
}

impl ScalarExpression {
    pub fn exist_column(&self, table_name: &str, col_id: &ColumnId) -> bool {
        match self {
            ScalarExpression::ColumnRef(col) => {
                Self::_is_belong(table_name, col) && col.id() == Some(*col_id)
            }
            ScalarExpression::Alias { expr, .. } => expr.exist_column(table_name, col_id),
            ScalarExpression::TypeCast { expr, .. } => expr.exist_column(table_name, col_id),
            ScalarExpression::IsNull { expr, .. } => expr.exist_column(table_name, col_id),
            ScalarExpression::Unary { expr, .. } => expr.exist_column(table_name, col_id),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.exist_column(table_name, col_id)
                    || right_expr.exist_column(table_name, col_id)
            }
            ScalarExpression::AggCall { args, .. }
            | ScalarExpression::Tuple(args)
            | ScalarExpression::Function(ScalarFunction { args, .. })
            | ScalarExpression::Coalesce { exprs: args, .. } => args
                .iter()
                .any(|expr| expr.exist_column(table_name, col_id)),
            ScalarExpression::In { expr, args, .. } => {
                expr.exist_column(table_name, col_id)
                    || args
                        .iter()
                        .any(|expr| expr.exist_column(table_name, col_id))
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                expr.exist_column(table_name, col_id)
                    || left_expr.exist_column(table_name, col_id)
                    || right_expr.exist_column(table_name, col_id)
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                expr.exist_column(table_name, col_id)
                    || for_expr
                        .as_ref()
                        .map(|expr| expr.exist_column(table_name, col_id))
                        == Some(true)
                    || from_expr
                        .as_ref()
                        .map(|expr| expr.exist_column(table_name, col_id))
                        == Some(true)
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.exist_column(table_name, col_id) || in_expr.exist_column(table_name, col_id)
            }
            ScalarExpression::Constant(_) => false,
            ScalarExpression::Reference { .. } | ScalarExpression::Empty => unreachable!(),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.exist_column(table_name, col_id)
                    || left_expr.exist_column(table_name, col_id)
                    || right_expr.exist_column(table_name, col_id)
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.exist_column(table_name, col_id)
                    || right_expr.exist_column(table_name, col_id)
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                matches!(
                    operand_expr
                        .as_ref()
                        .map(|expr| expr.exist_column(table_name, col_id)),
                    Some(true)
                ) || expr_pairs.iter().any(|(expr_1, expr_2)| {
                    expr_1.exist_column(table_name, col_id)
                        || expr_2.exist_column(table_name, col_id)
                }) || matches!(
                    else_expr
                        .as_ref()
                        .map(|expr| expr.exist_column(table_name, col_id)),
                    Some(true)
                )
            }
        }
    }

    pub(crate) fn unpack_val(&self) -> Option<ValueRef> {
        match self {
            ScalarExpression::Constant(val) => Some(val.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_val(),
            ScalarExpression::TypeCast { expr, ty, .. } => expr
                .unpack_val()
                .and_then(|val| DataValue::clone(&val).cast(ty).ok().map(Arc::new)),
            ScalarExpression::IsNull { expr, .. } => {
                let is_null = expr.unpack_val().map(|val| val.is_null());

                Some(Arc::new(DataValue::Boolean(is_null)))
            }
            ScalarExpression::Unary { expr, op, .. } => {
                let val = expr.unpack_val()?;

                DataValue::unary_op(&val, op).ok().map(Arc::new)
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => {
                let left = left_expr.unpack_val()?;
                let right = right_expr.unpack_val()?;

                DataValue::binary_op(&left, &right, op).ok().map(Arc::new)
            }
            _ => None,
        }
    }

    pub(crate) fn unpack_col(&self, is_deep: bool) -> Option<ColumnRef> {
        match self {
            ScalarExpression::ColumnRef(col) => Some(col.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_col(is_deep),
            ScalarExpression::Unary { expr, .. } => expr.unpack_col(is_deep),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                if !is_deep {
                    return None;
                }

                left_expr
                    .unpack_col(true)
                    .or_else(|| right_expr.unpack_col(true))
            }
            _ => None,
        }
    }

    pub fn simplify(&mut self) -> Result<(), DatabaseError> {
        self._simplify(&mut Vec::new())
    }

    pub fn constant_calculation(&mut self) -> Result<(), DatabaseError> {
        match self {
            ScalarExpression::Unary { expr, op, .. } => {
                expr.constant_calculation()?;

                if let ScalarExpression::Constant(unary_val) = expr.as_ref() {
                    let value = DataValue::unary_op(unary_val, op)?;
                    let _ = mem::replace(self, ScalarExpression::Constant(Arc::new(value)));
                }
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => {
                left_expr.constant_calculation()?;
                right_expr.constant_calculation()?;

                if let (
                    ScalarExpression::Constant(left_val),
                    ScalarExpression::Constant(right_val),
                ) = (left_expr.as_ref(), right_expr.as_ref())
                {
                    let value = DataValue::binary_op(left_val, right_val, op)?;
                    let _ = mem::replace(self, ScalarExpression::Constant(Arc::new(value)));
                }
            }
            ScalarExpression::Alias { expr, .. } => expr.constant_calculation()?,
            ScalarExpression::TypeCast { expr, .. } => expr.constant_calculation()?,
            ScalarExpression::IsNull { expr, .. } => expr.constant_calculation()?,
            ScalarExpression::AggCall { args, .. } => {
                for expr in args {
                    expr.constant_calculation()?;
                }
            }
            ScalarExpression::In { expr, args, .. } => {
                expr.constant_calculation()?;
                for arg in args {
                    arg.constant_calculation()?;
                }
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                expr.constant_calculation()?;
                left_expr.constant_calculation()?;
                right_expr.constant_calculation()?;
            }
            ScalarExpression::SubString {
                expr,
                from_expr,
                for_expr,
            } => {
                expr.constant_calculation()?;
                if let Some(from_expr) = from_expr {
                    from_expr.constant_calculation()?;
                }
                if let Some(for_expr) = for_expr {
                    for_expr.constant_calculation()?;
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.constant_calculation()?;
                in_expr.constant_calculation()?;
            }
            ScalarExpression::Tuple(exprs) | ScalarExpression::Coalesce { exprs, .. } => {
                for expr in exprs {
                    expr.constant_calculation()?;
                }
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.constant_calculation()?;
                left_expr.constant_calculation()?;
                right_expr.constant_calculation()?;
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.constant_calculation()?;
                right_expr.constant_calculation()?;
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                if let Some(operand_expr) = operand_expr {
                    operand_expr.constant_calculation()?;
                }
                for (left_expr, right_expr) in expr_pairs {
                    left_expr.constant_calculation()?;
                    right_expr.constant_calculation()?;
                }
                if let Some(else_expr) = else_expr {
                    else_expr.constant_calculation()?;
                }
            }
            ScalarExpression::Constant(_)
            | ScalarExpression::ColumnRef(_)
            | ScalarExpression::Empty
            | ScalarExpression::Reference { .. }
            | ScalarExpression::Function(_) => (),
        }

        Ok(())
    }

    // Tips: Indirect expressions like `ScalarExpression:：Alias` will be lost
    fn _simplify(&mut self, replaces: &mut Vec<Replace>) -> Result<(), DatabaseError> {
        match self {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
            } => {
                Self::fix_expr(replaces, left_expr, right_expr, op)?;

                // `(c1 - 1) and (c1 + 2)` cannot fix!
                Self::fix_expr(replaces, right_expr, left_expr, op)?;

                if Self::is_arithmetic(op) {
                    match (left_expr.unpack_col(false), right_expr.unpack_col(false)) {
                        (Some(col), None) => {
                            replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: mem::replace(right_expr, ScalarExpression::Empty),
                                op: *op,
                                ty: *ty,
                                is_column_left: true,
                            }));
                        }
                        (None, Some(col)) => {
                            replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: mem::replace(left_expr, ScalarExpression::Empty),
                                op: *op,
                                ty: *ty,
                                is_column_left: false,
                            }));
                        }
                        (None, None) => {
                            if replaces.is_empty() {
                                return Ok(());
                            }

                            match (left_expr.unpack_col(true), right_expr.unpack_col(true)) {
                                (Some(col), None) => {
                                    replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: ScalarExpression::ColumnRef(col),
                                        val_expr: mem::replace(right_expr, ScalarExpression::Empty),
                                        op: *op,
                                        ty: *ty,
                                        is_column_left: true,
                                    }));
                                }
                                (None, Some(col)) => {
                                    replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: ScalarExpression::ColumnRef(col),
                                        val_expr: mem::replace(left_expr, ScalarExpression::Empty),
                                        op: *op,
                                        ty: *ty,
                                        is_column_left: false,
                                    }));
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
            }
            ScalarExpression::Alias { expr, .. } => expr._simplify(replaces)?,
            ScalarExpression::TypeCast { expr, .. } => {
                if let Some(val) = expr.unpack_val() {
                    let _ = mem::replace(self, ScalarExpression::Constant(val));
                }
            }
            ScalarExpression::IsNull { expr, .. } => {
                if let Some(val) = expr.unpack_val() {
                    let _ = mem::replace(
                        self,
                        ScalarExpression::Constant(Arc::new(DataValue::Boolean(Some(
                            val.is_null(),
                        )))),
                    );
                }
            }
            ScalarExpression::Unary { expr, op, ty } => {
                if let Some(val) = expr.unpack_val() {
                    let new_expr =
                        ScalarExpression::Constant(Arc::new(DataValue::unary_op(&val, op)?));
                    let _ = mem::replace(self, new_expr);
                } else {
                    replaces.push(Replace::Unary(ReplaceUnary {
                        child_expr: expr.as_ref().clone(),
                        op: *op,
                        ty: *ty,
                    }));
                }
            }
            ScalarExpression::In {
                expr,
                negated,
                args,
            } => {
                if args.is_empty() {
                    return Ok(());
                }

                let (op_1, op_2) = if *negated {
                    (BinaryOperator::NotEq, BinaryOperator::And)
                } else {
                    (BinaryOperator::Eq, BinaryOperator::Or)
                };
                let mut new_expr = ScalarExpression::Binary {
                    op: op_1,
                    left_expr: expr.clone(),
                    right_expr: Box::new(args.remove(0)),
                    ty: LogicalType::Boolean,
                };

                for arg in args.drain(..) {
                    new_expr = ScalarExpression::Binary {
                        op: op_2,
                        left_expr: Box::new(ScalarExpression::Binary {
                            op: op_1,
                            left_expr: expr.clone(),
                            right_expr: Box::new(arg),
                            ty: LogicalType::Boolean,
                        }),
                        right_expr: Box::new(new_expr),
                        ty: LogicalType::Boolean,
                    }
                }

                let _ = mem::replace(self, new_expr);
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                negated,
            } => {
                let (op, left_op, right_op) = if *negated {
                    (BinaryOperator::Or, BinaryOperator::Lt, BinaryOperator::Gt)
                } else {
                    (
                        BinaryOperator::And,
                        BinaryOperator::GtEq,
                        BinaryOperator::LtEq,
                    )
                };
                let new_expr = ScalarExpression::Binary {
                    op,
                    left_expr: Box::new(ScalarExpression::Binary {
                        op: left_op,
                        left_expr: expr.clone(),
                        right_expr: mem::replace(left_expr, Box::new(ScalarExpression::Empty)),
                        ty: LogicalType::Boolean,
                    }),
                    right_expr: Box::new(ScalarExpression::Binary {
                        op: right_op,
                        left_expr: mem::replace(expr, Box::new(ScalarExpression::Empty)),
                        right_expr: mem::replace(right_expr, Box::new(ScalarExpression::Empty)),
                        ty: LogicalType::Boolean,
                    }),
                    ty: LogicalType::Boolean,
                };

                let _ = mem::replace(self, new_expr);
            }
            // FIXME: Maybe `ScalarExpression::Tuple` can be replaced?
            _ => (),
        }

        Ok(())
    }

    fn is_arithmetic(op: &mut BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Plus
                | BinaryOperator::Divide
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
        )
    }

    fn fix_expr(
        replaces: &mut Vec<Replace>,
        left_expr: &mut Box<ScalarExpression>,
        right_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) -> Result<(), DatabaseError> {
        left_expr._simplify(replaces)?;

        if Self::is_arithmetic(op) {
            return Ok(());
        }
        while let Some(replace) = replaces.pop() {
            match replace {
                Replace::Binary(binary) => Self::fix_binary(binary, left_expr, right_expr, op),
                Replace::Unary(unary) => {
                    Self::fix_unary(unary, left_expr, right_expr, op);
                    Self::fix_expr(replaces, left_expr, right_expr, op)?;
                }
            }
        }

        Ok(())
    }

    fn fix_unary(
        replace_unary: ReplaceUnary,
        col_expr: &mut Box<ScalarExpression>,
        val_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) {
        let ReplaceUnary {
            child_expr,
            op: fix_op,
            ty: fix_ty,
        } = replace_unary;
        let _ = mem::replace(col_expr, Box::new(child_expr));

        let expr = mem::replace(val_expr, Box::new(ScalarExpression::Empty));
        let _ = mem::replace(
            val_expr,
            Box::new(ScalarExpression::Unary {
                op: fix_op,
                expr,
                ty: fix_ty,
            }),
        );
        let _ = mem::replace(
            op,
            match fix_op {
                UnaryOperator::Plus => *op,
                UnaryOperator::Minus => match *op {
                    BinaryOperator::Plus => BinaryOperator::Minus,
                    BinaryOperator::Minus => BinaryOperator::Plus,
                    BinaryOperator::Multiply => BinaryOperator::Divide,
                    BinaryOperator::Divide => BinaryOperator::Multiply,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    source_op => source_op,
                },
                UnaryOperator::Not => match *op {
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    source_op => source_op,
                },
            },
        );
    }

    fn fix_binary(
        replace_binary: ReplaceBinary,
        left_expr: &mut Box<ScalarExpression>,
        right_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) {
        let ReplaceBinary {
            column_expr,
            val_expr,
            op: fix_op,
            ty: fix_ty,
            is_column_left,
        } = replace_binary;
        let op_flip = |op: BinaryOperator| match op {
            BinaryOperator::Plus => BinaryOperator::Minus,
            BinaryOperator::Minus => BinaryOperator::Plus,
            BinaryOperator::Multiply => BinaryOperator::Divide,
            BinaryOperator::Divide => BinaryOperator::Multiply,
            _ => unreachable!(),
        };
        let comparison_flip = |op: BinaryOperator| match op {
            BinaryOperator::Gt => BinaryOperator::Lt,
            BinaryOperator::GtEq => BinaryOperator::LtEq,
            BinaryOperator::Lt => BinaryOperator::Gt,
            BinaryOperator::LtEq => BinaryOperator::GtEq,
            source_op => source_op,
        };
        let temp_expr = mem::replace(right_expr, Box::new(ScalarExpression::Empty));
        let (fixed_op, fixed_left_expr, fixed_right_expr) = if is_column_left {
            (op_flip(fix_op), temp_expr, Box::new(val_expr))
        } else {
            if matches!(fix_op, BinaryOperator::Minus | BinaryOperator::Multiply) {
                let _ = mem::replace(op, comparison_flip(*op));
            }
            (fix_op, Box::new(val_expr), temp_expr)
        };

        let _ = mem::replace(left_expr, Box::new(column_expr));
        let _ = mem::replace(
            right_expr,
            Box::new(ScalarExpression::Binary {
                op: fixed_op,
                left_expr: fixed_left_expr,
                right_expr: fixed_right_expr,
                ty: fix_ty,
            }),
        );
    }

    fn _is_belong(table_name: &str, col: &ColumnRef) -> bool {
        matches!(
            col.table_name().map(|name| table_name == name.as_str()),
            Some(true)
        )
    }
}
