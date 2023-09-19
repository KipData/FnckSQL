use std::cmp::Ordering;
use std::collections::Bound;
use std::mem;
use std::sync::Arc;
use crate::catalog::ColumnRef;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::expression::value_compute::{binary_op, unary_op};
use crate::types::{ColumnId, LogicalType};
use crate::types::errors::TypeError;
use crate::types::value::{DataValue, ValueRef};

#[derive(Debug, PartialEq)]
pub enum IndexBinary {
    Scope {
        min: Bound<ValueRef>,
        max: Bound<ValueRef>
    },
    Eq(ValueRef),
    NotEq(ValueRef),

    And(Vec<IndexBinary>),
    Or(Vec<IndexBinary>)
}


impl IndexBinary {
    fn is_null(&self) -> Result<bool, TypeError> {
        match self {
            IndexBinary::Scope { min, max } => Ok(matches!((min, max), (Bound::Unbounded, Bound::Unbounded))),
            IndexBinary::Eq(val) | IndexBinary::NotEq(val) => Ok(val.is_null()),
            _ => Err(TypeError::InvalidType),
        }
    }

    // FIXME: 聚合时将scope以外的等值条件已经非等值条件去除
    // Tips: Not `And` and `Or`
    pub fn scope_aggregation(binaries: Vec<IndexBinary>) -> Result<Vec<IndexBinary>, TypeError> {
        fn bound_compared(left_bound: &Bound<ValueRef>, right_bound: &Bound<ValueRef>, is_min: bool) -> Option<Ordering> {
            let op = |is_min, order: Ordering| {
                if is_min {
                    order
                } else {
                    order.reverse()
                }
            };

            match (left_bound, right_bound) {
                (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
                (Bound::Unbounded, _) => Some(op(is_min, Ordering::Less)),
                (_, Bound::Unbounded) => Some(op(is_min, Ordering::Greater)),
                (Bound::Included(left), Bound::Included(right)) => left.partial_cmp(right),
                (Bound::Included(left), Bound::Excluded(right)) => {
                    left.partial_cmp(right)
                        .map(|order| order.then(op(is_min, Ordering::Less)))
                },
                (Bound::Excluded(left), Bound::Excluded(right)) => left.partial_cmp(right),
                (Bound::Excluded(left), Bound::Included(right)) => {
                    left.partial_cmp(right)
                        .map(|order| order.then(op(is_min, Ordering::Greater)))
                },
            }
        }

        let mut new_binaries = Vec::new();
        let mut scope_min = Bound::Unbounded;
        let mut scope_max = Bound::Unbounded;

        for binary in binaries {
            if let IndexBinary::Scope { min, max } = binary {
                if let Some(order) = bound_compared(&scope_min, &min, true) {
                    if order.is_lt() {
                        scope_min = min;
                    }
                }

                if let Some(order) = bound_compared(&scope_max, &max, false) {
                    if order.is_gt() {
                        scope_max = max;
                    }
                }
            } else if !binary.is_null()? {
                new_binaries.push(binary);
            }
        }
        if !matches!((&scope_min, &scope_max), (Bound::Unbounded, Bound::Unbounded)) {
            new_binaries.push(IndexBinary::Scope {
                min: scope_min,
                max: scope_max,
            })
        }

        Ok(new_binaries)
    }
}

struct ReplaceBinary {
    column_expr: ScalarExpression,
    val_expr: ScalarExpression,
    op: BinaryOperator,
    ty: LogicalType,
    is_column_left: bool
}

impl ScalarExpression {
    fn unpack_val(&self) -> Option<ValueRef> {
        match self {
            ScalarExpression::Constant(val) => Some(val.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_val(),
            ScalarExpression::TypeCast { expr, ty, .. } => {
                expr.unpack_val()
                    .and_then(|val| DataValue::clone(&val)
                        .cast(ty).ok()
                        .map(Arc::new))
            }
            ScalarExpression::IsNull { expr } => {
                let is_null = expr.unpack_val().map(|val| val.is_null());

                Some(Arc::new(DataValue::Boolean(is_null)))
            },
            ScalarExpression::Unary { expr, op, .. } => {
                let val = expr.unpack_val()?;

                unary_op(&val, op).ok()
                    .map(Arc::new)

            }
            ScalarExpression::Binary { left_expr, right_expr, op, .. } => {
                let left = left_expr.unpack_val()?;
                let right = right_expr.unpack_val()?;

                binary_op(&left, &right, op).ok()
                    .map(Arc::new)
            }
            _ => None
        }
    }

    fn unpack_col(&self, is_binary_then_return: bool) -> Option<ColumnRef> {
        match self {
            ScalarExpression::ColumnRef(col) => Some(col.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_col(is_binary_then_return),
            ScalarExpression::Binary { left_expr, right_expr, .. } => {
                if is_binary_then_return {
                    return None;
                }

                match (left_expr.unpack_col(is_binary_then_return),
                       right_expr.unpack_col(is_binary_then_return))
                {
                    (Some(col), None) | (None, Some(col)) => Some(col),
                    _ => None
                }
            }
            _ => None
        }
    }

    // Tips: Indirect expressions like `ScalarExpression:：Alias` will be lost
    fn arithmetic_flip(&mut self, col_id: &ColumnId, fix_option: &mut Option<ReplaceBinary>) {
        match self {
            ScalarExpression::Binary { left_expr, right_expr, op, ty } => {
                let mut is_fixed = false;

                left_expr.arithmetic_flip(col_id, fix_option);
                is_fixed = fix_option.is_some();
                Self::fix_binary(fix_option, left_expr, right_expr, op);

                // `(c1 - 1) and (c1 + 2)` cannot fix!
                if fix_option.is_none() && !is_fixed {
                    right_expr.arithmetic_flip(col_id, fix_option);
                    Self::fix_binary(fix_option, right_expr, left_expr, op);
                }

                if matches!(op, BinaryOperator::Plus | BinaryOperator::Divide
                    | BinaryOperator::Minus | BinaryOperator::Multiply)
                {
                    match (left_expr.unpack_col(true), right_expr.unpack_col(true)) {
                        (Some(_), Some(_)) => (),
                        (Some(col), None) => {
                            if col_id != &col.id{
                                return;
                            }
                            fix_option.replace(ReplaceBinary{
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: right_expr.as_ref().clone(),
                                op: *op,
                                ty: *ty,
                                is_column_left: true,
                            });
                        }
                        (None, Some(col)) => {
                            if col_id != &col.id{
                                return;
                            }
                            fix_option.replace(ReplaceBinary{
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: left_expr.as_ref().clone(),
                                op: *op,
                                ty: *ty,
                                is_column_left: false,
                            });
                        }
                        _ => ()
                    }
                }
            }
            ScalarExpression::Alias { expr, .. } => expr.arithmetic_flip(col_id, fix_option),
            ScalarExpression::TypeCast { expr, .. } => expr.arithmetic_flip(col_id, fix_option),
            ScalarExpression::IsNull { expr, .. } => expr.arithmetic_flip(col_id, fix_option),
            ScalarExpression::Unary { expr, .. } => expr.arithmetic_flip(col_id, fix_option),
            _ => ()
        }
    }

    fn fix_binary(fix_option: &mut Option<ReplaceBinary>, left_expr: &mut Box<ScalarExpression>, right_expr: &mut Box<ScalarExpression>, op: &mut BinaryOperator) {
        if let Some(ReplaceBinary { column_expr, val_expr, op: fix_op, ty: fix_ty, is_column_left })
            = fix_option.take()
        {
            let op_flip = |op: BinaryOperator| {
                match op {
                    BinaryOperator::Plus => BinaryOperator::Minus,
                    BinaryOperator::Minus => BinaryOperator::Plus,
                    BinaryOperator::Multiply => BinaryOperator::Divide,
                    BinaryOperator::Divide => BinaryOperator::Multiply,
                    _ => unreachable!()
                }
            };
            let comparison_flip = |op: BinaryOperator| {
                match op {
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    source_op => source_op
                }
            };
            let (fixed_op, fixed_left_expr, fixed_right_expr) = if is_column_left {
                (op_flip(fix_op), right_expr.clone(), Box::new(val_expr))
            } else {
                if matches!(fix_op, BinaryOperator::Minus | BinaryOperator::Multiply) {
                    let _ = mem::replace(op, comparison_flip(*op));
                }
                (fix_op, Box::new(val_expr), right_expr.clone())
            };

            let _ = mem::replace(left_expr, Box::new(column_expr));
            let _ = mem::replace(right_expr, Box::new(ScalarExpression::Binary {
                op: fixed_op,
                left_expr: fixed_left_expr,
                right_expr: fixed_right_expr,
                ty: fix_ty,
            }));
        }
    }

    pub fn convert_binary(&self, col_id: &ColumnId) -> Result<Option<IndexBinary>, TypeError> {
        let mut expr = self.clone();

        expr.arithmetic_flip(col_id, &mut None);

        match &expr {
            ScalarExpression::Binary { left_expr, right_expr, op, .. } => {
                match (left_expr.convert_binary(col_id)?, right_expr.convert_binary(col_id)?) {
                    (Some(left_binary), Some(right_binary)) => {
                        match (left_binary, right_binary) {
                            (IndexBinary::And(mut left), IndexBinary::And(mut right))
                            | (IndexBinary::Or(mut left), IndexBinary::Or(mut right)) => {
                                left.append(&mut right);

                                Ok(Some(IndexBinary::And(left)))
                            }
                            (IndexBinary::And(mut left), IndexBinary::Or(mut right)) => {
                                right.append(&mut left);

                                Ok(Some(IndexBinary::Or(right)))
                            }
                            (IndexBinary::Or(mut left), IndexBinary::And(mut right)) => {
                                left.append(&mut right);

                                Ok(Some(IndexBinary::Or(left)))
                            }
                            (IndexBinary::And(mut binaries), binary)
                            | (binary, IndexBinary::And(mut binaries)) => {
                                binaries.push(binary);

                                Ok(Some(IndexBinary::And(binaries)))
                            }
                            (IndexBinary::Or(mut binaries), binary)
                            | (binary, IndexBinary::Or(mut binaries)) => {
                                binaries.push(binary);

                                Ok(Some(IndexBinary::Or(binaries)))
                            }
                            (left, right) => {
                                match op {
                                    BinaryOperator::And => {
                                        Ok(Some(IndexBinary::And(vec![left, right])))
                                    }
                                    BinaryOperator::Or => {
                                        Ok(Some(IndexBinary::Or(vec![left, right])))
                                    }
                                    BinaryOperator::Xor => todo!(),
                                    _ => Ok(None)
                                }
                            }
                        }
                    },
                    (None, None) => {
                        if let (Some(col), Some(val)) = (left_expr.unpack_col(false), right_expr.unpack_val()) {
                            if !col.id == *col_id {
                                return Ok(None);
                            }

                            return match op {
                                BinaryOperator::Gt => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Excluded(val.clone()),
                                        max: Bound::Unbounded
                                    }))
                                }
                                BinaryOperator::Lt => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Unbounded,
                                        max: Bound::Excluded(val.clone()),
                                    }))
                                }
                                BinaryOperator::GtEq => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Included(val.clone()),
                                        max: Bound::Unbounded
                                    }))
                                }
                                BinaryOperator::LtEq => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Unbounded,
                                        max: Bound::Included(val.clone()),
                                    }))
                                }
                                BinaryOperator::Eq | BinaryOperator::Spaceship => {
                                    Ok(Some(IndexBinary::Eq(val.clone())))
                                },
                                BinaryOperator::NotEq => {
                                    Ok(Some(IndexBinary::NotEq(val.clone())))
                                },
                                _ => Ok(None)
                            };
                        }
                        if let (Some(val), Some(col)) = (left_expr.unpack_val(), right_expr.unpack_col(false)) {
                            if !col.id == *col_id {
                                return Ok(None);
                            }

                            return match op {
                                BinaryOperator::Gt => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Unbounded,
                                        max: Bound::Excluded(val.clone())
                                    }))
                                }
                                BinaryOperator::Lt => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Excluded(val.clone()),
                                        max: Bound::Unbounded,
                                    }))
                                }
                                BinaryOperator::GtEq => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Unbounded,
                                        max: Bound::Included(val.clone())
                                    }))
                                }
                                BinaryOperator::LtEq => {
                                    Ok(Some(IndexBinary::Scope {
                                        min: Bound::Included(val.clone()),
                                        max: Bound::Unbounded,
                                    }))
                                }
                                BinaryOperator::Eq | BinaryOperator::Spaceship => {
                                    Ok(Some(IndexBinary::Eq(val.clone())))
                                },
                                BinaryOperator::NotEq => {
                                    Ok(Some(IndexBinary::NotEq(val.clone())))
                                },
                                _ => Ok(None)
                            };
                        }

                        return Ok(None);
                    }
                    (Some(binary), None) | (None, Some(binary)) => return Ok(Some(binary)),
                }
            },
            ScalarExpression::Alias { expr, .. } => expr.convert_binary(col_id),
            ScalarExpression::TypeCast { expr, .. } => expr.convert_binary(col_id),
            ScalarExpression::IsNull { expr } => expr.convert_binary(col_id),
            ScalarExpression::Unary { expr, .. } => expr.convert_binary(col_id),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::Bound;
    use std::sync::Arc;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::expression::index::IndexBinary;
    use crate::types::errors::TypeError;
    use crate::types::LogicalType;
    use crate::types::value::DataValue;

    fn build_test_expr() -> (ScalarExpression, ScalarExpression) {
        let col_1 = Arc::new(ColumnCatalog {
            id: 0,
            name: "c1".to_string(),
            table_name: None,
            nullable: false,
            desc: ColumnDesc {
                column_datatype: LogicalType::Integer,
                is_primary: false,
                is_unique: false,
            },
        });

        let c1_main_binary_expr = ScalarExpression::Binary {
            op: BinaryOperator::Minus,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(1))))),
            ty: LogicalType::Integer,
        };
        let val_main_binary_expr = ScalarExpression::Binary {
            op: BinaryOperator::Minus,
            left_expr: Box::new(ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(1))))),
            right_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            ty: LogicalType::Integer,
        };

        let comparison_expr_op = |expr| {
            ScalarExpression::Binary {
                op: BinaryOperator::GtEq,
                left_expr: Box::new(ScalarExpression::Alias {
                    expr: Box::from(expr),
                    alias: "alias".to_string(),
                }),
                right_expr: Box::new(ScalarExpression::Constant(Arc::new(DataValue::Int32(Some(2))))),
                ty: LogicalType::Boolean,
            }
        };
        // c1 - 1 >= 2
        let c1_main_expr = comparison_expr_op(c1_main_binary_expr);
        // 1 - c1 >= 2
        let val_main_expr = comparison_expr_op(val_main_binary_expr);

        (c1_main_expr, val_main_expr)
    }

    #[test]
    fn test_convert_binary_simple() -> Result<(), TypeError> {
        let col_1 = Arc::new(ColumnCatalog {
            id: 0,
            name: "c1".to_string(),
            table_name: None,
            nullable: false,
            desc: ColumnDesc {
                column_datatype: LogicalType::Integer,
                is_primary: false,
                is_unique: false,
            },
        });
        let val_1 = Arc::new(DataValue::Int32(Some(1)));

        let binary_eq = ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            right_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_eq, IndexBinary::Eq(val_1.clone()));

        let binary_not_eq = ScalarExpression::Binary {
            op: BinaryOperator::NotEq,
            left_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            right_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_not_eq, IndexBinary::NotEq(val_1.clone()));

        let binary_lt = ScalarExpression::Binary {
            op: BinaryOperator::Lt,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_lt, IndexBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(val_1.clone())
        });

        let binary_lteq = ScalarExpression::Binary {
            op: BinaryOperator::LtEq,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_lteq, IndexBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Included(val_1.clone())
        });

        let binary_gt = ScalarExpression::Binary {
            op: BinaryOperator::Gt,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_gt, IndexBinary::Scope {
            min: Bound::Excluded(val_1.clone()),
            max: Bound::Unbounded
        });

        let binary_gteq = ScalarExpression::Binary {
            op: BinaryOperator::GtEq,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }.convert_binary(&0)?.unwrap();

        assert_eq!(binary_gteq, IndexBinary::Scope {
            min: Bound::Included(val_1.clone()),
            max: Bound::Unbounded
        });

        Ok(())
    }

    #[test]
    fn test_arithmetic_flip() {
        let (mut c1_main_expr, mut val_main_expr) = build_test_expr();

        // c1 - 1 >= 2
        c1_main_expr.arithmetic_flip(&0, &mut None);
        println!("{:#?}", c1_main_expr);

        // 1 - c1 >= 2
        val_main_expr.arithmetic_flip(&0, &mut None);
        println!("{:#?}", val_main_expr);
    }

    #[test]
    fn test_convert_binary_nested() -> Result<(), TypeError> {
        let (mut c1_main_expr, mut val_main_expr) = build_test_expr();

        // c1 - 1 >= 2
        c1_main_expr.arithmetic_flip(&0, &mut None);
        println!("{:#?}", c1_main_expr.convert_binary(&0)?);

        // 1 - c1 >= 2
        val_main_expr.arithmetic_flip(&0, &mut None);
        println!("{:#?}", val_main_expr.convert_binary(&0)?);

        Ok(())
    }

    #[test]
    fn test_scope_aggregation() {
        let val_1 = Arc::new(DataValue::Int32(Some(0)));
        let val_2 = Arc::new(DataValue::Int32(Some(1)));
        let val_3 = Arc::new(DataValue::Int32(Some(2)));
        let val_4 = Arc::new(DataValue::Int32(Some(3)));

        let binaries = vec![
            IndexBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Included(val_4.clone())
            },
            IndexBinary::Scope {
                min: Bound::Included(val_2.clone()),
                max: Bound::Excluded(val_3.clone())
            },
            IndexBinary::Scope {
                min: Bound::Excluded(val_2.clone()),
                max: Bound::Included(val_3.clone())
            },
            IndexBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Excluded(val_4.clone())
            },
            IndexBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            },
            IndexBinary::Eq(val_1.clone()),
        ];

        assert_eq!(
            IndexBinary::scope_aggregation(binaries).unwrap(),
            vec![
                IndexBinary::Eq(val_1.clone()),
                IndexBinary::Scope {
                    min: Bound::Excluded(val_2.clone()),
                    max: Bound::Excluded(val_3.clone()),
                }
            ]
        )
    }
}