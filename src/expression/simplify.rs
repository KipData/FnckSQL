use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::ScalarFunction;
use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
use crate::types::value::{DataValue, ValueRef, NULL_VALUE};
use crate::types::{ColumnId, LogicalType};
use ahash::RandomState;
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::{Bound, HashSet};
use std::fmt::Formatter;
use std::sync::Arc;
use std::{fmt, mem};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ConstantBinary {
    Scope {
        min: Bound<ValueRef>,
        max: Bound<ValueRef>,
    },
    Eq(ValueRef),
    NotEq(ValueRef),

    // ConstantBinary in And can only be Scope\Eq\NotEq
    And(Vec<ConstantBinary>),
    // ConstantBinary in Or can only be Scope\Eq\NotEq\And
    Or(Vec<ConstantBinary>),
}

impl ConstantBinary {
    #[allow(dead_code)]
    fn is_null(&self) -> Result<bool, DatabaseError> {
        match self {
            ConstantBinary::Scope { min, max } => {
                let op = |bound: &Bound<ValueRef>| {
                    if let Bound::Included(val) | Bound::Excluded(val) = bound {
                        val.is_null()
                    } else {
                        false
                    }
                };
                if op(min) || op(max) {
                    return Ok(true);
                }

                Ok(matches!((min, max), (Bound::Unbounded, Bound::Unbounded)))
            }
            ConstantBinary::Eq(val) | ConstantBinary::NotEq(val) => Ok(val.is_null()),
            _ => Err(DatabaseError::InvalidType),
        }
    }

    pub fn rearrange(self) -> Result<Vec<ConstantBinary>, DatabaseError> {
        match self {
            ConstantBinary::Or(binaries) => {
                if binaries.is_empty() {
                    return Ok(vec![]);
                }

                let mut condition_binaries = Vec::new();

                for binary in binaries {
                    match binary {
                        ConstantBinary::Or(_) => return Err(DatabaseError::InvalidType),
                        ConstantBinary::And(mut and_binaries) => {
                            condition_binaries.append(&mut and_binaries);
                        }
                        ConstantBinary::Scope {
                            min: Bound::Unbounded,
                            max: Bound::Unbounded,
                        } => (),
                        source => condition_binaries.push(source),
                    }
                }
                // Sort
                condition_binaries.sort_by(|a, b| {
                    let op = |binary: &ConstantBinary| match binary {
                        ConstantBinary::Scope { min, .. } => min.clone(),
                        ConstantBinary::Eq(val) => Bound::Included(val.clone()),
                        ConstantBinary::NotEq(val) => Bound::Excluded(val.clone()),
                        _ => unreachable!(),
                    };

                    Self::bound_compared(&op(a), &op(b), true).unwrap_or(Ordering::Equal)
                });

                let mut merged_binaries: Vec<ConstantBinary> = Vec::new();

                for condition in condition_binaries {
                    let op = |binary: &ConstantBinary| match binary {
                        ConstantBinary::Scope { min, max } => (min.clone(), max.clone()),
                        ConstantBinary::Eq(val) => (Bound::Unbounded, Bound::Included(val.clone())),
                        ConstantBinary::NotEq(val) => {
                            (Bound::Unbounded, Bound::Excluded(val.clone()))
                        }
                        _ => unreachable!(),
                    };
                    let mut is_push = merged_binaries.is_empty();

                    for binary in merged_binaries.iter_mut().rev() {
                        match binary {
                            ConstantBinary::Scope { max, .. } => {
                                let (condition_min, condition_max) = op(&condition);
                                let is_lt_min = Self::bound_compared(max, &condition_min, false)
                                    .unwrap_or(Ordering::Equal)
                                    .is_lt();
                                let is_lt_max = Self::bound_compared(max, &condition_max, false)
                                    .unwrap_or(Ordering::Equal)
                                    .is_lt();

                                if !is_lt_min && is_lt_max {
                                    let _ = mem::replace(max, condition_max);
                                } else if !matches!(condition, ConstantBinary::Scope { .. }) {
                                    is_push = is_lt_max;
                                } else if is_lt_min && is_lt_max {
                                    is_push = true
                                }

                                break;
                            }
                            ConstantBinary::Eq(_) => is_push = true,
                            _ => (),
                        }
                    }

                    if is_push {
                        merged_binaries.push(condition);
                    }
                }

                Ok(merged_binaries)
            }
            ConstantBinary::And(binaries) => Ok(binaries),
            source => Ok(vec![source]),
        }
    }

    pub fn scope_aggregation(&mut self) -> Result<(), DatabaseError> {
        match self {
            // `Or` is allowed to contain And, `Scope`, `Eq/NotEq`
            // Tips: Only single-level `And`
            ConstantBinary::Or(binaries) => {
                let mut or_binaries = Vec::new();
                for binary in binaries {
                    match binary {
                        ConstantBinary::And(and_binaries) => {
                            or_binaries.append(&mut Self::and_scope_aggregation(and_binaries)?);
                        }
                        ConstantBinary::Or(_) => {
                            unreachable!("`Or` does not allow nested `Or`")
                        }
                        cb => {
                            or_binaries.push(cb.clone());
                        }
                    }
                }
                let or_binaries = Self::or_scope_aggregation(&or_binaries);
                let _ = mem::replace(self, ConstantBinary::Or(or_binaries));
            }
            // `And` is allowed to contain Scope, `Eq/NotEq`
            ConstantBinary::And(binaries) => {
                let and_binaries = Self::and_scope_aggregation(binaries)?;
                let _ = mem::replace(self, ConstantBinary::And(and_binaries));
            }
            _ => (),
        }

        Ok(())
    }

    fn bound_compared(
        left_bound: &Bound<ValueRef>,
        right_bound: &Bound<ValueRef>,
        is_min: bool,
    ) -> Option<Ordering> {
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
            (Bound::Included(left), Bound::Excluded(right)) => left
                .partial_cmp(right)
                .map(|order| order.then(op(is_min, Ordering::Less))),
            (Bound::Excluded(left), Bound::Excluded(right)) => left.partial_cmp(right),
            (Bound::Excluded(left), Bound::Included(right)) => left
                .partial_cmp(right)
                .map(|order| order.then(op(is_min, Ordering::Greater))),
        }
    }

    // Tips: It only makes sense if the condition is and aggregation
    fn and_scope_aggregation(
        binaries: &[ConstantBinary],
    ) -> Result<Vec<ConstantBinary>, DatabaseError> {
        if binaries.is_empty() {
            return Ok(vec![]);
        }

        let mut scope_min = Bound::Unbounded;
        let mut scope_max = Bound::Unbounded;
        let mut eq_set = HashSet::with_hasher(RandomState::new());

        let sort_op = |binary: &&ConstantBinary| match binary {
            ConstantBinary::Scope { .. } => 3,
            ConstantBinary::NotEq(_) => 2,
            ConstantBinary::Eq(_) => 1,
            ConstantBinary::And(_) | ConstantBinary::Or(_) => 0,
        };

        // Aggregate various ranges to get the minimum range
        for binary in binaries.iter().sorted_by_key(sort_op) {
            match binary {
                ConstantBinary::Scope { min, max } => {
                    // Skip if eq or noteq exists
                    if !eq_set.is_empty() {
                        continue;
                    }

                    if let Some(order) = Self::bound_compared(&scope_min, min, true) {
                        if order.is_lt() {
                            scope_min = min.clone();
                        }
                    }

                    if let Some(order) = Self::bound_compared(&scope_max, max, false) {
                        if order.is_gt() {
                            scope_max = max.clone();
                        }
                    }
                }
                ConstantBinary::Eq(val) => {
                    let _ = eq_set.insert(val.clone());
                }
                ConstantBinary::NotEq(val) => {
                    let _ = eq_set.remove(val);
                }
                ConstantBinary::Or(_) | ConstantBinary::And(_) => {
                    return Err(DatabaseError::InvalidType)
                }
            }
        }

        let eq_option = eq_set
            .into_iter()
            .sorted_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .next()
            .map(ConstantBinary::Eq);

        if let Some(eq) = eq_option {
            Ok(vec![eq])
        } else if !matches!(
            (&scope_min, &scope_max),
            (Bound::Unbounded, Bound::Unbounded)
        ) {
            let scope_binary = ConstantBinary::Scope {
                min: scope_min,
                max: scope_max,
            };

            Ok(vec![scope_binary])
        } else {
            Ok(vec![])
        }
    }

    // Tips: It only makes sense if the condition is or aggregation
    fn or_scope_aggregation(binaries: &Vec<ConstantBinary>) -> Vec<ConstantBinary> {
        if binaries.is_empty() {
            return vec![];
        }
        let mut scopes = Vec::new();
        let mut eqs = Vec::new();

        let mut scope_margin = None;

        for binary in binaries {
            if matches!(scope_margin, Some((Bound::Unbounded, Bound::Unbounded))) {
                break;
            }
            match binary {
                ConstantBinary::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                } => {
                    scope_margin = Some((Bound::Unbounded, Bound::Unbounded));
                    break;
                }
                ConstantBinary::Scope { min, max } => {
                    if let Some((scope_min, scope_max)) = &mut scope_margin {
                        if matches!(
                            Self::bound_compared(scope_min, min, true).map(Ordering::is_gt),
                            Some(true)
                        ) {
                            let _ = mem::replace(scope_min, min.clone());
                        }
                        if matches!(
                            Self::bound_compared(scope_max, max, false).map(Ordering::is_lt),
                            Some(true)
                        ) {
                            let _ = mem::replace(scope_max, max.clone());
                        }
                    } else {
                        scope_margin = Some((min.clone(), max.clone()))
                    }

                    scopes.push((min, max))
                }
                ConstantBinary::Eq(val) => eqs.push(val),
                _ => (),
            }
        }
        if matches!(
            scope_margin,
            Some((Bound::Unbounded, Bound::Unbounded)) | None
        ) {
            return vec![];
        }

        let mut merge_scopes: Vec<(Bound<ValueRef>, Bound<ValueRef>)> = Vec::new();

        match scope_margin {
            Some((Bound::Unbounded, _)) => {
                if let Some((_, max)) = scopes.iter().max_by(|(_, max_a), (_, max_b)| {
                    Self::bound_compared(max_a, max_b, false).unwrap()
                }) {
                    merge_scopes.push((Bound::Unbounded, (**max).clone()))
                }
            }
            Some((_, Bound::Unbounded)) => {
                if let Some((min, _)) = scopes.iter().min_by(|(min_a, _), (min_b, _)| {
                    Self::bound_compared(min_a, min_b, true).unwrap()
                }) {
                    merge_scopes.push(((**min).clone(), Bound::Unbounded))
                }
            }
            _ => {
                scopes.sort_by(|(min_a, _), (min_b, _)| {
                    Self::bound_compared(min_a, min_b, true).unwrap()
                });

                for (min, max) in scopes {
                    if merge_scopes.is_empty() {
                        merge_scopes.push((min.clone(), max.clone()));
                        continue;
                    }

                    let last_pos = merge_scopes.len() - 1;
                    let last_scope: &mut _ = &mut merge_scopes[last_pos];
                    if Self::bound_compared(&last_scope.0, min, true)
                        .unwrap()
                        .is_gt()
                    {
                        merge_scopes.push((min.clone(), max.clone()));
                    } else if Self::bound_compared(&last_scope.1, max, false)
                        .unwrap()
                        .is_lt()
                    {
                        last_scope.1 = max.clone();
                    }
                }
            }
        }
        merge_scopes
            .into_iter()
            .map(|(min, max)| ConstantBinary::Scope {
                min: min.clone(),
                max: max.clone(),
            })
            .chain(eqs.into_iter().map(|val| ConstantBinary::Eq(val.clone())))
            .collect_vec()
    }

    fn join_write(f: &mut Formatter, binaries: &[ConstantBinary], op: &str) -> fmt::Result {
        let binaries = binaries.iter().map(|binary| format!("{}", binary)).join(op);
        write!(f, " {} ", binaries)?;

        Ok(())
    }
}

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
            | ScalarExpression::Function(ScalarFunction { args, .. }) => args
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
            ScalarExpression::Constant(_) => false,
            ScalarExpression::Reference { .. } | ScalarExpression::Empty => unreachable!(),
        }
    }

    fn unpack_val(&self) -> Option<ValueRef> {
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

    fn unpack_col(&self, is_deep: bool) -> Option<ColumnRef> {
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
            _ => (),
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

    /// The definition of Or is not the Or in the Where condition.
    /// The And and Or of ConstantBinary are concerned with the data range that needs to be aggregated.
    /// - `ConstantBinary::And`: Aggregate the minimum range of all conditions in and
    /// - `ConstantBinary::Or`: Rearrange and sort the range of each OR data
    pub fn convert_binary(
        &self,
        table_name: &str,
        id: &ColumnId,
    ) -> Result<Option<ConstantBinary>, DatabaseError> {
        match self {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => {
                match (
                    left_expr.convert_binary(table_name, id)?,
                    right_expr.convert_binary(table_name, id)?,
                ) {
                    (Some(left_binary), Some(right_binary)) => match (left_binary, right_binary) {
                        (ConstantBinary::And(mut left), ConstantBinary::And(mut right)) => match op
                        {
                            BinaryOperator::And => {
                                left.append(&mut right);

                                Ok(Some(ConstantBinary::And(left)))
                            }
                            BinaryOperator::Or => Ok(Some(ConstantBinary::Or(vec![
                                ConstantBinary::And(left),
                                ConstantBinary::And(right),
                            ]))),
                            BinaryOperator::Xor => todo!(),
                            _ => unreachable!(),
                        },
                        (ConstantBinary::Or(mut left), ConstantBinary::Or(mut right)) => match op {
                            BinaryOperator::And | BinaryOperator::Or => {
                                left.append(&mut right);

                                Ok(Some(ConstantBinary::Or(left)))
                            }
                            BinaryOperator::Xor => todo!(),
                            _ => unreachable!(),
                        },
                        (ConstantBinary::And(mut left), ConstantBinary::Or(mut right)) => {
                            right.append(&mut left);

                            Ok(Some(ConstantBinary::Or(right)))
                        }
                        (ConstantBinary::Or(mut left), ConstantBinary::And(mut right)) => {
                            left.append(&mut right);

                            Ok(Some(ConstantBinary::Or(left)))
                        }
                        (ConstantBinary::And(mut binaries), binary)
                        | (binary, ConstantBinary::And(mut binaries)) => {
                            binaries.push(binary);

                            Ok(Some(ConstantBinary::And(binaries)))
                        }
                        (ConstantBinary::Or(mut binaries), binary)
                        | (binary, ConstantBinary::Or(mut binaries)) => {
                            binaries.push(binary);

                            Ok(Some(ConstantBinary::Or(binaries)))
                        }
                        (left, right) => match op {
                            BinaryOperator::And => Ok(Some(ConstantBinary::And(vec![left, right]))),
                            BinaryOperator::Or => Ok(Some(ConstantBinary::Or(vec![left, right]))),
                            BinaryOperator::Xor => todo!(),
                            _ => Ok(None),
                        },
                    },
                    (None, None) => {
                        if let (Some(col), Some(val)) =
                            (left_expr.unpack_col(false), right_expr.unpack_val())
                        {
                            return Ok(Self::new_binary(table_name, id, *op, col, val, false));
                        }
                        if let (Some(val), Some(col)) =
                            (left_expr.unpack_val(), right_expr.unpack_col(false))
                        {
                            return Ok(Self::new_binary(table_name, id, *op, col, val, true));
                        }

                        Ok(None)
                    }
                    (Some(binary), None) => {
                        Ok(Self::check_or(table_name, id, right_expr, op, binary))
                    }
                    (None, Some(binary)) => {
                        Ok(Self::check_or(table_name, id, left_expr, op, binary))
                    }
                }
            }
            ScalarExpression::Alias { expr, .. }
            | ScalarExpression::TypeCast { expr, .. }
            | ScalarExpression::Unary { expr, .. }
            | ScalarExpression::In { expr, .. }
            | ScalarExpression::Between { expr, .. }
            | ScalarExpression::SubString { expr, .. } => expr.convert_binary(table_name, id),
            ScalarExpression::IsNull { expr, negated, .. } => match expr.as_ref() {
                ScalarExpression::ColumnRef(column) => {
                    if let (Some(col_id), Some(col_table)) = (column.id(), column.table_name()) {
                        if id == &col_id && col_table.as_str() == table_name {
                            return Ok(Some(if *negated {
                                ConstantBinary::NotEq(NULL_VALUE.clone())
                            } else {
                                ConstantBinary::Eq(NULL_VALUE.clone())
                            }));
                        }
                    }

                    Ok(None)
                }
                ScalarExpression::Constant(_)
                | ScalarExpression::Alias { .. }
                | ScalarExpression::TypeCast { .. }
                | ScalarExpression::IsNull { .. }
                | ScalarExpression::Unary { .. }
                | ScalarExpression::Binary { .. }
                | ScalarExpression::AggCall { .. }
                | ScalarExpression::In { .. }
                | ScalarExpression::Between { .. }
                | ScalarExpression::SubString { .. }
                | ScalarExpression::Function(_) => expr.convert_binary(table_name, id),
                ScalarExpression::Tuple(_)
                | ScalarExpression::Reference { .. }
                | ScalarExpression::Empty => unreachable!(),
            },
            ScalarExpression::Constant(_) | ScalarExpression::ColumnRef(_) => Ok(None),
            // FIXME: support `convert_binary`
            ScalarExpression::Tuple(_)
            | ScalarExpression::AggCall { .. }
            | ScalarExpression::Function(_) => Ok(None),
            ScalarExpression::Reference { .. } | ScalarExpression::Empty => unreachable!(),
        }
    }

    /// check if: c1 > c2 or c1 > 1
    /// this case it makes no sense to just extract c1 > 1
    fn check_or(
        table_name: &str,
        col_id: &ColumnId,
        right_expr: &ScalarExpression,
        op: &BinaryOperator,
        binary: ConstantBinary,
    ) -> Option<ConstantBinary> {
        if matches!(op, BinaryOperator::Or) && right_expr.exist_column(table_name, col_id) {
            return None;
        }

        Some(binary)
    }

    fn new_binary(
        table_name: &str,
        col_id: &ColumnId,
        mut op: BinaryOperator,
        col: ColumnRef,
        val: ValueRef,
        is_flip: bool,
    ) -> Option<ConstantBinary> {
        if !Self::_is_belong(table_name, &col) || col.id() != Some(*col_id) {
            return None;
        }

        if is_flip {
            op = match op {
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                source_op => source_op,
            };
        }

        match op {
            BinaryOperator::Gt => Some(ConstantBinary::Scope {
                min: Bound::Excluded(val.clone()),
                max: Bound::Unbounded,
            }),
            BinaryOperator::Lt => Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val.clone()),
            }),
            BinaryOperator::GtEq => Some(ConstantBinary::Scope {
                min: Bound::Included(val.clone()),
                max: Bound::Unbounded,
            }),
            BinaryOperator::LtEq => Some(ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val.clone()),
            }),
            BinaryOperator::Eq | BinaryOperator::Spaceship => Some(ConstantBinary::Eq(val.clone())),
            BinaryOperator::NotEq => Some(ConstantBinary::NotEq(val.clone())),
            _ => None,
        }
    }

    fn _is_belong(table_name: &str, col: &ColumnRef) -> bool {
        matches!(
            col.table_name().map(|name| table_name == name.as_str()),
            Some(true)
        )
    }
}

impl fmt::Display for ConstantBinary {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ConstantBinary::Scope { min, max } => {
                match min {
                    Bound::Unbounded => write!(f, "-∞")?,
                    Bound::Included(value) => write!(f, "[{}", value)?,
                    Bound::Excluded(value) => write!(f, "({}", value)?,
                }

                write!(f, ", ")?;

                match max {
                    Bound::Unbounded => write!(f, "+∞")?,
                    Bound::Included(value) => write!(f, "{}]", value)?,
                    Bound::Excluded(value) => write!(f, "{})", value)?,
                }

                Ok(())
            }
            ConstantBinary::Eq(value) => write!(f, "{}", value),
            ConstantBinary::NotEq(value) => write!(f, "!{}", value),
            ConstantBinary::And(binaries) => Self::join_write(f, binaries, " AND "),
            ConstantBinary::Or(binaries) => Self::join_write(f, binaries, " OR "),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::errors::DatabaseError;
    use crate::expression::simplify::ConstantBinary;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use std::sync::Arc;

    #[test]
    fn test_convert_binary_simple() -> Result<(), DatabaseError> {
        let col_1 = Arc::new(ColumnCatalog {
            summary: ColumnSummary {
                id: Some(0),
                name: "c1".to_string(),
                table_name: Some(Arc::new("t1".to_string())),
            },
            nullable: false,
            desc: ColumnDesc {
                column_datatype: LogicalType::Integer,
                is_primary: false,
                is_unique: false,
                default: None,
            },
        });
        let val_1 = Arc::new(DataValue::Int32(Some(1)));

        let binary_eq = ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            right_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(binary_eq, ConstantBinary::Eq(val_1.clone()));

        let binary_not_eq = ScalarExpression::Binary {
            op: BinaryOperator::NotEq,
            left_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            right_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(binary_not_eq, ConstantBinary::NotEq(val_1.clone()));

        let binary_lt = ScalarExpression::Binary {
            op: BinaryOperator::Lt,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(
            binary_lt,
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val_1.clone())
            }
        );

        let binary_lteq = ScalarExpression::Binary {
            op: BinaryOperator::LtEq,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(
            binary_lteq,
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val_1.clone())
            }
        );

        let binary_gt = ScalarExpression::Binary {
            op: BinaryOperator::Gt,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(
            binary_gt,
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Unbounded
            }
        );

        let binary_gteq = ScalarExpression::Binary {
            op: BinaryOperator::GtEq,
            left_expr: Box::new(ScalarExpression::ColumnRef(col_1.clone())),
            right_expr: Box::new(ScalarExpression::Constant(val_1.clone())),
            ty: LogicalType::Boolean,
        }
        .convert_binary("t1", &0)?
        .unwrap();

        assert_eq!(
            binary_gteq,
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Unbounded
            }
        );

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_eq_noteq() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::And(vec![
            ConstantBinary::Eq(val_0.clone()),
            ConstantBinary::NotEq(val_1.clone()),
            ConstantBinary::Eq(val_2.clone()),
            ConstantBinary::NotEq(val_3.clone()),
        ]);

        binary.scope_aggregation()?;

        assert_eq!(binary, ConstantBinary::And(vec![ConstantBinary::Eq(val_0)]));

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_eq_noteq_cover() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::And(vec![
            ConstantBinary::Eq(val_0.clone()),
            ConstantBinary::NotEq(val_1.clone()),
            ConstantBinary::Eq(val_2.clone()),
            ConstantBinary::NotEq(val_3.clone()),
            ConstantBinary::NotEq(val_0.clone()),
            ConstantBinary::NotEq(val_1.clone()),
            ConstantBinary::NotEq(val_2.clone()),
            ConstantBinary::NotEq(val_3.clone()),
        ]);

        binary.scope_aggregation()?;

        assert_eq!(binary, ConstantBinary::And(vec![]));

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_scope() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::And(vec![
            ConstantBinary::Scope {
                min: Bound::Excluded(val_0.clone()),
                max: Bound::Included(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Excluded(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Included(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Excluded(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            },
        ]);

        binary.scope_aggregation()?;

        assert_eq!(
            binary,
            ConstantBinary::And(vec![ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Excluded(val_2.clone()),
            }])
        );

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_mixed() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::And(vec![
            ConstantBinary::Scope {
                min: Bound::Excluded(val_0.clone()),
                max: Bound::Included(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Excluded(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Included(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Excluded(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            },
            ConstantBinary::Eq(val_1.clone()),
            ConstantBinary::Eq(val_0.clone()),
            ConstantBinary::NotEq(val_1.clone()),
        ]);

        binary.scope_aggregation()?;

        assert_eq!(
            binary,
            ConstantBinary::And(vec![ConstantBinary::Eq(val_0.clone())])
        );

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_or() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::Or(vec![
            ConstantBinary::Scope {
                min: Bound::Excluded(val_0.clone()),
                max: Bound::Included(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Excluded(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Included(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Excluded(val_3.clone()),
            },
        ]);

        binary.scope_aggregation()?;

        assert_eq!(
            binary,
            ConstantBinary::Or(vec![ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Included(val_3.clone()),
            }])
        );

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_or_unbounded() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::Or(vec![
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Unbounded,
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Unbounded,
            },
        ]);

        binary.scope_aggregation()?;

        assert_eq!(binary, ConstantBinary::Or(vec![]));

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_or_lower_unbounded() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(2)));
        let val_1 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::Or(vec![
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val_0.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val_0.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val_1.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val_1.clone()),
            },
        ]);

        binary.scope_aggregation()?;

        assert_eq!(
            binary,
            ConstantBinary::Or(vec![ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val_1.clone()),
            }])
        );

        Ok(())
    }

    #[test]
    fn test_scope_aggregation_or_upper_unbounded() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(2)));
        let val_1 = Arc::new(DataValue::Int32(Some(3)));

        let mut binary = ConstantBinary::Or(vec![
            ConstantBinary::Scope {
                min: Bound::Excluded(val_0.clone()),
                max: Bound::Unbounded,
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Unbounded,
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Unbounded,
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Unbounded,
            },
        ]);

        binary.scope_aggregation()?;

        assert_eq!(
            binary,
            ConstantBinary::Or(vec![ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Unbounded,
            }])
        );

        Ok(())
    }

    #[test]
    fn test_rearrange() -> Result<(), DatabaseError> {
        let val_0 = Arc::new(DataValue::Int32(Some(0)));
        let val_1 = Arc::new(DataValue::Int32(Some(1)));
        let val_2 = Arc::new(DataValue::Int32(Some(2)));
        let val_3 = Arc::new(DataValue::Int32(Some(3)));

        let val_5 = Arc::new(DataValue::Int32(Some(5)));

        let val_6 = Arc::new(DataValue::Int32(Some(6)));
        let val_7 = Arc::new(DataValue::Int32(Some(7)));
        let val_8 = Arc::new(DataValue::Int32(Some(8)));

        let val_10 = Arc::new(DataValue::Int32(Some(10)));

        let binary = ConstantBinary::Or(vec![
            ConstantBinary::Scope {
                min: Bound::Excluded(val_6.clone()),
                max: Bound::Included(val_10.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_0.clone()),
                max: Bound::Included(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_1.clone()),
                max: Bound::Excluded(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Excluded(val_1.clone()),
                max: Bound::Included(val_2.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_0.clone()),
                max: Bound::Excluded(val_3.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Included(val_6.clone()),
                max: Bound::Included(val_7.clone()),
            },
            ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            },
            ConstantBinary::NotEq(val_8.clone()),
            ConstantBinary::Eq(val_5.clone()),
            ConstantBinary::Eq(val_0.clone()),
            ConstantBinary::Eq(val_1.clone()),
        ]);

        assert_eq!(
            binary.rearrange()?,
            vec![
                ConstantBinary::Scope {
                    min: Bound::Included(val_0.clone()),
                    max: Bound::Included(val_3.clone()),
                },
                ConstantBinary::Eq(val_5.clone()),
                ConstantBinary::Scope {
                    min: Bound::Included(val_6.clone()),
                    max: Bound::Included(val_10.clone()),
                }
            ]
        );

        Ok(())
    }
}
