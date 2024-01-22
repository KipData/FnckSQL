#[macro_export]
macro_rules! single_mapping {
    ($ty:ty, $pattern:expr, $option:expr) => {
        impl MatchPattern for $ty {
            fn pattern(&self) -> &Pattern {
                &$pattern
            }
        }

        impl ImplementationRule for $ty {
            fn to_expression(&self, _: &Operator, group_expr: &mut GroupExpression) -> Result<(), OptimizerError> {
                group_expr.append_expr(Expression {
                    ops: vec![$option]
                });

                Ok(())
            }
        }
    };
}