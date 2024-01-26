#[macro_export]
macro_rules! single_mapping {
    ($ty:ty, $pattern:expr, $option:expr) => {
        impl MatchPattern for $ty {
            fn pattern(&self) -> &Pattern {
                &$pattern
            }
        }

        impl<T: Transaction> ImplementationRule<T> for $ty {
            fn to_expression(
                &self,
                _: &Operator,
                _: &HistogramLoader<'_, T>,
                group_expr: &mut GroupExpression,
            ) -> Result<(), OptimizerError> {
                //TODO: CostModel
                group_expr.append_expr(Expression {
                    op: $option,
                    cost: None,
                });

                Ok(())
            }
        }
    };
}
