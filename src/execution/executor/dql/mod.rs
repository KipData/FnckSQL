pub(crate) mod seq_scan;
pub(crate) mod projection;
pub(crate) mod values;
pub(crate) mod filter;
pub(crate) mod sort;
pub(crate) mod limit;
pub(crate) mod join;
pub(crate) mod dummy;
pub(crate) mod aggregate;

#[cfg(test)]
pub(crate) mod test {
    use std::sync::Arc;
    use itertools::Itertools;
    use crate::types::value::{DataValue, ValueRef};

    pub(crate) fn build_integers(ints: Vec<Option<i32>>) -> Vec<ValueRef> {
        ints.into_iter()
            .map(|i| Arc::new(DataValue::Int32(i)))
            .collect_vec()
    }
}