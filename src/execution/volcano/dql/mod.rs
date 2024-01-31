pub(crate) mod aggregate;
pub(crate) mod dummy;
pub(crate) mod explain;
pub(crate) mod filter;
pub(crate) mod index_scan;
pub(crate) mod join;
pub(crate) mod limit;
pub(crate) mod projection;
pub(crate) mod seq_scan;
pub(crate) mod show_table;
pub(crate) mod sort;
pub(crate) mod values;

#[cfg(test)]
pub(crate) mod test {
    use crate::types::value::{DataValue, ValueRef};
    use itertools::Itertools;
    use std::sync::Arc;

    pub(crate) fn build_integers(ints: Vec<Option<i32>>) -> Vec<ValueRef> {
        ints.into_iter()
            .map(|i| Arc::new(DataValue::Int32(i)))
            .collect_vec()
    }
}
