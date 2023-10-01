#![feature(error_generic_member_access)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(generators)]
#![feature(iterator_try_collect)]
#![feature(slice_pattern)]
#![feature(bound_map)]
extern crate core;
pub mod binder;
pub mod catalog;
pub mod db;
pub mod expression;
pub mod parser;
pub mod planner;
pub mod types;
mod optimizer;
pub mod execution;
pub mod storage;

pub mod mock {
    use std::sync::Arc;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    pub fn build_tuple() -> Tuple {
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false),
                None
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar(None), false, false),
                None
            )),
        ];
        let values = vec![
            Arc::new(DataValue::Int32(Some(9))),
            Arc::new(DataValue::Utf8(Some("LOL".to_string()))),
        ];

        Tuple {
            id: None,
            columns,
            values,
        }
    }
}
