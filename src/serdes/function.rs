use crate::expression::function::scala::ArcScalarFunctionImpl;
use crate::expression::function::table::ArcTableFunctionImpl;
use crate::implement_serialization_by_bincode;

implement_serialization_by_bincode!(ArcScalarFunctionImpl);
implement_serialization_by_bincode!(ArcTableFunctionImpl);
