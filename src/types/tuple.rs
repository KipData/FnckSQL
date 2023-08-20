use crate::types::value::DataValue;

pub type TupleId = usize;

pub struct Tuple {
    id: TupleId,
    pub values: Vec<DataValue>
}