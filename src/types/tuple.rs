use crate::types::value::DataValue;

pub type TupleId = usize;

#[derive(Clone)]
pub struct Tuple {
    pub id: TupleId,
    pub values: Vec<DataValue>
}