use super::DataType;

#[derive(Debug, PartialEq, Clone)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    //     Blob(Blob),
    //     Decimal(Decimal),
    //     Date(Date),
    // Interval(Interval),
}
impl DataValue {
    /// Get the type of value. `None` means NULL.
    pub fn data_type(&self) -> Option<DataType> {
        todo!()
    }
}
