use arrow::array::{Array, BooleanArray};
use anyhow::Result;

/// Downcast an Arrow Array to a concrete type
macro_rules! downcast_value {
    ($Value:expr, $Type:ident) => {{
        use std::any::type_name;

        if let Some(value) = $Value.as_any().downcast_ref::<$Type>() {
            Ok(value)
        } else {
            Err(anyhow::anyhow!("could not cast value to {}", type_name::<$Type>()))
        }
    }};
}

/// Downcast ArrayRef to BooleanArray
pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray> {
    downcast_value!(array, BooleanArray)
}