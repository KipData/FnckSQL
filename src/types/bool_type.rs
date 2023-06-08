use crate::types::{DataType, DataTypeEnum};
use std::any::Any;
use std::sync::Arc;

/// BoolType is a boolean type
pub(crate) struct BoolType {
    /// TODO: pub(crate) or pub?
    pub(crate) nullable: bool,
}

impl DataType for BoolType {
    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn get_type(&self) -> DataTypeEnum {
        DataTypeEnum::Bool
    }

    fn get_data_len(&self) -> u32 {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_int32() {
        let int32_type = BoolType { nullable: false };
        assert_eq!(int32_type.is_nullable(), false);
        assert_eq!(int32_type.get_data_len(), 1);
    }
}
