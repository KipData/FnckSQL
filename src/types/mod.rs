pub mod value;

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Release};
use integer_encoding::FixedInt;
pub use sqlparser::ast::DataType as DataTypeKind;

static ID_BUF: AtomicU32 = AtomicU32::new(0);

/// Inner data type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    #[inline]
    pub const fn new(kind: DataTypeKind, nullable: bool) -> DataType {
        DataType { kind, nullable }
    }
    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    #[inline]
    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }
}

pub trait DataTypeExt {
    fn nullable(self) -> DataType;
    fn not_null(self) -> DataType;
}

impl DataTypeExt for DataTypeKind {
    #[inline]
    fn nullable(self) -> DataType {
        DataType::new(self, true)
    }
    #[inline]
    fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}

pub(crate) struct IdGenerator { }

impl IdGenerator {
    pub(crate) fn encode_to_raw() -> Vec<u8> {
        ID_BUF
            .load(Acquire)
            .encode_fixed_vec()
    }

    pub(crate) fn from_raw(buf: &[u8]) {
        Self::init(u32::decode_fixed(buf))
    }

    pub(crate) fn init(init_value: u32) {
        ID_BUF.store(init_value, Release)
    }

    pub(crate) fn build() -> u32 {
        ID_BUF.fetch_add(1, Release)
    }
}

pub type TableId = u32;
pub type ColumnId = u32;


#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering::Release;
    use crate::types::{ID_BUF, IdGenerator};

    /// Tips: 由于IdGenerator为static全局性质生成的id，因此需要单独测试避免其他测试方法干扰
    #[test]
    #[ignore]
    fn test_id_generator() {
        assert_eq!(IdGenerator::build(), 0);
        assert_eq!(IdGenerator::build(), 1);

        let buf = IdGenerator::encode_to_raw();
        test_id_generator_reset();

        assert_eq!(IdGenerator::build(), 0);

        IdGenerator::from_raw(&buf);

        assert_eq!(IdGenerator::build(), 2);
        assert_eq!(IdGenerator::build(), 3);
    }

    fn test_id_generator_reset() {
        ID_BUF.store(0, Release)
    }
}