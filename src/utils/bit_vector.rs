use integer_encoding::FixedInt;
use itertools::Itertools;
use std::slice;

#[derive(Debug, Default)]
pub struct BitVector {
    #[allow(dead_code)]
    len: u64,
    bit_groups: Vec<i8>,
}

impl BitVector {
    pub fn new(len: usize) -> BitVector {
        BitVector {
            len: len as u64,
            bit_groups: vec![0; (len + 7) / 8],
        }
    }

    pub fn set_bit(&mut self, index: usize, value: bool) {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if value {
            self.bit_groups[byte_index] |= 1 << bit_index;
        } else {
            self.bit_groups[byte_index] &= !(1 << bit_index);
        }
    }

    pub fn get_bit(&self, index: usize) -> bool {
        self.bit_groups[index / 8] >> (index % 8) & 1 != 0
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[allow(dead_code)]
    pub fn to_raw(&self, bytes: &mut Vec<u8>) {
        bytes.append(&mut u64::encode_fixed_vec(self.len));

        for bits in &self.bit_groups {
            bytes.append(&mut bits.encode_fixed_vec());
        }
    }

    #[allow(dead_code)]
    pub fn from_raw(bytes: &[u8]) -> Self {
        let len = u64::decode_fixed(&bytes[0..8]);
        let bit_groups = bytes[8..]
            .iter()
            .map(|bit| i8::decode_fixed(slice::from_ref(bit)))
            .collect_vec();

        BitVector { len, bit_groups }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::bit_vector::BitVector;

    #[test]
    fn bit_vector_serialization() {
        let mut vector = BitVector::new(100);

        vector.set_bit(99, true);

        let mut bytes = Vec::new();

        vector.to_raw(&mut bytes);
        let vector = BitVector::from_raw(&bytes);

        for i in 0..98 {
            assert!(!vector.get_bit(i));
        }
        assert!(vector.get_bit(99));
    }

    #[test]
    fn bit_vector_simple() {
        let mut vector = BitVector::new(100);

        vector.set_bit(99, true);

        for i in 0..98 {
            assert!(!vector.get_bit(i));
        }
        assert!(vector.get_bit(99));
    }
}
