use crate::expression::simplify::ConstantBinary;
use crate::types::value::DataValue;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::{cmp, mem};

type FastHasher = SipHasher13;

// https://github.com/jedisct1/rust-count-min-sketch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketch<K> {
    counters: Vec<Vec<usize>>,
    offsets: Vec<usize>,
    hashers: [FastHasher; 2],
    mask: usize,
    k_num: usize,
    phantom_k: PhantomData<K>,
}

impl CountMinSketch<DataValue> {
    /// Tips:
    /// - binaries must be used `ConstantBinary::scope_aggregation` and `ConstantBinary::rearrange`
    /// - just count with `ConstantBinary::Eq`
    pub fn collect_count(&self, binaries: &[ConstantBinary]) -> usize {
        let mut count = 0;

        for binary in binaries {
            count += match binary {
                ConstantBinary::Eq(value) => self.estimate(value),
                ConstantBinary::NotEq(_) | ConstantBinary::Scope { .. } => 0,
                ConstantBinary::And(binaries) | ConstantBinary::Or(binaries) => {
                    self.collect_count(binaries)
                }
            }
        }

        count
    }
}

impl<K: Hash> CountMinSketch<K> {
    pub fn new(capacity: usize, probability: f64, tolerance: f64) -> Self {
        let width = Self::optimal_width(capacity, tolerance);
        let k_num = Self::optimal_k_num(probability);
        let counters = vec![vec![0; width]; k_num];
        let offsets = vec![0; k_num];
        let hashers = [Self::sip_new(), Self::sip_new()];
        CountMinSketch {
            counters,
            offsets,
            hashers,
            mask: Self::mask(width),
            k_num,
            phantom_k: PhantomData,
        }
    }

    pub fn add<Q: ?Sized + Hash>(&mut self, key: &Q, value: usize)
    where
        K: Borrow<Q>,
    {
        let mut hashes = [0u64, 0u64];
        let lowest = (0..self.k_num)
            .map(|k_i| {
                let offset = self.offset(&mut hashes, key, k_i);
                self.offsets[k_i] = offset;
                self.counters[k_i][offset]
            })
            .min()
            .unwrap();
        for k_i in 0..self.k_num {
            let offset = self.offsets[k_i];
            if self.counters[k_i][offset] == lowest {
                self.counters[k_i][offset] = self.counters[k_i][offset].saturating_add(value);
            }
        }
    }

    pub fn increment<Q: ?Sized + Hash>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
    {
        self.add(key, 1)
    }

    pub fn estimate<Q: ?Sized + Hash>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        let mut hashes = [0u64, 0u64];
        (0..self.k_num)
            .map(|k_i| {
                let offset = self.offset(&mut hashes, key, k_i);
                self.counters[k_i][offset]
            })
            .min()
            .unwrap()
    }

    #[allow(dead_code)]
    pub fn estimate_memory(
        capacity: usize,
        probability: f64,
        tolerance: f64,
    ) -> Result<usize, &'static str> {
        let width = Self::optimal_width(capacity, tolerance);
        let k_num = Self::optimal_k_num(probability);
        Ok(width * mem::size_of::<u64>() * k_num)
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        for k_i in 0..self.k_num {
            for counter in &mut self.counters[k_i] {
                *counter = 0
            }
        }
        self.hashers = [Self::sip_new(), Self::sip_new()];
    }

    fn optimal_width(capacity: usize, tolerance: f64) -> usize {
        let e = tolerance / (capacity as f64);
        let width = (2.0 / e).round() as usize;
        cmp::max(2, width)
            .checked_next_power_of_two()
            .expect("Width would be way too large")
    }

    fn mask(width: usize) -> usize {
        assert!(width > 1);
        assert_eq!(width & (width - 1), 0);
        width - 1
    }

    fn optimal_k_num(probability: f64) -> usize {
        cmp::max(1, ((1.0 - probability).ln() / 0.5f64.ln()) as usize)
    }

    fn sip_new() -> FastHasher {
        let mut rng = rand::thread_rng();
        FastHasher::new_with_keys(rng.next_u64(), rng.next_u64())
    }

    fn offset<Q: ?Sized + Hash>(&self, hashes: &mut [u64; 2], key: &Q, k_i: usize) -> usize
    where
        K: Borrow<Q>,
    {
        if k_i < 2 {
            let sip = &mut self.hashers[k_i].clone();
            key.hash(sip);
            let hash = sip.finish();
            hashes[k_i] = hash;
            hash as usize & self.mask
        } else {
            hashes[0].wrapping_add((k_i as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5)
                as usize
                & self.mask
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::simplify::ConstantBinary;
    use crate::optimizer::core::cm_sketch::CountMinSketch;
    use crate::types::value::DataValue;
    use std::collections::Bound;
    use std::sync::Arc;

    #[test]
    fn test_increment() {
        let mut cms = CountMinSketch::<&str>::new(100, 0.95, 10.0);
        for _ in 0..300 {
            cms.increment("key");
        }
        assert_eq!(cms.estimate("key"), 300);
    }

    #[test]
    fn test_increment_multi() {
        let mut cms = CountMinSketch::<u64>::new(100, 0.99, 2.0);
        for i in 0..1_000_000 {
            cms.increment(&(i % 100));
        }
        for key in 0..100 {
            assert!(cms.estimate(&key) >= 9_000);
        }
    }

    #[test]
    fn test_collect_count() {
        let mut cms = CountMinSketch::<DataValue>::new(100, 0.95, 10.0);
        for _ in 0..300 {
            cms.increment(&DataValue::Int32(Some(300)));
        }
        assert_eq!(
            cms.collect_count(&vec![
                ConstantBinary::Eq(Arc::new(DataValue::Int32(Some(300)))),
                ConstantBinary::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                }
            ]),
            300
        );
    }
}
