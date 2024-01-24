use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use rand::random;
#[allow(deprecated)]
use std::hash::{Hash, Hasher, SipHasher};

#[derive(Debug)]
pub enum Estimator {
    HyperLogLog,
    LinearCounting, // スモールレンジの見積もりに使用する。
}

// https://qiita.com/tatsuya6502/items/832f71b78c62ecad65c5
pub struct HyperLogLog {
    b: u8,
    #[allow(unused)]
    b_mask: usize,
    m: usize,
    alpha: f64,
    registers: Vec<u8>,
    hasher_key0: u64,
    hasher_key1: u64,
}

impl HyperLogLog {
    pub fn new(b: u8) -> Result<Self, Box<dyn Error>> {
        if b < 4 || b > 16 {
            return Err(From::from(format!("b must be between 4 and 16. b = {}", b)));
        }

        let m = 1 << b;
        let alpha = Self::get_alpha(b)?;

        Ok(HyperLogLog {
            alpha,
            b,
            b_mask: m - 1,
            m,
            registers: vec![0; m],
            hasher_key0: random(),
            hasher_key1: random(),
        })
    }

    pub fn insert<H: Hash>(&mut self, value: &H) {
        let x = self.hash(value);
        let j = x as usize & self.b_mask;
        let w = x >> self.b;

        let p1 = Self::position_of_leftmost_one_bit(w, 64 - self.b);
        let p2 = &mut self.registers[j];
        if *p2 < p1 {
            *p2 = p1;
        }
    }

    pub fn cardinality(&self) -> f64 {
        Self::estimate_cardinality(self).0
    }

    pub fn typical_error_rate(&self) -> f64 {
        1.04 / (self.m as f64).sqrt()
    }

    pub fn histgram_of_register_value_distribution(&self) -> String {
        let mut histgram = Vec::new();

        let mut map = BTreeMap::new();
        for x in &self.registers {
            let count = map.entry(*x).or_insert(0);
            *count += 1;
        }

        if let (Some(last_reg_value), Some(max_count)) = (map.keys().last(), map.values().max()) {
            let width = 40.0;
            let rate = width / (*max_count as f64);

            for i in 0..(last_reg_value + 1) {
                let mut line = format!("-{:3}: ", i);

                if let Some(count) = map.get(&i) {
                    let h_bar = std::iter::repeat("*")
                        .take((*count as f64 * rate).ceil() as usize)
                        .collect::<String>();
                    line.push_str(&h_bar);
                    line.push_str(&format!(" {}", count));
                } else {
                    line.push_str("0");
                };

                histgram.push(line);
            }
        }
        histgram.join("\n")
    }

    fn get_alpha(b: u8) -> Result<f64, Box<dyn Error>> {
        if b < 4 || b > 16 {
            Err(From::from(format!("b must be between 4 and 16. b = {}", b)))
        } else {
            Ok(match b {
                4 => 0.673, // α16
                5 => 0.697, // α32
                6 => 0.709, // α64
                _ => 0.7213 / (1.0 + 1.079 / (1 << b) as f64),
            })
        }
    }

    #[allow(deprecated)]
    fn hash<H: Hash>(&self, value: &H) ->u64 {
        let mut hasher = SipHasher::new_with_keys(self.hasher_key0, self.hasher_key1);
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn position_of_leftmost_one_bit(s: u64, max_width: u8) -> u8 {
        Self::count_leading_zeros(s, max_width) + 1
    }

    fn count_leading_zeros(mut s: u64, max_width: u8) -> u8 {
        let mut lz = max_width;
        while s != 0 {
            lz -= 1;
            s >>= 1;
        }
        lz
    }

    fn estimate_cardinality(hll: &HyperLogLog) -> (f64, Estimator) {
        let m_f64 = hll.m as f64;
        let est = Self::raw_hyperloglog_estimate(hll.alpha, m_f64, &hll.registers);

        if est < (5.0 / 2.0 * m_f64) {
            match Self::count_zero_registers(&hll.registers) {
                0 => (est, Estimator::HyperLogLog),
                v => (Self::linear_counting_estimate(m_f64, v as f64), Estimator::LinearCounting),
            }
        } else {
            (est, Estimator::HyperLogLog)
        }
    }

    fn count_zero_registers(registers: &[u8]) -> usize {
        registers.iter().filter(|&x| *x == 0).count()
    }

    fn raw_hyperloglog_estimate(alpha: f64, m: f64, registers: &[u8]) -> f64 {
        let sum = registers.iter().map(|&x| 2.0f64.powi(-(x as i32))).sum::<f64>();
        alpha * m * m / sum
    }

    fn linear_counting_estimate(m: f64, number_of_zero_registers: f64) -> f64 {
        m * (m / number_of_zero_registers).ln()
    }
}

impl fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (est, est_method) = Self::estimate_cardinality(self);
        write!(f,
               r#"HyperLogLog
  estimated cardinality: {}
  estimation method:     {:?}
  -----------------------------------------------------
  b:      {} bits (typical error rate: {}%)
  m:      {} registers
  alpha:  {}
  hasher: ({}, {})"#,
               est,
               est_method,
               self.b,
               self.typical_error_rate() * 100.0,
               self.m,
               self.alpha,
               self.hasher_key0,
               self.hasher_key1)
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;
    use rand_distr::Normal;
    use rand::thread_rng;
    use rand::distributions::Distribution;
    use crate::optimizer::utils::hyper_log_log::HyperLogLog;

    #[allow(unused)]
    fn plot_normal_distribution(data: &[f64], num: usize) {
        let max_value = data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min_value = data.iter().cloned().fold(f64::INFINITY, f64::min);

        let num_bins = num / 100;
        let bin_width = (max_value - min_value) / num_bins as f64;

        let mut bin_counts = vec![0i64; num_bins];

        for &sample in data {
            let bin = ((sample - min_value) / bin_width).clamp(0.0, num_bins as f64 - 1.0) as usize;
            bin_counts[bin] += 1;
        }

        for bin in 0..num_bins {
            let lower_bound = min_value + bin as f64 * bin_width;
            let upper_bound = lower_bound + bin_width;
            let bin_count = bin_counts[bin];

            print!("{: <width$} - {: <width$} | ", lower_bound, upper_bound, width = 8);

            for _ in 0..bin_count {
                print!("#");
            }

            println!();
        }
    }

    #[test]
    fn create_hll() {
        assert!(HyperLogLog::new(3).is_err());
        assert!(HyperLogLog::new(17).is_err());

        let hll = HyperLogLog::new(4);
        assert!(hll.is_ok());

        let hll = hll.unwrap();
        assert_eq!(hll.b, 4);
        assert_eq!(hll.m, 2_usize.pow(4));
        assert_eq!(hll.alpha, 0.673);
        assert_eq!(hll.registers.len(), 2_usize.pow(4));

        assert!(HyperLogLog::new(16).is_ok());
    }

    #[test]
    fn normal_distribution() {
        let item_num = 100000;

        let mut rng = thread_rng();
        let normal = Normal::new((item_num / 8) as f64, (item_num / 4) as f64).unwrap();

        let mut hll = HyperLogLog::new(14).unwrap();
        let mut samples = Vec::new();

        for _ in 0..item_num {
            let sample = normal.sample(&mut rng);

            samples.push(sample);
            hll.insert(&OrderedFloat(sample));
        }
        println!("\n=== Cardinality: {}\n", hll.cardinality());

        println!("Histogram: \n{}", hll.histgram_of_register_value_distribution());

        println!("Samples Len: {}", samples.len());

        // plot_normal_distribution(&samples, item_num);
    }
}