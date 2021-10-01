use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use near_vm_logic::ExtCosts;
use num_rational::Ratio;
use std::collections::BTreeMap;

pub struct ExtCostsGenerator {
    agg: BTreeMap<Metric, DataStats>,
    result: BTreeMap<ExtCosts, Ratio<u64>>,
}

impl ExtCostsGenerator {
    pub fn new(measurement: &Measurements) -> Self {
        let agg = measurement.aggregate();
        Self { agg, result: Default::default() }
    }

    fn extract_value(&mut self, metric: Metric, ext_cost: ExtCosts) -> Option<Ratio<u64>> {
        let base = self.agg.get(&Metric::noop)?;
        let agg = self.agg.get(&metric)?;
        let multiplier = agg.ext_costs[&ext_cost];
        Some(Ratio::new(agg.upper_with_base(base), multiplier))
    }

    fn extract(&mut self, metric: Metric, ext_cost: ExtCosts) {
        if let Some(value) = self.extract_value(metric, ext_cost) {
            self.result.insert(ext_cost, value);
        }
    }

    fn extract_max(&mut self, metric1: Metric, metric2: Metric, ext_cost: ExtCosts) {
        if let Some(value1) = self.extract_value(metric1, ext_cost) {
            if let Some(value2) = self.extract_value(metric2, ext_cost) {
                self.result.insert(ext_cost, value1.max(value2));
            }
        }
    }

    pub fn compute(mut self) -> BTreeMap<ExtCosts, Ratio<u64>> {
        use ExtCosts::*;
        use Metric::*;

        self.extract(base_1M, base);
        self.extract(read_memory_10b_10k, read_memory_base);
        self.extract(read_memory_1Mib_10k, read_memory_byte);
        self.extract(write_register_10b_10k, write_register_base);
        self.extract(write_register_1Mib_10k, write_register_byte);
        self.extract(read_register_10b_10k, read_register_base);
        self.extract(read_register_1Mib_10k, read_register_byte);
        self.extract(write_memory_10b_10k, write_memory_base);
        self.extract(write_memory_1Mib_10k, write_memory_byte);

        self.extract(utf16_log_10b_10k, log_base);
        self.extract(utf16_log_10kib_10k, log_byte);

        self.extract(utf8_log_10b_10k, utf8_decoding_base);

        // Charge the maximum between non-nul-terminated and nul-terminated costs.
        self.extract_max(utf8_log_10kib_10k, nul_utf8_log_10kib_10k, utf8_decoding_byte);

        self.extract(utf16_log_10b_10k, utf16_decoding_base);

        // Charge the maximum between non-nul-terminated and nul-terminated costs.
        self.extract_max(utf16_log_10kib_10k, nul_utf16_log_10kib_10k, utf16_decoding_byte);

        self.extract(sha256_10b_10k, sha256_base);
        self.extract(sha256_10kib_10k, sha256_byte);

        self.extract(keccak256_10b_10k, keccak256_base);
        self.extract(keccak256_10kib_10k, keccak256_byte);

        self.extract(keccak512_10b_10k, keccak512_base);
        self.extract(keccak512_10kib_10k, keccak512_byte);

        self.extract(ripemd160_10b_10k, ripemd160_base);
        self.extract(ripemd160_10kib_10k, ripemd160_block);

        self.extract(ecrecover_10k, ecrecover_base);

        #[cfg(feature = "protocol_feature_alt_bn128")]
        {
            self.extract(alt_bn128_g1_multiexp_1_1k, alt_bn128_g1_multiexp_base);
            self.extract(alt_bn128_g1_multiexp_10_1k, alt_bn128_g1_multiexp_byte);
            self.extract(alt_bn128_g1_multiexp_10_1k, alt_bn128_g1_multiexp_sublinear);

            self.extract(alt_bn128_g1_sum_1_1k, alt_bn128_g1_sum_base);
            self.extract(alt_bn128_g1_sum_10_1k, alt_bn128_g1_sum_byte);

            self.extract(alt_bn128_pairing_check_1_1k, alt_bn128_pairing_check_base);
            self.extract(alt_bn128_pairing_check_10_1k, alt_bn128_pairing_check_byte);
        }

        // TODO: Redo storage costs once we have counting of nodes and we have size peek.
        self.extract(storage_write_10b_key_10b_value_1k, storage_write_base);
        self.extract(storage_write_10kib_key_10b_value_1k, storage_write_key_byte);
        self.extract(storage_write_10b_key_10kib_value_1k, storage_write_value_byte);
        self.extract(storage_write_10b_key_10kib_value_1k_evict, storage_write_evicted_byte);
        self.extract(storage_read_10b_key_10b_value_1k, storage_read_base);
        self.extract(storage_read_10kib_key_10b_value_1k, storage_read_key_byte);
        self.extract(storage_read_10b_key_10kib_value_1k, storage_read_value_byte);
        self.extract(storage_remove_10b_key_10b_value_1k, storage_remove_base);
        self.extract(storage_remove_10kib_key_10b_value_1k, storage_remove_key_byte);
        self.extract(storage_remove_10b_key_10kib_value_1k, storage_remove_ret_value_byte);
        self.extract(storage_has_key_10b_key_10b_value_1k, storage_has_key_base);
        self.extract(storage_has_key_10kib_key_10b_value_1k, storage_has_key_byte);

        self.extract(promise_and_100k, promise_and_base);
        self.extract(promise_and_100k_on_1k_and, promise_and_per_promise);
        self.extract(promise_return_100k, promise_return);

        self.result
    }
}
impl std::fmt::Display for ExtCostsGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (k, v) in &self.agg {
            writeln!(f, "{:?}\t\t\t\t{}", k, v)?;
        }
        Ok(())
    }
}
