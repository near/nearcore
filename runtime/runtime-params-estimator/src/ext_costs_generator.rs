use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use near_vm_logic::ExtCosts;
use std::collections::BTreeMap;

pub struct ExtCostsGenerator {
    agg: BTreeMap<Metric, DataStats>,
    result: BTreeMap<ExtCosts, f64>,
}

impl ExtCostsGenerator {
    pub fn new(measurement: &Measurements) -> Self {
        let aggregated = measurement.aggregate();
        Self { agg: aggregated, result: Default::default() }
    }

    fn extract_value(
        &mut self,
        metric: Metric,
        ext_cost: ExtCosts,
        ignore_costs: &[ExtCosts],
    ) -> f64 {
        let Self { agg, result } = self;
        let agg = &agg[&metric];
        let mut res = agg.upper() as f64;
        let mut multiplier = None;
        for (k, v) in &agg.ext_costs {
            if ignore_costs.contains(k) || k == &ExtCosts::touching_trie_node {
                continue;
            }
            if k == &ext_cost {
                multiplier = Some(*v);
                continue;
            }
            res -= match result.get(k) {
                Some(x) => x * (*v),
                None => panic!(
                    "While extracting {:?} cost from {:?} metric, {:?} cost was not computed yet",
                    ext_cost, metric, k
                ),
            };
        }
        match multiplier {
            Some(x) => res /= x,
            None => panic!(
                "While extracting {:?} cost from {:?} metric the cost was not found",
                ext_cost, metric
            ),
        };
        res
    }

    fn extract(&mut self, metric: Metric, ext_cost: ExtCosts, ignore_costs: &[ExtCosts]) {
        let value = self.extract_value(metric, ext_cost, ignore_costs);
        self.result.insert(ext_cost, value);
    }

    pub fn compute(&mut self) -> BTreeMap<ExtCosts, f64> {
        self.result.clear();
        use ExtCosts::*;
        use Metric::*;
        self.extract(base_1M, base, &[]);
        self.extract(read_memory_10b_10k, read_memory_base, &[read_memory_byte]);
        self.extract(read_memory_1Mib_10k, read_memory_byte, &[]);
        self.extract(write_register_10b_10k, write_register_base, &[write_register_byte]);
        self.extract(write_register_1Mib_10k, write_register_byte, &[]);
        self.extract(read_register_10b_10k, read_register_base, &[read_register_byte]);
        self.extract(read_register_1Mib_10k, read_register_byte, &[]);
        self.extract(write_memory_10b_10k, write_memory_base, &[write_memory_byte]);
        self.extract(write_memory_1Mib_10k, write_memory_byte, &[]);

        self.result.insert(log_base, 0f64);
        self.result.insert(log_byte, 0f64);
        self.extract(utf8_log_10b_10k, utf8_decoding_base, &[utf8_decoding_byte]);
        // Charge the maximum between non-nul-terminated and nul-terminated costs.
        let utf8_byte = self.extract_value(utf8_log_10kib_10k, utf8_decoding_byte, &[]);
        let nul_utf8_byte = self.extract_value(nul_utf8_log_10kib_10k, utf8_decoding_byte, &[]);
        self.result.insert(utf8_decoding_byte, utf8_byte.max(nul_utf8_byte));
        self.extract(utf16_log_10b_10k, utf16_decoding_base, &[utf16_decoding_byte]);
        // Charge the maximum between non-nul-terminated and nul-terminated costs.
        let utf16_byte = self.extract_value(utf16_log_10kib_10k, utf16_decoding_byte, &[]);
        let nul_utf16_byte = self.extract_value(nul_utf16_log_10kib_10k, utf16_decoding_byte, &[]);
        self.result.insert(utf16_decoding_byte, utf16_byte.max(nul_utf16_byte));

        self.extract(sha256_10b_10k, sha256_base, &[sha256_byte]);
        self.extract(sha256_10kib_10k, sha256_byte, &[]);

        // TODO: Redo storage costs once we have counting of nodes and we have size peek.
        self.extract(
            storage_write_10b_key_10b_value_1k,
            storage_write_base,
            &[storage_write_key_byte, storage_write_value_byte, storage_write_evicted_byte],
        );
        self.extract(
            storage_write_10kib_key_10b_value_1k,
            storage_write_key_byte,
            &[storage_write_value_byte, storage_write_evicted_byte],
        );
        self.extract(
            storage_write_10b_key_10kib_value_1k,
            storage_write_value_byte,
            &[storage_write_evicted_byte],
        );
        self.extract(storage_write_10b_key_10kib_value_1k_evict, storage_write_evicted_byte, &[]);
        self.extract(
            storage_read_10b_key_10b_value_1k,
            storage_read_base,
            &[storage_read_key_byte, storage_read_value_byte],
        );
        self.extract(
            storage_read_10kib_key_10b_value_1k,
            storage_read_key_byte,
            &[storage_read_value_byte],
        );
        self.extract(storage_read_10b_key_10kib_value_1k, storage_read_value_byte, &[]);
        self.extract(
            storage_remove_10b_key_10b_value_1k,
            storage_remove_base,
            &[storage_remove_key_byte, storage_remove_ret_value_byte],
        );
        self.extract(
            storage_remove_10kib_key_10b_value_1k,
            storage_remove_key_byte,
            &[storage_remove_ret_value_byte],
        );
        self.extract(storage_remove_10b_key_10kib_value_1k, storage_remove_ret_value_byte, &[]);
        self.extract(
            storage_has_key_10b_key_10b_value_1k,
            storage_has_key_base,
            &[storage_has_key_byte],
        );
        self.extract(storage_has_key_10kib_key_10b_value_1k, storage_has_key_byte, &[]);

        self.extract(
            storage_iter_prefix_10b_1k,
            storage_iter_create_prefix_base,
            &[storage_iter_create_prefix_byte],
        );
        self.extract(storage_iter_prefix_10kib_1k, storage_iter_create_prefix_byte, &[]);
        self.extract(
            storage_iter_range_10b_from_10b_to_1k,
            storage_iter_create_range_base,
            &[storage_iter_create_from_byte, storage_iter_create_to_byte],
        );
        self.extract(
            storage_iter_range_10kib_from_10b_to_1k,
            storage_iter_create_from_byte,
            &[storage_iter_create_to_byte],
        );
        self.extract(storage_iter_range_10b_from_10kib_to_1k, storage_iter_create_to_byte, &[]);

        self.extract(
            storage_next_10b_from_10b_to_1k_10b_key_10b_value,
            storage_iter_next_base,
            &[storage_iter_next_key_byte, storage_iter_next_value_byte],
        );
        self.extract(
            storage_next_10kib_from_10b_to_1k_10b_key_10b_value,
            storage_iter_next_key_byte,
            &[storage_iter_next_value_byte],
        );
        self.extract(
            storage_next_10b_from_10kib_to_1k_10b_key_10b_value,
            storage_iter_next_value_byte,
            &[],
        );

        self.extract(promise_and_100k, promise_and_base, &[promise_and_per_promise]);
        self.extract(promise_and_100k_on_1k_and, promise_and_per_promise, &[]);
        self.extract(promise_return_100k, promise_return, &[]);
        self.result.clone()
    }
}
impl std::fmt::Display for ExtCostsGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (k, v) in &self.agg {
            writeln!(f, "{:?}\t\t\t\t{}", k, v)?;
        }
        Ok(())
    }
}
