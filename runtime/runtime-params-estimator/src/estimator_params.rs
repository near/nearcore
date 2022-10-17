//! Some parameters are use within the estimator to transform measurements to gas costs.
//! These parameters have been estimated manually and are now hard-coded for a more deterministic estimation of runtime parameters.
//! This module contains the hard-coded constants as well as the code to manually re-estimate them.

use near_primitives::{hash::CryptoHash, types::Gas};
use num_rational::Ratio;

use crate::{config::GasMetric, gas_cost::GasCost};

// All constant below are measured in Gas, respectively, in fractions thereof.

/// How much gas there is in a nanosecond worth of computation.
pub(crate) const GAS_IN_NS: Ratio<Gas> = Ratio::new_raw(1_000_000, 1);
// We use factor of 8 to approximately match the price of SHA256 operation between
// time-based and icount-based metric as measured on 3.2Ghz Core i5.
pub(crate) const GAS_IN_INSTR: Ratio<Gas> = Ratio::new_raw(1_000_000, 8);

// IO bytes as measured on the sys_call level are rather unstable when using
// RocksDB as storage solution. Measuring it for debugging purposes is still useful
// but a conversion from total read/written bytes to gas is always going to be inaccurate.
// Consequently, set both values to 0 such that they do not influence gas costs.
pub(crate) const IO_READ_BYTE_COST: Ratio<Gas> = Ratio::new_raw(0, 1);
pub(crate) const IO_WRITE_BYTE_COST: Ratio<Gas> = Ratio::new_raw(0, 1);

/// Measure the cost for running a sha256 Rust implementation (on an arbitrary input).
///
/// This runs outside the WASM runtime and is intended to measure the overall hardware capabilities of the test system.
/// (The motivation is to stay as close as possible to original estimations done with this code:
/// <https://github.com/near/calibrator/blob/c6fbb170a905fbc630ebd84ebc97f7226ec87ead/src/main.rs#L8-L21>)
pub(crate) fn sha256_cost(metric: GasMetric, repeats: u64) -> GasCost {
    let cpu = measure_operation(repeats, metric, exec_sha256);
    let cpu_per_rep = cpu / repeats;
    cpu_per_rep
}

fn exec_sha256(repeats: u64) -> i64 {
    let mut result = 0;
    for index in 0..repeats {
        let input = "what should I do but tend upon the hours, and times of your desire";
        let val = CryptoHash::hash_bytes(input.as_bytes());
        assert_eq!(val.to_string(), "BTEVNkcDtaui6SJC19Efnrf7A3dCKT9v8y24NiC3S7RR");
        result += val.as_bytes()[(index % 32) as usize] as i64;
    }
    result
}

#[used]
static mut SINK: i64 = 0;

fn measure_operation<F: FnOnce(u64) -> i64>(count: u64, metric: GasMetric, op: F) -> GasCost {
    let start = GasCost::measure(metric);
    let value = op(count);
    let result = start.elapsed();
    unsafe {
        SINK = value;
    }
    result
}
