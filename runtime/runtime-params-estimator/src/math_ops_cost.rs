#[cfg(feature = "protocol_feature_math_extension")]
mod math_ops_cost {
    use crate::cases::{ratio_to_gas, ratio_to_gas_signed};
    use crate::testbed_runners::{end_count, start_count, GasMetric};
    use crate::vm_estimator::least_squares_method;
    use near_primitives::num_rational::Ratio;
    use near_vm_logic::{ecrecover, ripemd160};

    #[derive(Debug)]
    pub struct EvmMathCosts {
        ripemd160_base: i64,
        ripemd160_per_byte: i64,
        ecrecover_base: i64,
    }

    #[used]
    static mut SINK: i64 = 0;

    fn measure_operation<F: FnOnce(usize) -> i64>(
        gas_metric: GasMetric,
        count: usize,
        op: F,
    ) -> u64 {
        let start = start_count(gas_metric);
        let result = op(count);
        let end = end_count(gas_metric, &start);
        // Do this to ensure call to measurer isn't optimized away.
        unsafe {
            SINK = result;
        }
        end
    }

    fn compute_ripemd160_cost(gas_metric: GasMetric) -> (i64, i64) {
        let mut xs = vec![];
        let mut ys = vec![];
        for i in vec![3, 30, 300, 30000] {
            xs.push(i as u64);
            ys.push(measure_operation(gas_metric, i, |repeat| {
                let value = vec![1u8; repeat];
                let digest = ripemd160(&value);
                digest.as_slice()[0] as i64
            }));
        }

        let (a, b, _err) = least_squares_method(&xs, &ys);
        (ratio_to_gas_signed(gas_metric, a), ratio_to_gas_signed(gas_metric, b))
    }

    fn compute_ecrecover_base(gas_metric: GasMetric) -> i64 {
        let cost = measure_operation(gas_metric, 1, |_| do_ecrecover());
        ratio_to_gas(gas_metric, Ratio::new(cost, 1)) as i64
    }

    fn do_ecrecover() -> i64 {
        let hash: [u8; 32] = [
            0x7d, 0xba, 0xf5, 0x58, 0xb0, 0xa1, 0xa5, 0xdc, 0x7a, 0x67, 0x20, 0x21, 0x17, 0xab,
            0x14, 0x3c, 0x1d, 0x86, 0x05, 0xa9, 0x83, 0xe4, 0xa7, 0x43, 0xbc, 0x06, 0xfc, 0xc0,
            0x31, 0x62, 0xdc, 0x0d,
        ];
        let signature: [u8; 65] = [
            0x5d, 0x99, 0xb6, 0xf7, 0xf6, 0xd1, 0xf7, 0x3d, 0x1a, 0x26, 0x49, 0x7f, 0x2b, 0x1c,
            0x89, 0xb2, 0x4c, 0x09, 0x93, 0x91, 0x3f, 0x86, 0xe9, 0xa2, 0xd0, 0x2c, 0xd6, 0x98,
            0x87, 0xd9, 0xc9, 0x4f, 0x3c, 0x88, 0x03, 0x58, 0x57, 0x9d, 0x81, 0x1b, 0x21, 0xdd,
            0x1b, 0x7f, 0xd9, 0xbb, 0x01, 0xc1, 0xd8, 0x1d, 0x10, 0xe6, 0x9f, 0x03, 0x84, 0xe6,
            0x75, 0xc3, 0x2b, 0x39, 0x64, 0x3b, 0xe8, 0x92, /* version */ 0,
        ];
        let result = ecrecover(signature, hash, 0).unwrap();
        result.as_ref()[0] as i64
    }

    pub fn cost_of_math(gas_metric: GasMetric) -> EvmMathCosts {
        #[cfg(debug_assertions)]
        println!("WARNING: must run in release mode to provide accurate results!");
        let (ripemd160_base, ripemd160_per_byte) = compute_ripemd160_cost(gas_metric);
        let ecrecover_base = compute_ecrecover_base(gas_metric);
        EvmMathCosts { ripemd160_base, ripemd160_per_byte, ecrecover_base }
    }

    #[test]
    fn test_math_cost_time() {
        // cargo test --release --features protocol_feature_math_extension \
        //  --package runtime-params-estimator \
        // --lib math_ops_cost::math_ops_cost::test_math_cost_time \
        // -- --exact --nocapture
        println!("{:?}", cost_of_math(GasMetric::Time));
    }

    #[test]
    fn test_math_cost_icount() {
        // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh cargo test --release \
        // --features protocol_feature_math_extension \
        // --package runtime-params-estimator \
        // --lib math_ops_cost::math_ops_cost::test_math_cost_icount \
        // -- --exact --nocapture
        // Where runner.sh is
        // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
        // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
        println!("{:?}", cost_of_math(GasMetric::ICount));
    }
}
