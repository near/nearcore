use near_parameters::{Fee, RuntimeConfig, RuntimeFeesConfig, StorageUsageConfig};
use near_primitives::num_rational::Rational32;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use rand::{Rng, RngCore, thread_rng};

pub fn random_config() -> RuntimeConfig {
    let mut rng = thread_rng();
    let mut random_fee = || Fee {
        send_sir: rng.next_u64() % 1000,
        send_not_sir: rng.next_u64() % 1000,
        execution: rng.next_u64() % 1000,
    };
    RuntimeConfig {
        fees: std::sync::Arc::new(RuntimeFeesConfig {
            action_fees: enum_map::enum_map! {
                _ => random_fee(),
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: rng.next_u64() % 10000,
                num_extra_bytes_record: rng.next_u64() % 10000,
                storage_amount_per_byte: rng.next_u64() as u128,
                global_contract_storage_amount_per_byte: rng.next_u64() as u128,
            },
            burnt_gas_reward: Rational32::new((rng.next_u32() % 100).try_into().unwrap(), 100),
            pessimistic_gas_price_inflation_ratio: Rational32::new(
                (101 + rng.next_u32() % 10).try_into().unwrap(),
                100,
            ),
            refund_gas_price_changes: !ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION),
            gas_refund_penalty: Rational32::new(rng.gen_range(0..=i32::MAX), i32::MAX),
            min_gas_refund_penalty: rng.next_u64(),
        }),
        ..RuntimeConfig::test()
    }
}

#[test]
fn test_random_fees() {
    assert_ne!(random_config().fees, random_config().fees);
}
