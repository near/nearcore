use near_primitives::num_rational::Rational;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig, StorageUsageConfig,
};
use rand::{thread_rng, RngCore};

pub fn random_config() -> RuntimeConfig {
    let mut rng = thread_rng();
    let mut random_fee = || Fee {
        send_sir: rng.next_u64() % 1000,
        send_not_sir: rng.next_u64() % 1000,
        execution: rng.next_u64() % 1000,
    };
    RuntimeConfig {
        transaction_costs: RuntimeFeesConfig {
            action_receipt_creation_config: random_fee(),
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: random_fee(),
                cost_per_byte: random_fee(),
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: random_fee(),
                deploy_contract_cost: random_fee(),
                deploy_contract_cost_per_byte: random_fee(),
                function_call_cost: random_fee(),
                function_call_cost_per_byte: random_fee(),
                transfer_cost: random_fee(),
                stake_cost: random_fee(),
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: random_fee(),
                    function_call_cost: random_fee(),
                    function_call_cost_per_byte: random_fee(),
                },
                delete_key_cost: random_fee(),
                delete_account_cost: random_fee(),
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: rng.next_u64() % 10000,
                num_extra_bytes_record: rng.next_u64() % 10000,
            },
            burnt_gas_reward: Rational::new((rng.next_u32() % 100).try_into().unwrap(), 100),
            pessimistic_gas_price_inflation_ratio: Rational::new(
                (101 + rng.next_u32() % 10).try_into().unwrap(),
                100,
            ),
        },
        ..RuntimeConfig::test()
    }
}

#[test]
fn test_random_fees() {
    assert_ne!(random_config().transaction_costs, random_config().transaction_costs);
}
