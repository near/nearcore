use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig, StorageUsageConfig,
};
#[cfg(feature = "protocol_feature_evm")]
use near_runtime_fees::{
    EvmBls12ConstOpCost, EvmBn128PairingCost, EvmCostConfig, EvmLinearCost, EvmModexpCost,
    EvmPrecompileCostConfig,
};
use node_runtime::config::RuntimeConfig;
use num_rational::Rational;
use rand::{thread_rng, RngCore};
use std::convert::TryInto;

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
            #[cfg(feature = "protocol_feature_evm")]
            evm_config: EvmCostConfig {
                bootstrap_cost: rng.next_u64() % 1000,
                deploy_cost_per_evm_gas: rng.next_u64() % 1000,
                deploy_cost_per_byte: rng.next_u64() % 1000,
                funcall_cost_base: rng.next_u64() % 1000,
                funcall_cost_per_evm_gas: rng.next_u64() % 1000,
                precompile_costs: EvmPrecompileCostConfig {
                    ecrecover_cost: EvmLinearCost {
                        base: rng.next_u64() % 1000,
                        word: rng.next_u64() % 1000,
                    },
                    sha256_cost: EvmLinearCost {
                        base: rng.next_u64() % 1000,
                        word: rng.next_u64() % 1000,
                    },
                    ripemd160_cost: EvmLinearCost {
                        base: rng.next_u64() % 1000,
                        word: rng.next_u64() % 1000,
                    },
                    identity_cost: EvmLinearCost {
                        base: rng.next_u64() % 1000,
                        word: rng.next_u64() % 1000,
                    },
                    modexp_cost: EvmModexpCost { divisor: rng.next_u64() % 1000 + 1 },
                    bn128_add_cost: EvmBls12ConstOpCost { price: rng.next_u64() % 1000 },
                    bn128_mul_cost: EvmBls12ConstOpCost { price: rng.next_u64() % 1000 },
                    bn128_pairing_cost: EvmBn128PairingCost {
                        base: rng.next_u64() % 1000,
                        pair: rng.next_u64() % 1000,
                    },
                    blake2f_cost: rng.next_u64() % 1000,
                },
            },
            #[cfg(feature = "protocol_feature_evm")]
            evm_deposit: (rng.next_u64() % 10000) as u128 * 10u128.pow(23),
        },
        ..Default::default()
    }
}

#[test]
fn test_random_fees() {
    assert_ne!(random_config().transaction_costs, random_config().transaction_costs);
}
