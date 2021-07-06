use std::collections::{HashSet, HashMap};
use std::io::Result;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use near_primitives::runtime::config::{RuntimeConfig, AccountCreationConfig};
use near_primitives::runtime::fees::{RuntimeFeesConfig, Fee, DataReceiptCreationConfig, ActionCreationConfig, AccessKeyCreationConfig, StorageUsageConfig};
use near_primitives::num_rational::{Ratio, Rational};
use near_primitives::config::{VMConfig, VMLimitConfig, ExtCostsConfig};
use near_primitives::types::AccountId;
use serde_json::{Value, Number};
use std::fs::File;
use std::io::Write;

/// Compares JSON object for approximate equality.
/// You can use `[..]` wildcard in strings (useful for OS dependent things such
/// as paths). You can use a `"{...}"` string literal as a wildcard for
/// arbitrary nested JSON. Arrays are sorted before comparison.
fn find_diff<'a>(previous: &'a Value, current: &'a Value, key: String, diff: &mut HashMap<String, f64>) {
    match (previous, current) {
        (Value::Number(l), Value::Number(r)) => {
            let l = l.as_f64().unwrap();
            let r = r.as_f64().unwrap();
            let numer = (r - l);
            let denom = l;
            diff.insert(key, if denom != 0f64 { numer / denom } else { 0f64 });
        },
        (Value::Object(l), Value::Object(r)) => {
            fn sorted_key_values(obj: &serde_json::Map<String, Value>) -> Vec<(&String, &Value)> {
                let mut entries = obj.iter().collect::<Vec<_>>();
                entries.sort_by_key(|it| it.0);
                entries
            }

            if l.len() != r.len() {
                panic!("Different length of nested keys list found on the key {}: l.len() = {}, r.len() = {}", key, l.len(), r.len());
            }

            let l = sorted_key_values(l);
            let r = sorted_key_values(r);

            l.into_iter().zip(r).for_each(|(l, r)| {
                if l.0 != r.0 {
                    panic!("Different nested keys found on the key {}: l = {}, r = {}", key, l.0, r.0);
                };
                find_diff(l.1, r.1, key.clone() + "|" + l.0, diff);
            });
        },
        (Value::Bool(_), Value::Bool(_)) => {},
        (Value::String(_), Value::String(_)) => {},
        (Value::Null, Value::Null) => {},
        (Value::Array(_), Value::Array(_)) => {},
        (l, r) => panic!("Values structure did not match on the key {}: l = {}, r = {}", key, l, r),
    }
}

fn main() -> Result<()> {
    // Script to verify that receipts being restored after apply_chunks fix were actually created.
    // Because receipt hashes are unique, we only check for their presence.
    // See https://github.com/near/nearcore/pull/4248/ for more details.
    // Requirement: mainnet archival node dump.

    eprintln!("config-comparator started");

    const SAFETY_MULTIPLIER: u64 = 3;
    let config = RuntimeConfig::default();
    let config_2 = RuntimeConfig {
        storage_amount_per_byte: 90900000000000000000,
        transaction_costs: RuntimeFeesConfig {
            action_receipt_creation_config: Fee {
                send_sir: 193297500000,
                send_not_sir: 193297500000,
                execution: 193297500000,
            },
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: Fee {
                    send_sir: 2483023417000,
                    send_not_sir: 2483023417000,
                    execution: 2483023417000,
                },
                cost_per_byte: Fee {
                    send_sir: 14020958,
                    send_not_sir: 14020958,
                    execution: 14020958,
                },
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: Fee {
                    send_sir: 21628500000,
                    send_not_sir: 21628500000,
                    execution: 21628500000,
                },
                deploy_contract_cost: Fee {
                    send_sir: 66516500000,
                    send_not_sir: 66516500000,
                    execution: 67151328394,
                },
                deploy_contract_cost_per_byte: Fee {
                    send_sir: 7628263,
                    send_not_sir: 7628263,
                    execution: 7628263,
                },
                function_call_cost: Fee {
                    send_sir: 533960000000,
                    send_not_sir: 533960000000,
                    execution: 533960000000,
                },
                function_call_cost_per_byte: Fee {
                    send_sir: 213706,
                    send_not_sir: 213706,
                    execution: 213706,
                },
                transfer_cost: Fee {
                    send_sir: 26002500000,
                    send_not_sir: 26002500000,
                    execution: 26002500000,
                },
                stake_cost: Fee {
                    send_sir: 58593500000,
                    send_not_sir: 58593500000,
                    execution: 58593500000,
                },
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: Fee {
                        send_sir: 35019500000,
                        send_not_sir: 35019500000,
                        execution: 35019500000,
                    },
                    function_call_cost: Fee {
                        send_sir: 40925500000,
                        send_not_sir: 40925500000,
                        execution: 40925500000,
                    },
                    function_call_cost_per_byte: Fee {
                        send_sir: 13192350,
                        send_not_sir: 13192350,
                        execution: 13192350,
                    },
                },
                delete_key_cost: Fee {
                    send_sir: 15688000000,
                    send_not_sir: 15688000000,
                    execution: 15688000000,
                },
                delete_account_cost: Fee {
                    send_sir: 167912500000,
                    send_not_sir: 167912500000,
                    execution: 167912500000,
                },
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: 100,
                num_extra_bytes_record: 40,
            },
            burnt_gas_reward: Rational::new(3, 10),
            pessimistic_gas_price_inflation_ratio: Rational::new(103, 100),
        },
        wasm_config: VMConfig {
            ext_costs: ExtCostsConfig {
                base: 69038876,
                contract_compile_base: 0,
                contract_compile_bytes: 0,
                read_memory_base: 243552600,
                read_memory_byte: 739299,
                write_memory_base: 289972100,
                write_memory_byte: 136991,
                read_register_base: 176228200,
                read_register_byte: 59484,
                write_register_base: 329128400,
                write_register_byte: 739097,
                utf8_decoding_base: 401454600,
                utf8_decoding_byte: 99224663,
                utf16_decoding_base: 558814400,
                utf16_decoding_byte: 56497872,
                sha256_base: 956819700,
                sha256_byte: 7073294,
                keccak256_base: 1284357300,
                keccak256_byte: 5121132,
                keccak512_base: 1219474900,
                keccak512_byte: 8907202,
                ripemd160_base: 0,
                ripemd160_block: 0,
                ecrecover_base: 0,
                log_base: 558814400,
                log_byte: 4727121,
                storage_write_base: 13897636000,
                storage_write_key_byte: 20440405,
                storage_write_value_byte: 71995908,
                storage_write_evicted_byte: 157924922,
                storage_read_base: 32459340000,
                storage_read_key_byte: 279087046,
                storage_read_value_byte: 252352473,
                storage_remove_base: 44978364000,
                storage_remove_key_byte: 308887274,
                storage_remove_ret_value_byte: 268935884,
                storage_has_key_base: 29022143000,
                storage_has_key_byte: 259823087,
                storage_iter_create_prefix_base: 0,
                storage_iter_create_prefix_byte: 0,
                storage_iter_create_range_base: 0,
                storage_iter_create_from_byte: 0,
                storage_iter_create_to_byte: 0,
                storage_iter_next_base: 0,
                storage_iter_next_key_byte: 0,
                storage_iter_next_value_byte: 0,
                touching_trie_node: 9274097142,
                promise_and_base: 682944970,
                promise_and_per_promise: 3922886,
                promise_return: 331636390,
                validator_stake_base: 303944908800,
                validator_total_stake_base: 303944908800,
            },
            grow_mem_cost: 1,
            regular_op_cost: 1093191,
            limit_config: VMLimitConfig {
                max_gas_burnt: 200000000000000,
                max_gas_burnt_view: 200000000000000,
                max_stack_height: 16384,
                initial_memory_pages: 1024,
                max_memory_pages: 2048,
                registers_memory_limit: 1073741824,
                max_register_size: 104857600,
                max_number_registers: 100,
                max_number_logs: 100,
                max_total_log_length: 16384,
                max_total_prepaid_gas: 300000000000000,
                max_actions_per_receipt: 100,
                max_number_bytes_method_names: 2000,
                max_length_method_name: 256,
                max_arguments_length: 4194304,
                max_length_returned_data: 4194304,
                max_contract_size: 4194304,
                max_transaction_size: 0,
                max_length_storage_key: 4194304,
                max_length_storage_value: 4194304,
                max_promises_per_function_call_action: 1024,
                max_number_input_data_dependencies: 128,
            },
        },
        account_creation_config: AccountCreationConfig {
            min_allowed_top_level_account_length: 0,
            registrar_account_id: AccountId::from("registrar"),
        },
    };
    let mut config_3 = RuntimeConfig {
        storage_amount_per_byte: 90900000000000000000,
        transaction_costs: RuntimeFeesConfig {
            action_receipt_creation_config: Fee {
                send_sir: 164973625000,
                send_not_sir: 164973625000,
                execution: 164973625000,
            },
            data_receipt_creation_config: DataReceiptCreationConfig {
                base_cost: Fee {
                    send_sir: 50745691155500,
                    send_not_sir: 50745691155500,
                    execution: 50745691155500,
                },
                cost_per_byte: Fee {
                    send_sir: 13895993,
                    send_not_sir: 13895993,
                    execution: 13895993,
                },
            },
            action_creation_config: ActionCreationConfig {
                create_account_cost: Fee {
                    send_sir: 30911812500,
                    send_not_sir: 30911812500,
                    execution: 30911812500,
                },
                deploy_contract_cost: Fee {
                    send_sir: 52376500000,
                    send_not_sir: 52376500000,
                    execution: 52826825505,
                },
                deploy_contract_cost_per_byte: Fee {
                    send_sir: 8298561,
                    send_not_sir: 8298561,
                    execution: 2333358119803,
                },
                function_call_cost: Fee {
                    send_sir: 257123312500,
                    send_not_sir: 257123312500,
                    execution: 257123312500,
                },
                function_call_cost_per_byte: Fee {
                    send_sir: 31498,
                    send_not_sir: 31498,
                    execution: 31498,
                },
                transfer_cost: Fee {
                    send_sir: 32271375000,
                    send_not_sir: 32271375000,
                    execution: 32271375000,
                },
                stake_cost: Fee {
                    send_sir: 63955375000,
                    send_not_sir: 63955375000,
                    execution: 63955375000,
                },
                add_key_cost: AccessKeyCreationConfig {
                    full_access_cost: Fee {
                        send_sir: 32792187500,
                        send_not_sir: 32792187500,
                        execution: 32792187500,
                    },
                    function_call_cost: Fee {
                        send_sir: 32156250000,
                        send_not_sir: 32156250000,
                        execution: 32156250000,
                    },
                    function_call_cost_per_byte: Fee {
                        send_sir: 12654743,
                        send_not_sir: 12654743,
                        execution: 12654743,
                    },
                },
                delete_key_cost: Fee {
                    send_sir: 16654750000,
                    send_not_sir: 16654750000,
                    execution: 16654750000,
                },
                delete_account_cost: Fee {
                    send_sir: 135365687500,
                    send_not_sir: 135365687500,
                    execution: 135365687500,
                },
            },
            storage_usage_config: StorageUsageConfig {
                num_bytes_account: 100,
                num_extra_bytes_record: 40,
            },
            burnt_gas_reward: Rational::new(3, 10),
            pessimistic_gas_price_inflation_ratio: Rational::new(103, 100),
        },
        wasm_config: VMConfig {
            ext_costs: ExtCostsConfig {
                base: 86192651,
                contract_compile_base: 0,
                contract_compile_bytes: 0,
                read_memory_base: 240583625,
                read_memory_byte: 1266996,
                write_memory_base: 299679375,
                write_memory_byte: 61639,
                read_register_base: 208015000,
                read_register_byte: 32267,
                write_register_base: 331427625,
                write_register_byte: 1267082,
                utf8_decoding_base: 415690312,
                utf8_decoding_byte: 98402985,
                utf16_decoding_base: 562920137,
                utf16_decoding_byte: 54853932,
                sha256_base: 985787500,
                sha256_byte: 9533871,
                keccak256_base: 1312011187,
                keccak256_byte: 7064781,
                keccak512_base: 1305269762,
                keccak512_byte: 12106739,
                ripemd160_base: 0,
                ripemd160_block: 0,
                ecrecover_base: 0,
                log_base: 562920137,
                log_byte: 4108400,
                storage_write_base: 17176720250,
                storage_write_key_byte: 26416770,
                storage_write_value_byte: 2044093784,
                storage_write_evicted_byte: 3118598072,
                storage_read_base: 26620283375,
                storage_read_key_byte: 5451228865,
                storage_read_value_byte: 5217699173,
                storage_remove_base: 31537079375,
                storage_remove_key_byte: 5895304107,
                storage_remove_ret_value_byte: 5429953409,
                storage_has_key_base: 24861784375,
                storage_has_key_byte: 5048138526,
                storage_iter_create_prefix_base: 0,
                storage_iter_create_prefix_byte: 0,
                storage_iter_create_range_base: 0,
                storage_iter_create_from_byte: 0,
                storage_iter_create_to_byte: 0,
                storage_iter_next_base: 0,
                storage_iter_next_key_byte: 0,
                storage_iter_next_value_byte: 0,
                touching_trie_node: 7605795250,
                promise_and_base: 5259182120,
                promise_and_per_promise: 43190415,
                promise_return: 4631026297,
                validator_stake_base: 303944908800,
                validator_total_stake_base: 303944908800,
            },
            grow_mem_cost: 1,
            regular_op_cost: 1310769,
            limit_config: VMLimitConfig {
                max_gas_burnt: 200000000000000,
                max_gas_burnt_view: 200000000000000,
                max_stack_height: 16384,
                initial_memory_pages: 1024,
                max_memory_pages: 2048,
                registers_memory_limit: 1073741824,
                max_register_size: 104857600,
                max_number_registers: 100,
                max_number_logs: 100,
                max_total_log_length: 16384,
                max_total_prepaid_gas: 300000000000000,
                max_actions_per_receipt: 100,
                max_number_bytes_method_names: 2000,
                max_length_method_name: 256,
                max_arguments_length: 4194304,
                max_length_returned_data: 4194304,
                max_contract_size: 4194304,
                max_transaction_size: 0,
                max_length_storage_key: 4194304,
                max_length_storage_value: 4194304,
                max_promises_per_function_call_action: 1024,
                max_number_input_data_dependencies: 128,
            },
        },
        account_creation_config: AccountCreationConfig {
            min_allowed_top_level_account_length: 0,
            registrar_account_id: AccountId::from("registrar"),
        },
    };

    let str = serde_json::to_string_pretty(&config)
        .expect("Failed serializing the runtime config");
    let mut file =
        File::create("/tmp/data/runtime_config_old.json").expect("Failed to create file");
    if let Err(err) = file.write_all(str.as_bytes()) {
        panic!("Failed to write runtime config to file {}", err);
    }

    // eprintln!("{:?}", config_2);
    let value_1 = serde_json::to_value(config).unwrap();
    let mut value_2 = serde_json::to_value(config_2).unwrap();
    eprintln!("{:?}", value_2["wasm_config"]["ext_costs"]);
    if let Value::Object(ext_costs) = &mut value_2["wasm_config"]["ext_costs"] {
        for (k, v) in ext_costs.clone().iter() {
            if let Value::Number(x) = v {
                let y = x.as_u64().unwrap();
                ext_costs.insert(k.clone(), Value::Number(Number::from(SAFETY_MULTIPLIER * y)));
            };
        }
    };
    // eprintln!("{:?}", value_2);
    let mut diff = Default::default();
    find_diff(&value_1, &value_2, String::new(), &mut diff);
    // eprintln!("{:?}", diff);
    let mut x = diff.into_iter().collect::<Vec<_>>();
    x.sort_by(|(_, v1), (_, v2)| v1.partial_cmp(v2).unwrap());
    for i in x.iter() {
        eprintln!("{:?}", i);
    }

    Ok(())
}

