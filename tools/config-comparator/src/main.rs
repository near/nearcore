use std::collections::HashSet;
use std::io::Result;
use std::iter::FromIterator;
use std::path::Path;

use clap::{App, Arg};

use near_primitives::runtime::config::{RuntimeConfig, RuntimeFeesConfig, AccountCreationConfig};
use near_primitives::runtime::config::ActionCreationConfig;
use near_primitives::runtime::fees::{RuntimeFeesConfig, Fee, DataReceiptCreationConfig, ActionCreationConfig, AccessKeyCreationConfig, StorageUsageConfig};
use near_primitives::num_rational::{Ratio, Rational};
use near_primitives::config::{VMConfig, VMLimitConfig, ExtCostsConfig};

fn main() -> Result<()> {
    // Script to verify that receipts being restored after apply_chunks fix were actually created.
    // Because receipt hashes are unique, we only check for their presence.
    // See https://github.com/near/nearcore/pull/4248/ for more details.
    // Requirement: mainnet archival node dump.

    eprintln!("config-comparator started");

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
            evm_config: Default::default(),
            evm_deposit: 0
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
                alt_bn128_g1_multiexp_base: 0,
                alt_bn128_g1_multiexp_byte: 0,
                alt_bn128_g1_sum_base: 0,
                alt_bn128_g1_sum_byte: 0,
                alt_bn128_g1_multiexp_sublinear: 0,
                alt_bn128_pairing_check_base: 0,
                alt_bn128_pairing_check_byte: 0
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
    eprintln!("{:?}", config);

    // let default_home = get_default_home();
    // let matches = App::new("restored-receipts-verifier")
    //     .arg(
    //         Arg::new("home")
    //             .default_value(&default_home)
    //             .about("Directory for config and data (default \"~/.near\")")
    //             .takes_value(true),
    //     )
    //     .get_matches();
    //
    // let shard_id = 0u64;
    // let home_dir = matches.value_of("home").map(Path::new).unwrap();
    // let near_config = load_config(&home_dir);
    // let store = create_store(&get_store_path(&home_dir));
    // let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    // let runtime = NightshadeRuntime::new(
    //     &home_dir,
    //     store,
    //     &near_config.genesis,
    //     near_config.client_config.tracked_accounts.clone(),
    //     near_config.client_config.tracked_shards.clone(),
    //     None,
    //     near_config.client_config.max_gas_burnt_view,
    // );
    //
    // let mut receipts_missing = Vec::<Receipt>::new();
    // let height_first: u64 = 34691244; // First height for which lost receipts were found
    // let height_last: u64 = 35524259; // Height for which apply_chunks was already fixed
    //
    // eprintln!("Collecting missing receipts from blocks...");
    // for height in height_first..height_last {
    //     let block_hash_result = chain_store.get_block_hash_by_height(height);
    //     let block_hash = match block_hash_result {
    //         Ok(it) => it,
    //         Err(_) => {
    //             eprintln!("{} does not exist, skip", height);
    //             continue;
    //         }
    //     };
    //
    //     let block = chain_store.get_block(&block_hash).unwrap().clone();
    //     if block.chunks()[shard_id as usize].height_included() == height {
    //         eprintln!("{} included, skip", height);
    //         continue;
    //     }
    //
    //     let chunk_extra =
    //         chain_store.get_chunk_extra(block.header().prev_hash(), shard_id).unwrap().clone();
    //     let apply_result = runtime
    //         .apply_transactions(
    //             shard_id,
    //             chunk_extra.state_root(),
    //             block.header().height(),
    //             block.header().raw_timestamp(),
    //             block.header().prev_hash(),
    //             &block.hash(),
    //             &[],
    //             &[],
    //             chunk_extra.validator_proposals(),
    //             block.header().gas_price(),
    //             chunk_extra.gas_limit(),
    //             &block.header().challenges_result(),
    //             *block.header().random_value(),
    //             false,
    //             false, // because fix was not applied in for the blocks analyzed here
    //             None,
    //         )
    //         .unwrap();
    //
    //     let receipts_missing_after_apply: Vec<Receipt> =
    //         apply_result.receipt_result.values().cloned().into_iter().flatten().collect();
    //     receipts_missing.extend(receipts_missing_after_apply.into_iter());
    //     eprintln!("{} applied", height);
    // }
    //
    // let receipt_hashes_missing =
    //     receipts_missing.into_iter().map(|receipt| receipt.get_hash()).collect();
    //
    // eprintln!("Verifying receipt hashes...");
    // let (receipt_hashes_not_verified, receipt_hashes_still_missing) =
    //     get_differences_with_hashes_from_repo(receipt_hashes_missing);
    // assert!(
    //     receipt_hashes_not_verified.is_empty(),
    //     "Some of receipt hashes in repo were not verified successfully: {:?}",
    //     receipt_hashes_not_verified
    // );
    // assert!(
    //     receipt_hashes_still_missing.is_empty(),
    //     "Some of receipt hashes in repo are probably still not applied: {:?}",
    //     receipt_hashes_still_missing
    // );
    //
    // eprintln!("Receipt hashes in repo were verified successfully!");

    Ok(())
}

