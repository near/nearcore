//! Code to estimate each specific cost.
//!
//! Each cost is registered in `ALL_COSTS` array, together with a
//! cost-estimation function. Function can be arbitrary. Most, but not all,
//! costs are estimated using roughly the following algorithm:
//!
//!   * Create an instance of near starting with specific fixture with many
//!     accounts deployed.
//!   * Create a template transaction, like transfer from a to b.
//!   * Create a bunch of blocks stuffed with copies of this template
//!     transaction.
//!   * Measure the "time" it takes to process the blocks (including all
//!     generated receipts)
//!   * Divide the total time by number of blocks * number of transactions in a
//!     block.
//!
//! Some common variations:
//!
//!   * As we measure "the whole thing", we deduct base costs from composite
//!     measurements. For example, when estimating cost of transfer action we
//!     deduct the cost of an empty transaction, as that is accounted for
//!     separately.
//!   * For "per byte" costs, like `ActionDeployContractPerByte`, we estimate
//!     the time required for a "large" input, for an empty input, and divide
//!     the difference by the number of bytes. As an alternative, least squares
//!     method is used for cases where we have several interesting inputs with
//!     different sizes.
//!   * For host functions, use few blocks of transactions. Instead, each
//!     transaction calls host function from `wasm` in a loop.
//!   * Some costs are measured more directly. For example, to measure cost of
//!     wasm opcode we call vm_runner directly, bypassing the rest of runtime
//!     machinery.
//!   * Some RocksDB related estimations avoid nearcore entirely and run on
//!     completely independent database instances. This DB is controlled by the
//!     `rdb-` prefixed flags which are combined in `RocksDBTestConfig`.
//!
//! Some costs depend on each other. As we want to allow estimating a subset of
//! costs and don't want to just run everything in order (as that would be to
//! slow), we have a very simple manual caching infrastructure in place.
//!
//! Notes on code architecture:
//!
//! To keep estimations comprehensible, each estimation has a simple function
//! here in the top-level module. Calls to code in submodules should have a
//! descriptive function names so that it is obvious what it does without
//! digging deeper.
//!

mod cost;
mod cost_table;
mod costs_to_runtime_config;
mod estimator_context;
mod gas_cost;
mod qemu;
mod rocksdb;
mod transaction_builder;

pub(crate) mod estimator_params;
pub(crate) mod least_squares;

// Helper functions shared between modules
pub mod utils;

// Runs a VM (Default: Wasmer) on the given contract and measures the time it takes to do a single operation.
pub mod vm_estimator;
// Encapsulates the runtime so that it can be run separately from the rest of the node.
pub mod testbed;
// Prepares transactions and feeds them to the testbed in batches. Performs the warm up, takes care
// of nonces.
pub mod config;
mod function_call;
mod gas_metering;
mod trie;

use std::convert::{TryFrom, TryInto};
use std::time::Instant;

use estimator_params::sha256_cost;
use gas_cost::{LeastSquaresTolerance, NonNegativeTolerance};
use gas_metering::gas_metering_cost;
use near_crypto::{KeyType, SecretKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, SignedTransaction, StakeAction, TransferAction,
};
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{ExtCosts, VMConfig};
use near_vm_runner::MockCompiledContractCache;
use num_rational::Ratio;
use rand::Rng;
use serde_json::json;
use utils::{
    aggregate_per_block_measurements, average_cost, fn_cost, fn_cost_count, fn_cost_with_setup,
    generate_data_only_contract, generate_fn_name, noop_function_call_cost, read_resource,
    transaction_cost, transaction_cost_ext,
};
use vm_estimator::{compile_single_contract_cost, compute_compile_cost_vm};

use crate::config::Config;
use crate::cost_table::format_gas;
use crate::estimator_context::EstimatorContext;
use crate::gas_cost::GasCost;
use crate::rocksdb::{rocks_db_inserts_cost, rocks_db_read_cost};
use crate::transaction_builder::TransactionBuilder;
use crate::vm_estimator::create_context;

pub use crate::cost::Cost;
pub use crate::cost_table::CostTable;
pub use crate::costs_to_runtime_config::costs_to_runtime_config;
pub use crate::qemu::QemuCommandBuilder;
pub use crate::rocksdb::RocksDBTestConfig;

static ALL_COSTS: &[(Cost, fn(&mut EstimatorContext) -> GasCost)] = &[
    (Cost::ActionReceiptCreation, action_receipt_creation),
    (Cost::ActionSirReceiptCreation, action_sir_receipt_creation),
    (Cost::ActionTransfer, action_transfer),
    (Cost::ActionCreateAccount, action_create_account),
    (Cost::ActionDeleteAccount, action_delete_account),
    (Cost::ActionAddFullAccessKey, action_add_full_access_key),
    (Cost::ActionAddFunctionAccessKeyBase, action_add_function_access_key_base),
    (Cost::ActionAddFunctionAccessKeyPerByte, action_add_function_access_key_per_byte),
    (Cost::ActionDeleteKey, action_delete_key),
    (Cost::ActionStake, action_stake),
    (Cost::ActionDeployContractBase, action_deploy_contract_base),
    (Cost::ActionDeployContractPerByte, action_deploy_contract_per_byte),
    (Cost::ActionFunctionCallBase, action_function_call_base),
    (Cost::ActionFunctionCallPerByte, action_function_call_per_byte),
    (Cost::ActionFunctionCallBaseV2, action_function_call_base_v2),
    (Cost::ActionFunctionCallPerByteV2, action_function_call_per_byte_v2),
    (Cost::HostFunctionCall, host_function_call),
    (Cost::WasmInstruction, wasm_instruction),
    (Cost::DataReceiptCreationBase, data_receipt_creation_base),
    (Cost::DataReceiptCreationPerByte, data_receipt_creation_per_byte),
    (Cost::ReadMemoryBase, read_memory_base),
    (Cost::ReadMemoryByte, read_memory_byte),
    (Cost::WriteMemoryBase, write_memory_base),
    (Cost::WriteMemoryByte, write_memory_byte),
    (Cost::ReadRegisterBase, read_register_base),
    (Cost::ReadRegisterByte, read_register_byte),
    (Cost::WriteRegisterBase, write_register_base),
    (Cost::WriteRegisterByte, write_register_byte),
    (Cost::LogBase, log_base),
    (Cost::LogByte, log_byte),
    (Cost::Utf8DecodingBase, utf8_decoding_base),
    (Cost::Utf8DecodingByte, utf8_decoding_byte),
    (Cost::Utf16DecodingBase, utf16_decoding_base),
    (Cost::Utf16DecodingByte, utf16_decoding_byte),
    (Cost::Sha256Base, sha256_base),
    (Cost::Sha256Byte, sha256_byte),
    (Cost::Keccak256Base, keccak256_base),
    (Cost::Keccak256Byte, keccak256_byte),
    (Cost::Keccak512Base, keccak512_base),
    (Cost::Keccak512Byte, keccak512_byte),
    (Cost::Ripemd160Base, ripemd160_base),
    (Cost::Ripemd160Block, ripemd160_block),
    (Cost::EcrecoverBase, ecrecover_base),
    (Cost::AltBn128G1MultiexpBase, alt_bn128g1_multiexp_base),
    (Cost::AltBn128G1MultiexpElement, alt_bn128g1_multiexp_element),
    (Cost::AltBn128G1SumBase, alt_bn128g1_sum_base),
    (Cost::AltBn128G1SumElement, alt_bn128g1_sum_element),
    (Cost::AltBn128PairingCheckBase, alt_bn128_pairing_check_base),
    (Cost::AltBn128PairingCheckElement, alt_bn128_pairing_check_element),
    (Cost::StorageHasKeyBase, storage_has_key_base),
    (Cost::StorageHasKeyByte, storage_has_key_byte),
    (Cost::StorageReadBase, storage_read_base),
    (Cost::StorageReadKeyByte, storage_read_key_byte),
    (Cost::StorageReadValueByte, storage_read_value_byte),
    (Cost::StorageWriteBase, storage_write_base),
    (Cost::StorageWriteKeyByte, storage_write_key_byte),
    (Cost::StorageWriteValueByte, storage_write_value_byte),
    (Cost::StorageWriteEvictedByte, storage_write_evicted_byte),
    (Cost::StorageRemoveBase, storage_remove_base),
    (Cost::StorageRemoveKeyByte, storage_remove_key_byte),
    (Cost::StorageRemoveRetValueByte, storage_remove_ret_value_byte),
    (Cost::TouchingTrieNode, touching_trie_node),
    (Cost::ReadCachedTrieNode, read_cached_trie_node),
    (Cost::TouchingTrieNodeRead, touching_trie_node_read),
    (Cost::TouchingTrieNodeWrite, touching_trie_node_write),
    (Cost::ApplyBlock, apply_block_cost),
    (Cost::ContractCompileBase, contract_compile_base),
    (Cost::ContractCompileBytes, contract_compile_bytes),
    (Cost::ContractCompileBaseV2, contract_compile_base_v2),
    (Cost::ContractCompileBytesV2, contract_compile_bytes_v2),
    (Cost::DeployBytes, pure_deploy_bytes),
    (Cost::GasMeteringBase, gas_metering_base),
    (Cost::GasMeteringOp, gas_metering_op),
    (Cost::RocksDbInsertValueByte, rocks_db_insert_value_byte),
    (Cost::RocksDbReadValueByte, rocks_db_read_value_byte),
    (Cost::CpuBenchmarkSha256, cpu_benchmark_sha256),
    (Cost::OneCPUInstruction, one_cpu_instruction),
    (Cost::OneNanosecond, one_nanosecond),
];

// We use core-contracts, e2f60b5b0930a9df2c413e1460e179c65c8876e3.
static REAL_CONTRACTS_SAMPLE: [(&str, &str); 4] = [
    // File 341191, code 279965, data 56627.
    ("test-contract/res/lockup_contract.wasm", "terminate_vesting"),
    // File 257516, code 203545, data 50419.
    ("test-contract/res/staking_pool.wasm", "ping"),
    // File 135358, code 113152, data 19520.
    ("test-contract/res/voting_contract.wasm", "ping"),
    // File 124250, code 103473, data 18176.
    ("test-contract/res/whitelist.wasm", "add_staking_pool"),
];

pub fn run(config: Config) -> CostTable {
    let mut ctx = EstimatorContext::new(&config);
    let mut res = CostTable::default();

    for (cost, f) in ALL_COSTS.iter().copied() {
        let skip = match &ctx.config.costs_to_measure {
            None => false,
            Some(costs) => !costs.contains(&format!("{:?}", cost)),
        };
        if skip {
            continue;
        }

        let start = Instant::now();
        let measurement = f(&mut ctx);
        let time = start.elapsed();
        let name = cost.to_string();
        let uncertain = if measurement.is_uncertain() { "UNCERTAIN " } else { "" };
        let gas = measurement.to_gas();
        res.add(cost, gas);

        eprintln!(
            "{:<40} {:>25} gas [{:>25}] {:<10}(computed in {:.2?}) {}",
            name,
            format_gas(gas),
            format!("{:?}", measurement),
            uncertain,
            time,
            measurement.uncertain_message().unwrap_or_default(),
        );

        if config.json_output {
            let json = json! ({
                "name": name,
                "result": measurement.to_json(),
                "computed_in": time,
            });
            println!("{json}");
        }
    }
    eprintln!();

    res
}

fn action_receipt_creation(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cached) = ctx.cached.action_receipt_creation.clone() {
        return cached;
    }

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let (sender, receiver) = tb.random_account_pair();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let block_size = 100;
    // Sender != Receiver means this will be executed over two blocks.
    let block_latency = 1;
    let cost = transaction_cost_ext(ctx, block_size, &mut make_transaction, block_latency).0;

    ctx.cached.action_receipt_creation = Some(cost.clone());
    cost
}

fn action_sir_receipt_creation(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cached) = ctx.cached.action_sir_receipt_creation.clone() {
        return cached;
    }

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_account();
        let receiver = sender.clone();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let total_cost = transaction_cost(ctx, &mut make_transaction);

    let block_overhead_cost = apply_block_cost(ctx);
    let cost = total_cost.saturating_sub(&block_overhead_cost, &NonNegativeTolerance::PER_MILLE);

    ctx.cached.action_sir_receipt_creation = Some(cost.clone());
    cost
}

fn action_transfer(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let (sender, receiver) = tb.random_account_pair();

            let actions = vec![Action::Transfer(TransferAction { deposit: 1 })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        let block_size = 100;
        // Transferring from one account to another may touch two shards, thus executes over two blocks.
        let block_latency = 1;
        transaction_cost_ext(ctx, block_size, &mut make_transaction, block_latency).0
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_create_account(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_account();
            let new_account =
                AccountId::try_from(format!("{}_{}", sender, tb.rng().gen::<u64>())).unwrap();

            let actions = vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::Transfer(TransferAction { deposit: 10u128.pow(26) }),
            ];
            tb.transaction_from_actions(sender, new_account, actions)
        };
        let block_size = 100;
        // Creating a new account is initiated by an account that potentially is on a different shard. Thus, it executes over two blocks.
        let block_latency = 1;
        transaction_cost_ext(ctx, block_size, &mut make_transaction, block_latency).0
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_delete_account(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();
            let beneficiary_id = tb.random_unused_account();

            let actions = vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        let block_size = 100;
        // Deleting an account is initiated by an account that potentially is on a different shard. Thus, it executes over two blocks.
        let block_latency = 1;
        transaction_cost_ext(ctx, block_size, &mut make_transaction, block_latency).0
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_add_full_access_key(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();

            add_key_transaction(tb, sender, AccessKeyPermission::FullAccess)
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_add_function_access_key_base(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.action_add_function_access_key_base.clone() {
        return cost;
    }

    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver_id = tb.account(0).to_string();

            let permission = AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(100),
                receiver_id,
                method_names: vec!["method1".to_string()],
            });
            add_key_transaction(tb, sender, permission)
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE);
    ctx.cached.action_add_function_access_key_base = Some(cost.clone());
    cost
}

fn action_add_function_access_key_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let many_methods: Vec<_> = (0..1000).map(|i| format!("a123456{:03}", i)).collect();
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver_id = tb.account(0).to_string();

            let permission = AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(100),
                receiver_id,
                method_names: many_methods.clone(),
            });
            add_key_transaction(tb, sender, permission)
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = action_add_function_access_key_base(ctx);

    // 1k methods for 10 bytes each
    let bytes_per_transaction = 10 * 1000;

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / bytes_per_transaction
}

fn add_key_transaction(
    tb: &mut TransactionBuilder,
    sender: AccountId,
    permission: AccessKeyPermission,
) -> SignedTransaction {
    let receiver = sender.clone();

    let public_key = "ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847".parse().unwrap();
    let access_key = AccessKey { nonce: 0, permission };

    tb.transaction_from_actions(
        sender,
        receiver,
        vec![Action::AddKey(AddKeyAction { public_key, access_key })],
    )
}

fn action_delete_key(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::DeleteKey(DeleteKeyAction {
                public_key: SecretKey::from_seed(KeyType::ED25519, sender.as_ref()).public_key(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_stake(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::Stake(StakeAction {
                stake: 1,
                public_key: "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}

fn action_deploy_contract_base(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.deploy_contract_base.clone() {
        return cost;
    }

    let cost = {
        let code = read_resource("test-contract/res/smallest_contract.wasm");
        deploy_contract_cost(ctx, code, Some(b"sum"))
    };

    ctx.cached.deploy_contract_base = Some(cost.clone());
    cost
}
fn action_deploy_contract_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let mut xs = vec![];
    let mut ys = vec![];

    for (contract, pivot_fn) in REAL_CONTRACTS_SAMPLE {
        let code = read_resource(contract);
        xs.push(code.len() as u64);
        let cost = deploy_contract_cost(ctx, code, Some(pivot_fn.as_bytes()));
        // The sampled contracts are about 80% code. Since the deployment cost
        // is heavily dominated by compilation, we therefore use a multiplier of
        // 5/4 to guess what a contract with 100% code would cost to deploy.
        ys.push(cost * 5 / 4);
    }

    // We do linear regression on a cost curve that is mostly flat for small
    // contracts and steeper for larger contracts. Thus, the fitted linear
    // function usually crosses the y-axis somewhere in the negative. The
    // tolerance is chosen, quite arbitrarily, as a full base cost from protocol
    // v50. Values further in the negative indicate that the estimation error is
    // out of proportion.
    let negative_base_tolerance = 369_531_500_000u64;
    // For icount-based measurements, since we start compilation after the full
    // contract is already loaded into memory, it is possible that IO costs per
    // byte are essentially 0 and sometimes negative in the fitted curve. If
    // this negative value is small enough, this can be tolerated and the
    // parameter is clamped to 0 without marking the result as uncertain.
    let rel_factor_tolerance = 0.001;
    let (_base, per_byte) = GasCost::least_squares_method_gas_cost(
        &xs,
        &ys,
        &LeastSquaresTolerance::default()
            .base_abs_nn_tolerance(negative_base_tolerance)
            .factor_rel_nn_tolerance(rel_factor_tolerance),
        ctx.config.debug,
    );
    per_byte
}

/// Cost for deploying a specific contract.
///
/// This function will run however many iterations of the transaction as has
/// been defined in the config.
/// To avoid hitting the contract code cache, a pivot function name can be
/// provided. This must be a name of an exported function in the WASM module. It
/// will be dynamically modified on every iteration to a unique name. This
/// ensures a different code hash every time, without logical changes to the
/// contract.
fn deploy_contract_cost(
    ctx: &mut EstimatorContext,
    code: Vec<u8>,
    pivot_fn_name: Option<&[u8]>,
) -> GasCost {
    let mut code_num = 0;
    let mut code_factory = || {
        let mut code = code.clone();
        if let Some(pivot_fn_name) = pivot_fn_name {
            let unique_name = generate_fn_name(code_num, pivot_fn_name.len());
            code_num += 1;

            let start =
                code.windows(pivot_fn_name.len()).position(|slice| slice == pivot_fn_name).unwrap();
            code[start..(start + pivot_fn_name.len())].copy_from_slice(&unique_name);
        }
        code
    };

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_unused_account();
        let receiver = sender.clone();

        let actions = vec![Action::DeployContract(DeployContractAction { code: code_factory() })];
        tb.transaction_from_actions(sender, receiver, actions)
    };
    // Use a small block size since deployments are gas heavy.
    let block_size = 5;
    let (total_cost, _ext) = transaction_cost_ext(ctx, block_size, &mut make_transaction, 0);
    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}
fn contract_compile_base(ctx: &mut EstimatorContext) -> GasCost {
    compilation_cost_base_per_byte(ctx).0
}
fn contract_compile_bytes(ctx: &mut EstimatorContext) -> GasCost {
    compilation_cost_base_per_byte(ctx).1
}
fn compilation_cost_base_per_byte(ctx: &mut EstimatorContext) -> (GasCost, GasCost) {
    if let Some(base_byte_cost) = ctx.cached.compile_cost_base_per_byte.clone() {
        return base_byte_cost;
    }

    let verbose = ctx.config.debug;
    let base_byte_cost = compute_compile_cost_vm(ctx.config.metric, ctx.config.vm_kind, verbose);

    ctx.cached.compile_cost_base_per_byte = Some(base_byte_cost.clone());
    base_byte_cost
}
fn contract_compile_base_v2(ctx: &mut EstimatorContext) -> GasCost {
    contract_compile_base_per_byte_v2(ctx).0
}
fn contract_compile_bytes_v2(ctx: &mut EstimatorContext) -> GasCost {
    contract_compile_base_per_byte_v2(ctx).1
}
fn contract_compile_base_per_byte_v2(ctx: &mut EstimatorContext) -> (GasCost, GasCost) {
    if let Some(costs) = ctx.cached.compile_cost_base_per_byte_v2.clone() {
        return costs;
    }

    let smallest_contract = read_resource("test-contract/res/smallest_contract.wasm");

    let smallest_cost =
        compile_single_contract_cost(ctx.config.metric, ctx.config.vm_kind, &smallest_contract);
    let smallest_size = smallest_contract.len() as u64;

    let mut max_bytes_cost = GasCost::zero(ctx.config.metric);
    for (contract, _) in REAL_CONTRACTS_SAMPLE {
        let binary = read_resource(contract);
        let cost = compile_single_contract_cost(ctx.config.metric, ctx.config.vm_kind, &binary);
        let bytes_cost = cost.saturating_sub(&smallest_cost, &NonNegativeTolerance::PER_MILLE)
            / (binary.len() as u64 - smallest_size);
        max_bytes_cost = std::cmp::max(bytes_cost, max_bytes_cost);
    }

    let base_cost = smallest_cost.saturating_sub(
        &(max_bytes_cost.clone() * smallest_size),
        &NonNegativeTolerance::PER_MILLE,
    );
    let costs = (base_cost, max_bytes_cost);

    ctx.cached.compile_cost_base_per_byte_v2 = Some(costs.clone());
    costs
}
fn pure_deploy_bytes(ctx: &mut EstimatorContext) -> GasCost {
    let small_code = generate_data_only_contract(0);
    let large_code = generate_data_only_contract(bytesize::mb(4u64) as usize);
    let small_code_len = small_code.len();
    let large_code_len = large_code.len();
    let cost_empty = deploy_contract_cost(ctx, small_code, Some(b"main"));
    let cost_4mb = deploy_contract_cost(ctx, large_code, Some(b"main"));

    (cost_4mb - cost_empty) / (large_code_len - small_code_len) as u64
}

fn action_function_call_base(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = noop_function_call_cost(ctx);
    let base_cost = action_sir_receipt_creation(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE)
}
fn action_function_call_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", vec![0; 1024 * 1024])
        };
        transaction_cost(ctx, &mut make_transaction)
    };

    let base_cost = noop_function_call_cost(ctx);

    let bytes_per_transaction = 1024 * 1024;

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / bytes_per_transaction
}

fn action_function_call_base_v2(ctx: &mut EstimatorContext) -> GasCost {
    let (base, _per_byte) = action_function_call_base_per_byte_v2(ctx);
    base
}
fn action_function_call_per_byte_v2(ctx: &mut EstimatorContext) -> GasCost {
    let (_base, per_byte) = action_function_call_base_per_byte_v2(ctx);
    per_byte
}
fn action_function_call_base_per_byte_v2(ctx: &mut EstimatorContext) -> (GasCost, GasCost) {
    if let Some(base_byte_cost) = ctx.cached.action_function_call_base_per_byte_v2.clone() {
        return base_byte_cost;
    }

    let (base, byte) =
        crate::function_call::test_function_call(ctx.config.metric, ctx.config.vm_kind);
    let convert_ratio = |r: Ratio<i128>| -> Ratio<u64> {
        Ratio::new((*r.numer()).try_into().unwrap(), (*r.denom()).try_into().unwrap())
    };
    let base_byte_cost = (
        GasCost::from_gas(convert_ratio(base), ctx.config.metric),
        GasCost::from_gas(convert_ratio(byte), ctx.config.metric),
    );

    ctx.cached.action_function_call_base_per_byte_v2 = Some(base_byte_cost.clone());
    base_byte_cost
}

fn data_receipt_creation_base(ctx: &mut EstimatorContext) -> GasCost {
    // NB: there isn't `ExtCosts` for data receipt creation, so we ignore (`_`) the counts.
    // The function returns a chain of two promises.
    let block_latency = 2;
    let (total_cost, _) =
        fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base, block_latency);
    // The function returns a promise.
    let block_latency = 1;
    let (base_cost, _) =
        fn_cost_count(ctx, "data_receipt_base_10b_1000", ExtCosts::base, block_latency);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / 1000
}

fn data_receipt_creation_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    // NB: there isn't `ExtCosts` for data receipt creation, so we ignore (`_`) the counts.
    // The function returns a chain of two promises.
    let block_latency = 2;
    let (total_cost, _) =
        fn_cost_count(ctx, "data_receipt_100kib_1000", ExtCosts::base, block_latency);
    // The function returns a chain of two promises.
    let block_latency = 2;
    let (base_cost, _) = fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base, block_latency);

    let bytes_per_transaction = 1000 * 100 * 1024;

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / bytes_per_transaction
}

fn host_function_call(ctx: &mut EstimatorContext) -> GasCost {
    let block_latency = 0;
    let (total_cost, count) = fn_cost_count(ctx, "base_1M", ExtCosts::base, block_latency);
    assert_eq!(count, 1_000_000);

    let base_cost = noop_function_call_cost(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / count
}

fn wasm_instruction(ctx: &mut EstimatorContext) -> GasCost {
    let vm_kind = ctx.config.vm_kind;

    let code = read_resource(if cfg!(feature = "nightly_protocol_features") {
        "test-contract/res/nightly_large_contract.wasm"
    } else {
        "test-contract/res/stable_large_contract.wasm"
    });

    let n_iters = 10;

    let code = ContractCode::new(code.to_vec(), None);
    let mut fake_external = MockedExternal::new();
    let config = VMConfig::test();
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    let cache = MockCompiledContractCache::default();

    let mut run = || {
        let context = create_context(vec![]);
        let vm_result = vm_kind.runtime(config.clone()).unwrap().run(
            &code,
            "cpu_ram_soak_test",
            &mut fake_external,
            context,
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            Some(&cache),
        );
        assert!(vm_result.error().is_some());
        vm_result.outcome_error().0
    };

    let warmup_outcome = run();

    let total = {
        let start = GasCost::measure(ctx.config.metric);
        for _ in 0..n_iters {
            run();
        }
        start.elapsed()
    };

    let instructions_per_iter = {
        let op_cost = config.regular_op_cost as u64;
        warmup_outcome.burnt_gas / op_cost
    };

    let per_instruction = total / (instructions_per_iter * n_iters);
    per_instruction
}

fn read_memory_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "read_memory_10b_10k", ExtCosts::read_memory_base, 10_000)
}
fn read_memory_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "read_memory_1Mib_10k", ExtCosts::read_memory_byte, 1024 * 1024 * 10_000)
}

fn write_memory_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "write_memory_10b_10k", ExtCosts::write_memory_base, 10_000)
}
fn write_memory_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "write_memory_1Mib_10k", ExtCosts::write_memory_byte, 1024 * 1024 * 10_000)
}

fn read_register_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "read_register_10b_10k", ExtCosts::read_register_base, 10_000)
}
fn read_register_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "read_register_1Mib_10k", ExtCosts::read_register_byte, 1024 * 1024 * 10_000)
}

fn write_register_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "write_register_10b_10k", ExtCosts::write_register_base, 10_000)
}
fn write_register_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "write_register_1Mib_10k", ExtCosts::write_register_byte, 1024 * 1024 * 10_000)
}

fn log_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "utf16_log_10b_10k", ExtCosts::log_base, 10_000)
}
fn log_byte(ctx: &mut EstimatorContext) -> GasCost {
    // NOTE: We are paying per *output* byte here, hence 3/2 multiplier.
    fn_cost(ctx, "utf16_log_10kib_1k", ExtCosts::log_byte, (10 * 1024 * 3 / 2) * 1_000)
}

fn utf8_decoding_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "utf8_log_10b_10k", ExtCosts::utf8_decoding_base, 10_000)
}
fn utf8_decoding_byte(ctx: &mut EstimatorContext) -> GasCost {
    let no_nul = fn_cost(ctx, "utf8_log_10kib_1k", ExtCosts::utf8_decoding_byte, 10 * 1024 * 1_000);
    let nul = fn_cost(
        ctx,
        "nul_utf8_log_10kib_1k",
        ExtCosts::utf8_decoding_byte,
        (10 * 1024 - 1) * 1_000,
    );
    nul.max(no_nul)
}

fn utf16_decoding_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "utf16_log_10b_10k", ExtCosts::utf16_decoding_base, 10_000)
}
fn utf16_decoding_byte(ctx: &mut EstimatorContext) -> GasCost {
    let no_nul =
        fn_cost(ctx, "utf16_log_10kib_1k", ExtCosts::utf16_decoding_byte, 10 * 1024 * 1_000);
    let nul = fn_cost(
        ctx,
        "nul_utf16_log_10kib_1k",
        ExtCosts::utf16_decoding_byte,
        (10 * 1024 - 2) * 1_000,
    );
    nul.max(no_nul)
}

fn sha256_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "sha256_10b_10k", ExtCosts::sha256_base, 10_000)
}
fn sha256_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "sha256_10kib_10k", ExtCosts::sha256_byte, 10 * 1024 * 10_000)
}

fn keccak256_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "keccak256_10b_10k", ExtCosts::keccak256_base, 10_000)
}
fn keccak256_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "keccak256_10kib_10k", ExtCosts::keccak256_byte, 10 * 1024 * 10_000)
}

fn keccak512_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "keccak512_10b_10k", ExtCosts::keccak512_base, 10_000)
}
fn keccak512_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "keccak512_10kib_10k", ExtCosts::keccak512_byte, 10 * 1024 * 10_000)
}

fn ripemd160_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "ripemd160_10b_10k", ExtCosts::ripemd160_base, 10_000)
}
fn ripemd160_block(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "ripemd160_10kib_10k", ExtCosts::ripemd160_block, (10 * 1024 / 64 + 1) * 10_000)
}

fn ecrecover_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "ecrecover_10k", ExtCosts::ecrecover_base, 10_000)
}

fn alt_bn128g1_multiexp_base(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(ctx, "alt_bn128_g1_multiexp_1_1k", ExtCosts::alt_bn128_g1_multiexp_base, 1000);
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}
fn alt_bn128g1_multiexp_element(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_g1_multiexp_10_1k",
        ExtCosts::alt_bn128_g1_multiexp_element,
        10 * 1000,
    );
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}

fn alt_bn128g1_sum_base(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(ctx, "alt_bn128_g1_sum_1_1k", ExtCosts::alt_bn128_g1_sum_base, 1000);
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}
fn alt_bn128g1_sum_element(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(ctx, "alt_bn128_g1_sum_10_1k", ExtCosts::alt_bn128_g1_sum_element, 10 * 1000);
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}

fn alt_bn128_pairing_check_base(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_pairing_check_1_1k",
        ExtCosts::alt_bn128_pairing_check_base,
        1000,
    );
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}
fn alt_bn128_pairing_check_element(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_pairing_check_10_1k",
        ExtCosts::alt_bn128_pairing_check_element,
        10 * 1000,
    );
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}

fn storage_has_key_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10b_value_1k",
        "storage_has_key_10b_key_10b_value_1k",
        ExtCosts::storage_has_key_base,
        1000,
    )
}
fn storage_has_key_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10kib_key_10b_value_1k",
        "storage_has_key_10kib_key_10b_value_1k",
        ExtCosts::storage_has_key_byte,
        10 * 1024 * 1000,
    )
}

fn storage_read_base(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.storage_read_base.clone() {
        return cost;
    }

    let cost = fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10b_value_1k",
        "storage_read_10b_key_10b_value_1k",
        ExtCosts::storage_read_base,
        1000,
    );

    ctx.cached.storage_read_base = Some(cost.clone());
    cost
}
fn storage_read_key_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10kib_key_10b_value_1k",
        "storage_read_10kib_key_10b_value_1k",
        ExtCosts::storage_read_key_byte,
        10 * 1024 * 1000,
    )
}
fn storage_read_value_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10kib_value_1k",
        "storage_read_10b_key_10kib_value_1k",
        ExtCosts::storage_read_value_byte,
        10 * 1024 * 1000,
    )
}

fn storage_write_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "storage_write_10b_key_10b_value_1k", ExtCosts::storage_write_base, 1000)
}
fn storage_write_key_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(
        ctx,
        "storage_write_10kib_key_10b_value_1k",
        ExtCosts::storage_write_key_byte,
        10 * 1024 * 1000,
    )
}
fn storage_write_value_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(
        ctx,
        "storage_write_10b_key_10kib_value_1k",
        ExtCosts::storage_write_value_byte,
        10 * 1024 * 1000,
    )
}
fn storage_write_evicted_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10kib_value_1k",
        "storage_write_10b_key_10kib_value_1k",
        ExtCosts::storage_write_evicted_byte,
        10 * 1024 * 1000,
    )
}

fn storage_remove_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10b_value_1k",
        "storage_remove_10b_key_10b_value_1k",
        ExtCosts::storage_remove_base,
        1000,
    )
}
fn storage_remove_key_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10kib_key_10b_value_1k",
        "storage_remove_10kib_key_10b_value_1k",
        ExtCosts::storage_remove_key_byte,
        10 * 1024 * 1000,
    )
}
fn storage_remove_ret_value_byte(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost_with_setup(
        ctx,
        "storage_write_10b_key_10kib_value_1k",
        "storage_remove_10b_key_10kib_value_1k",
        ExtCosts::storage_remove_ret_value_byte,
        10 * 1024 * 1000,
    )
}

fn touching_trie_node(ctx: &mut EstimatorContext) -> GasCost {
    let read = touching_trie_node_read(ctx);
    let write = touching_trie_node_write(ctx);
    return std::cmp::max(read, write);
}

fn touching_trie_node_read(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.touching_trie_node_read.clone() {
        return cost;
    }
    let warmup_iters = ctx.config.warmup_iters_per_block;
    let measured_iters = ctx.config.iter_per_block;
    // Number of bytes in the final key. Will create 2x that many nodes.
    // Picked somewhat arbitrarily, balancing estimation time vs accuracy.
    let final_key_len = 1000;
    let mut testbed = ctx.testbed();
    let cost = trie::read_node_from_db(&mut testbed, warmup_iters, measured_iters, final_key_len);

    ctx.cached.touching_trie_node_read = Some(cost.clone());
    cost
}

fn touching_trie_node_write(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.touching_trie_node_write.clone() {
        return cost;
    }
    let warmup_iters = ctx.config.warmup_iters_per_block;
    let measured_iters = ctx.config.iter_per_block;
    // Number of bytes in the final key. Will create 2x that many nodes.
    // Picked somewhat arbitrarily, balancing estimation time vs accuracy.
    let final_key_len = 1000;
    let mut testbed = ctx.testbed();
    let cost = trie::write_node(&mut testbed, warmup_iters, measured_iters, final_key_len);

    ctx.cached.touching_trie_node_write = Some(cost.clone());
    cost
}

fn read_cached_trie_node(ctx: &mut EstimatorContext) -> GasCost {
    let warmup_iters = ctx.config.warmup_iters_per_block;
    let iters = ctx.config.iter_per_block;
    let mut testbed = ctx.testbed();

    let results = (0..(warmup_iters + iters))
        .map(|_| trie::read_node_from_chunk_cache(&mut testbed))
        .skip(warmup_iters)
        .collect::<Vec<_>>();
    average_cost(ctx.config, &results)
}

fn apply_block_cost(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.apply_block.clone() {
        return cost;
    }

    let mut testbed = ctx.testbed();

    let n_blocks = testbed.config.warmup_iters_per_block + testbed.config.iter_per_block;
    let blocks = vec![vec![]; n_blocks];

    let measurements = testbed.measure_blocks(blocks, 0);
    let measurements =
        measurements.into_iter().skip(testbed.config.warmup_iters_per_block).collect::<Vec<_>>();
    let (gas_cost, _ext_costs) = aggregate_per_block_measurements(testbed.config, 1, measurements);

    ctx.cached.apply_block = Some(gas_cost.clone());

    gas_cost
}

fn gas_metering_base(ctx: &mut EstimatorContext) -> GasCost {
    gas_metering(ctx).0
}

fn gas_metering_op(ctx: &mut EstimatorContext) -> GasCost {
    gas_metering(ctx).1
}

fn rocks_db_insert_value_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_bytes = ctx.config.rocksdb_test_config.op_count as u64
        * ctx.config.rocksdb_test_config.value_size as u64;
    rocks_db_inserts_cost(&ctx.config) / total_bytes
}

fn rocks_db_read_value_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_bytes = ctx.config.rocksdb_test_config.op_count as u64
        * ctx.config.rocksdb_test_config.value_size as u64;
    rocks_db_read_cost(&ctx.config) / total_bytes
}

fn gas_metering(ctx: &mut EstimatorContext) -> (GasCost, GasCost) {
    if let Some(cached) = ctx.cached.gas_metering_cost_base_per_op.clone() {
        return cached;
    }
    let (base, byte) = gas_metering_cost(&ctx.config);
    ctx.cached.gas_metering_cost_base_per_op = Some((base.clone(), byte.clone()));
    (base, byte)
}

fn cpu_benchmark_sha256(ctx: &mut EstimatorContext) -> GasCost {
    const REPEATS: u64 = 1_000_000;
    sha256_cost(ctx.config.metric, REPEATS)
}

/// Estimate how much gas is charged for 1 CPU instruction. (Using given runtime parameters, on the specific system this is being run on.)
fn one_cpu_instruction(ctx: &mut EstimatorContext) -> GasCost {
    eprintln!("Cannot estimate ONE_CPU_INSTRUCTION like any other cost. The result will only show the constant value currently used in the estimator.");
    GasCost::from_gas(estimator_params::GAS_IN_INSTR, ctx.config.metric)
}

/// Estimate how much gas is charged for 1 nanosecond of computation. (Using given runtime parameters, on the specific system this is being run on.)
fn one_nanosecond(ctx: &mut EstimatorContext) -> GasCost {
    // Currently we don't have a test for this, yet. 1 gas has just always been 1ns.
    // But it would be useful to go backwards and see how expensive computation time is on specific hardware.
    eprintln!("Cannot estimate ONE_NANOSECOND like any other cost. The result will only show the constant value currently used in the estimator.");
    GasCost::from_gas(estimator_params::GAS_IN_NS, ctx.config.metric)
}
