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
//!
//! Some costs depend on each other. As we want to allow estimating a subset of
//! costs and don't want to just run everything in order (as that would be to
//! slow), we have a very simple manual caching infrastructure in place.

mod cost;
mod cost_table;
mod costs_to_runtime_config;
mod estimator_context;
mod gas_cost;
mod qemu;
mod rocksdb;
mod transaction_builder;

pub(crate) mod estimator_params;

// Runs a VM (Default: Wasmer) on the given contract and measures the time it takes to do a single operation.
pub mod vm_estimator;
// Encapsulates the runtime so that it can be run separately from the rest of the node.
pub mod testbed;
// Prepares transactions and feeds them to the testbed in batches. Performs the warm up, takes care
// of nonces.
pub mod config;
mod function_call;
mod gas_metering;

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::time::Instant;

use estimator_params::sha256_cost;
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
use vm_estimator::compute_compile_cost_vm;

use crate::config::Config;
use crate::cost_table::format_gas;
use crate::estimator_context::{EstimatorContext, Testbed};
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
    (Cost::AltBn128G1MultiexpByte, alt_bn128g1_multiexp_byte),
    (Cost::AltBn128G1MultiexpSublinear, alt_bn128g1_multiexp_sublinear),
    (Cost::AltBn128G1SumBase, alt_bn128g1_sum_base),
    (Cost::AltBn128G1SumByte, alt_bn128g1_sum_byte),
    (Cost::AltBn128PairingCheckBase, alt_bn128_pairing_check_base),
    (Cost::AltBn128PairingCheckByte, alt_bn128_pairing_check_byte),
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
    (Cost::GasMeteringBase, gas_metering_base),
    (Cost::GasMeteringOp, gas_metering_op),
    (Cost::RocksDbInsertValueByte, rocks_db_insert_value_byte),
    (Cost::RocksDbReadValueByte, rocks_db_read_value_byte),
    (Cost::CpuBenchmarkSha256, cpu_benchmark_sha256),
    (Cost::OneCPUInstruction, one_cpu_instruction),
    (Cost::OneNanosecond, one_nanosecond),
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
        let uncertain = if measurement.is_uncertain() { "UNCERTAIN " } else { "" };
        let gas = measurement.to_gas();
        res.add(cost, gas);

        eprintln!(
            "{:<40} {:>25} gas [{:>25}] {:<10}(computed in {:?})",
            cost.to_string(),
            format_gas(gas),
            format!("{:?}", measurement),
            uncertain,
            start.elapsed(),
        );
    }
    eprintln!();

    res
}

fn action_receipt_creation(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cached) = ctx.cached.action_receipt_creation.clone() {
        return cached;
    }

    let testbed = ctx.testbed();

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let (sender, receiver) = tb.random_account_pair();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let cost = transaction_cost(testbed, &mut make_transaction);

    ctx.cached.action_receipt_creation = Some(cost.clone());
    cost
}

fn action_sir_receipt_creation(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cached) = ctx.cached.action_sir_receipt_creation.clone() {
        return cached;
    }

    let testbed = ctx.testbed();

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_account();
        let receiver = sender.clone();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let cost = transaction_cost(testbed, &mut make_transaction);

    ctx.cached.action_sir_receipt_creation = Some(cost.clone());
    cost
}

fn action_transfer(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let (sender, receiver) = tb.random_account_pair();

            let actions = vec![Action::Transfer(TransferAction { deposit: 1 })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_create_account(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

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
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_delete_account(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();
            let beneficiary_id = tb.random_unused_account();

            let actions = vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_add_full_access_key(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();

            add_key_transaction(tb, sender, AccessKeyPermission::FullAccess)
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_add_function_access_key_base(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.action_add_function_access_key_base.clone() {
        return cost;
    }

    let total_cost = {
        let testbed = ctx.testbed();

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
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost - base_cost;
    ctx.cached.action_add_function_access_key_base = Some(cost.clone());
    cost
}

fn action_add_function_access_key_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

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
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_add_function_access_key_base(ctx);

    // 1k methods for 10 bytes each
    let bytes_per_transaction = 10 * 1000;

    (total_cost - base_cost) / bytes_per_transaction
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
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::DeleteKey(DeleteKeyAction {
                public_key: SecretKey::from_seed(KeyType::ED25519, sender.as_ref()).public_key(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_stake(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::Stake(StakeAction {
                stake: 1,
                public_key: "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

/// For deploy costs, the cost is the sum two components:
///   * database-related costs which we measure by deploying a contract with
///     dummy payload
///   * cost of contract compilation, which we estimate using linear regression
///     on some real contracts.
fn action_deploy_contract_base(ctx: &mut EstimatorContext) -> GasCost {
    let (compilation_base_cost, _) = compilation_cost_base_per_byte(ctx);
    let base_cost = deploy_contract_base(ctx);
    base_cost + compilation_base_cost
}
fn action_deploy_contract_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let code = read_resource(if cfg!(feature = "nightly_protocol_features") {
            "test-contract/res/nightly_large_contract.wasm"
        } else {
            "test-contract/res/stable_large_contract.wasm"
        });
        deploy_contract_cost(ctx, code)
    };

    let (_, compilation_per_byte_cost) = compilation_cost_base_per_byte(ctx);
    let base_cost = deploy_contract_base(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction + compilation_per_byte_cost
}
fn deploy_contract_base(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.deploy_contract_base.clone() {
        return cost;
    }

    let total_cost = {
        let code = read_resource("test-contract/res/smallest_contract.wasm");
        deploy_contract_cost(ctx, code)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost - base_cost;
    ctx.cached.deploy_contract_base = Some(cost.clone());
    cost
}
fn deploy_contract_cost(ctx: &mut EstimatorContext, code: Vec<u8>) -> GasCost {
    let testbed = ctx.testbed();

    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_unused_account();
        let receiver = sender.clone();

        let actions = vec![Action::DeployContract(DeployContractAction { code: code.clone() })];
        tb.transaction_from_actions(sender, receiver, actions)
    };
    transaction_cost(testbed, &mut make_transaction)
}
fn compilation_cost_base_per_byte(ctx: &mut EstimatorContext) -> (GasCost, GasCost) {
    if let Some(base_byte_cost) = ctx.cached.compile_cost_base_per_byte.clone() {
        return base_byte_cost;
    }

    let verbose = false;
    let (base, byte) = compute_compile_cost_vm(ctx.config.metric, ctx.config.vm_kind, verbose);

    let base_byte_cost = (
        GasCost::from_gas(base.into(), ctx.config.metric),
        GasCost::from_gas(byte.into(), ctx.config.metric),
    );

    ctx.cached.compile_cost_base_per_byte = Some(base_byte_cost.clone());
    base_byte_cost
}

fn action_function_call_base(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = noop_function_call_cost(ctx);
    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}
fn action_function_call_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    let total_cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", vec![0; 1024 * 1024])
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    let base_cost = noop_function_call_cost(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
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
    let (total_cost, _) = fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base);
    let (base_cost, _) = fn_cost_count(ctx, "data_receipt_base_10b_1000", ExtCosts::base);
    (total_cost - base_cost) / 1000
}

fn data_receipt_creation_per_byte(ctx: &mut EstimatorContext) -> GasCost {
    // NB: there isn't `ExtCosts` for data receipt creation, so we ignore (`_`) the counts.
    let (total_cost, _) = fn_cost_count(ctx, "data_receipt_100kib_1000", ExtCosts::base);
    let (base_cost, _) = fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base);

    let bytes_per_transaction = 1000 * 100 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn host_function_call(ctx: &mut EstimatorContext) -> GasCost {
    let (total_cost, count) = fn_cost_count(ctx, "base_1M", ExtCosts::base);
    assert_eq!(count, 1_000_000);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}
fn noop_function_call_cost(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.noop_function_call_cost.clone() {
        return cost;
    }

    let cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", Vec::new())
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    ctx.cached.noop_function_call_cost = Some(cost.clone());
    cost
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
        let (outcome, err) = vm_kind.runtime(config.clone()).unwrap().run(
            &code,
            "cpu_ram_soak_test",
            &mut fake_external,
            context,
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            Some(&cache),
        );
        match (outcome, err) {
            (Some(it), Some(_)) => it,
            _ => panic!(),
        }
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
    fn_cost(ctx, "utf16_log_10kib_10k", ExtCosts::log_byte, (10 * 1024 * 3 / 2) * 10_000)
}

fn utf8_decoding_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "utf8_log_10b_10k", ExtCosts::utf8_decoding_base, 10_000)
}
fn utf8_decoding_byte(ctx: &mut EstimatorContext) -> GasCost {
    let no_nul =
        fn_cost(ctx, "utf8_log_10kib_10k", ExtCosts::utf8_decoding_byte, 10 * 1024 * 10_000);
    let nul = fn_cost(
        ctx,
        "nul_utf8_log_10kib_10k",
        ExtCosts::utf8_decoding_byte,
        (10 * 1024 - 1) * 10_000,
    );
    nul.max(no_nul)
}

fn utf16_decoding_base(ctx: &mut EstimatorContext) -> GasCost {
    fn_cost(ctx, "utf16_log_10b_10k", ExtCosts::utf16_decoding_base, 10_000)
}
fn utf16_decoding_byte(ctx: &mut EstimatorContext) -> GasCost {
    let no_nul =
        fn_cost(ctx, "utf16_log_10kib_10k", ExtCosts::utf16_decoding_byte, 10 * 1024 * 10_000);
    let nul = fn_cost(
        ctx,
        "nul_utf16_log_10kib_10k",
        ExtCosts::utf16_decoding_byte,
        (10 * 1024 - 2) * 10_000,
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
fn alt_bn128g1_multiexp_byte(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_g1_multiexp_10_1k",
        ExtCosts::alt_bn128_g1_multiexp_byte,
        964 * 1000,
    );
    #[cfg(not(feature = "protocol_feature_alt_bn128"))]
    return GasCost::zero(ctx.config.metric);
}
fn alt_bn128g1_multiexp_sublinear(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_g1_multiexp_10_1k",
        ExtCosts::alt_bn128_g1_multiexp_sublinear,
        743342 * 1000,
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
fn alt_bn128g1_sum_byte(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(ctx, "alt_bn128_g1_sum_10_1k", ExtCosts::alt_bn128_g1_sum_byte, 654 * 1000);
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
fn alt_bn128_pairing_check_byte(ctx: &mut EstimatorContext) -> GasCost {
    #[cfg(feature = "protocol_feature_alt_bn128")]
    return fn_cost(
        ctx,
        "alt_bn128_pairing_check_10_1k",
        ExtCosts::alt_bn128_pairing_check_byte,
        1924 * 1000,
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
    // TODO: Actually compute it once our storage is complete.
    // TODO: temporary value, as suggested by @nearmax, divisor is log_16(20000) ~ 3.57 ~ 7/2.
    storage_read_base(ctx) * 2 / 7
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
    let (base, byte) = gas_metering_cost(ctx.config.metric, ctx.config.vm_kind);
    let base = GasCost::from_gas(base.into(), ctx.config.metric);
    let byte = GasCost::from_gas(byte.into(), ctx.config.metric);
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

// Helpers

/// Get account id from its index.
fn get_account_id(account_index: usize) -> AccountId {
    AccountId::try_from(format!("near_{}_{}", account_index, account_index)).unwrap()
}

fn transaction_cost(
    testbed: Testbed,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
) -> GasCost {
    let block_size = 100;
    let (gas_cost, _ext_costs) = transaction_cost_ext(testbed, block_size, make_transaction);
    gas_cost
}

fn transaction_cost_ext(
    mut testbed: Testbed,
    block_size: usize,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let blocks = {
        let n_blocks = testbed.config.warmup_iters_per_block + testbed.config.iter_per_block;
        let mut blocks = Vec::with_capacity(n_blocks);
        for _ in 0..n_blocks {
            let mut block = Vec::with_capacity(block_size);
            for _ in 0..block_size {
                let tx = make_transaction(testbed.transaction_builder());
                block.push(tx)
            }
            blocks.push(block)
        }
        blocks
    };

    let measurements = testbed.measure_blocks(blocks);
    let measurements =
        measurements.into_iter().skip(testbed.config.warmup_iters_per_block).collect::<Vec<_>>();

    aggregate_per_block_measurements(testbed.config, block_size, measurements)
}

fn fn_cost(ctx: &mut EstimatorContext, method: &str, ext_cost: ExtCosts, count: u64) -> GasCost {
    let (total_cost, measured_count) = fn_cost_count(ctx, method, ext_cost);
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

fn fn_cost_count(ctx: &mut EstimatorContext, method: &str, ext_cost: ExtCosts) -> (GasCost, u64) {
    let block_size = 2;
    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_unused_account();
        tb.transaction_from_function_call(sender, method, Vec::new())
    };
    let testbed = ctx.testbed();
    let (gas_cost, ext_costs) = transaction_cost_ext(testbed, block_size, &mut make_transaction);
    let ext_cost = ext_costs[&ext_cost];
    (gas_cost, ext_cost)
}

/// Estimates the cost to call `method`, but makes sure that `setup` is called
/// before.
///
/// Used for storage costs -- `setup` writes stuff into the storage, where
/// `method` can then find it. We take care to make sure that `setup` is run in
/// a separate block, to make sure we hit the database and not an in-memory hash
/// map.
fn fn_cost_with_setup(
    ctx: &mut EstimatorContext,
    setup: &str,
    method: &str,
    ext_cost: ExtCosts,
    count: u64,
) -> GasCost {
    let (total_cost, measured_count) = {
        let block_size = 2usize;
        let n_blocks = ctx.config.warmup_iters_per_block + ctx.config.iter_per_block;

        let mut testbed = ctx.testbed();

        let blocks = {
            let mut blocks = Vec::with_capacity(2 * n_blocks);
            for _ in 0..n_blocks {
                let tb = testbed.transaction_builder();
                let mut setup_block = Vec::new();
                let mut block = Vec::new();
                for _ in 0..block_size {
                    let sender = tb.random_unused_account();
                    let setup_tx =
                        tb.transaction_from_function_call(sender.clone(), setup, Vec::new());
                    let tx = tb.transaction_from_function_call(sender, method, Vec::new());

                    setup_block.push(setup_tx);
                    block.push(tx);
                }
                blocks.push(setup_block);
                blocks.push(block);
            }
            blocks
        };

        let measurements = testbed.measure_blocks(blocks);
        // Filter out setup blocks.
        let measurements: Vec<_> = measurements
            .into_iter()
            .skip(ctx.config.warmup_iters_per_block * 2)
            .enumerate()
            .filter(|(i, _)| i % 2 == 1)
            .map(|(_, m)| m)
            .collect();

        let (gas_cost, ext_costs) =
            aggregate_per_block_measurements(ctx.config, block_size, measurements);
        (gas_cost, ext_costs[&ext_cost])
    };
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

fn aggregate_per_block_measurements(
    config: &Config,
    block_size: usize,
    measurements: Vec<(GasCost, HashMap<ExtCosts, u64>)>,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let mut block_costs = Vec::new();
    let mut total_ext_costs: HashMap<ExtCosts, u64> = HashMap::new();
    let mut total = GasCost::zero(config.metric);
    let mut n = 0;
    for (gas_cost, ext_cost) in measurements {
        block_costs.push(gas_cost.to_gas() as f64);
        total += gas_cost;
        n += block_size as u64;
        for (c, v) in ext_cost {
            *total_ext_costs.entry(c).or_default() += v;
        }
    }
    for v in total_ext_costs.values_mut() {
        *v /= n;
    }
    let mut gas_cost = total / n;
    gas_cost.set_uncertain(is_high_variance(&block_costs));
    (gas_cost, total_ext_costs)
}

/// We expect our cost computations to be fairly reproducible, and just flag
/// "high-variance" measurements as suspicious. To make results easily
/// explainable, we just require that all the samples don't deviate from the
/// mean by more than 15%, where the number 15 is somewhat arbitrary.
///
/// Note that this looks at block processing times, and each block contains
/// multiples of things we are actually measuring. As low block variance doesn't
/// guarantee low within-block variance, this is necessary an approximate sanity
/// check.
fn is_high_variance(samples: &[f64]) -> bool {
    let threshold = 0.15;

    let mean = samples.iter().copied().sum::<f64>() / (samples.len() as f64);

    let all_below_threshold =
        samples.iter().copied().all(|it| (mean - it).abs() < mean * threshold);

    !all_below_threshold
}

pub fn read_resource(path: &str) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(dir).join(path);
    std::fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to load test resource: {}, {}", path.display(), err))
}
