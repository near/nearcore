mod support;

use std::convert::TryFrom;
use std::time::Instant;

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
use num_rational::Ratio;
use rand::Rng;

use crate::cost_table::format_gas;
use crate::testbed_runners::{end_count, start_count, Config};
use crate::v2::support::{Ctx, GasCost};
use crate::vm_estimator::create_context;
use crate::{Cost, CostTable};

use self::support::TransactionBuilder;

static ALL_COSTS: &[(Cost, fn(&mut Ctx) -> GasCost)] = &[
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
    //
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
];

pub fn run(config: Config) -> CostTable {
    let mut ctx = Ctx::new(&config);
    let mut res = CostTable::default();

    for (cost, f) in ALL_COSTS.iter().copied() {
        let skip = match &ctx.config.metrics_to_measure {
            None => false,
            Some(costs) => !costs.contains(&format!("{:?}", cost)),
        };
        if skip {
            continue;
        }

        let start = Instant::now();
        let value = f(&mut ctx);
        let gas = value.to_gas();
        res.add(cost, gas);
        eprintln!(
            "{:<40} {:>25} gas  (computed in {:.2?})",
            cost.to_string(),
            format_gas(gas),
            start.elapsed()
        );
    }
    eprintln!();

    res
}

fn action_receipt_creation(ctx: &mut Ctx) -> GasCost {
    if let Some(cached) = ctx.cached.action_receipt_creation.clone() {
        return cached;
    }

    let mut testbed = ctx.test_bed();

    let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
        let (sender, receiver) = tb.random_account_pair();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let cost = testbed.average_transaction_cost(&mut make_transaction);

    ctx.cached.action_receipt_creation = Some(cost.clone());
    cost
}

fn action_sir_receipt_creation(ctx: &mut Ctx) -> GasCost {
    if let Some(cached) = ctx.cached.action_sir_receipt_creation.clone() {
        return cached;
    }

    let mut testbed = ctx.test_bed();

    let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
        let sender = tb.random_account();
        let receiver = sender.clone();

        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let cost = testbed.average_transaction_cost(&mut make_transaction);

    ctx.cached.action_sir_receipt_creation = Some(cost.clone());
    cost
}

fn action_transfer(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let (sender, receiver) = tb.random_account_pair();

            let actions = vec![Action::Transfer(TransferAction { deposit: 1 })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_create_account(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_account();
            let new_account =
                AccountId::try_from(format!("{}_{}", sender, tb.rng().gen::<u64>())).unwrap();

            let actions = vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::Transfer(TransferAction { deposit: 10u128.pow(26) }),
            ];
            tb.transaction_from_actions(sender, new_account, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_delete_account(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();
            let beneficiary_id = tb.random_unused_account();

            let actions = vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_add_full_access_key(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();

            add_key_transaction(tb, sender, AccessKeyPermission::FullAccess)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_add_function_access_key_base(ctx: &mut Ctx) -> GasCost {
    if let Some(cost) = ctx.cached.action_add_function_access_key_base.clone() {
        return cost;
    }

    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver_id = tb.account(0).to_string();

            let permission = AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(100),
                receiver_id,
                method_names: vec!["method1".to_string()],
            });
            add_key_transaction(tb, sender, permission)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost - base_cost;
    ctx.cached.action_add_function_access_key_base = Some(cost.clone());
    cost
}

fn action_add_function_access_key_per_byte(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let many_methods: Vec<_> = (0..1000).map(|i| format!("a123456{:03}", i)).collect();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver_id = tb.account(0).to_string();

            let permission = AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(100),
                receiver_id,
                method_names: many_methods.clone(),
            });
            add_key_transaction(tb, sender, permission)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_add_function_access_key_base(ctx);

    // 1k methods for 10 bytes each
    let bytes_per_transaction = 10 * 1000;

    (total_cost - base_cost) / bytes_per_transaction
}

fn add_key_transaction(
    tb: TransactionBuilder<'_, '_>,
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

fn action_delete_key(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::DeleteKey(DeleteKeyAction {
                public_key: SecretKey::from_seed(KeyType::ED25519, sender.as_ref()).public_key(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_stake(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::Stake(StakeAction {
                stake: 1,
                public_key: "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap(),
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_deploy_contract_base(ctx: &mut Ctx) -> GasCost {
    if let Some(cost) = ctx.cached.action_deploy_contract_base.clone() {
        return cost;
    }

    let total_cost = {
        let code = ctx.read_resource("test-contract/res/smallest_contract.wasm");
        deploy_contract_cost(ctx, code)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost - base_cost;
    ctx.cached.action_deploy_contract_base = Some(cost.clone());
    cost
}

fn action_deploy_contract_per_byte(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let code = ctx.read_resource(if cfg!(feature = "nightly_protocol_features") {
            "test-contract/res/nightly_large_contract.wasm"
        } else {
            "test-contract/res/stable_large_contract.wasm"
        });
        deploy_contract_cost(ctx, code)
    };

    let base_cost = action_deploy_contract_base(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn action_function_call_base(ctx: &mut Ctx) -> GasCost {
    let total_cost = noop_host_function_call_cost(ctx);
    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_function_call_per_byte(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", vec![0; 1024 * 1024])
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = noop_host_function_call_cost(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn data_receipt_creation_base(ctx: &mut Ctx) -> GasCost {
    let (total_cost, _) = fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base);
    let (base_cost, _) = fn_cost_count(ctx, "data_receipt_base_10b_1000", ExtCosts::base);

    total_cost - base_cost
}

fn data_receipt_creation_per_byte(ctx: &mut Ctx) -> GasCost {
    let (total_cost, _) = fn_cost_count(ctx, "data_receipt_100kib_1000", ExtCosts::base);
    let (base_cost, _) = fn_cost_count(ctx, "data_receipt_10b_1000", ExtCosts::base);

    let bytes_per_transaction = 1000 * 100 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn host_function_call(ctx: &mut Ctx) -> GasCost {
    let (total_cost, count) = fn_cost_count(ctx, "base_1M", ExtCosts::base);
    assert_eq!(count, 1_000_000);

    let base_cost = noop_host_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

fn wasm_instruction(ctx: &mut Ctx) -> GasCost {
    let vm_kind = ctx.config.vm_kind;

    let code = ctx.read_resource(if cfg!(feature = "nightly_protocol_features") {
        "test-contract/res/nightly_large_contract.wasm"
    } else {
        "test-contract/res/stable_large_contract.wasm"
    });

    let n_iters = 10;

    let code = ContractCode::new(code.to_vec(), None);
    let mut fake_external = MockedExternal::new();
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];

    let mut run = || {
        let context = create_context(vec![]);
        let (outcome, err) = near_vm_runner::run_vm(
            &code,
            "cpu_ram_soak_test",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            PROTOCOL_VERSION,
            None,
        );
        match (outcome, err) {
            (Some(it), Some(_)) => it,
            _ => panic!(),
        }
    };

    let warmup_outcome = run();

    let start = start_count(ctx.config.metric);
    for _ in 0..n_iters {
        run();
    }
    let total = end_count(ctx.config.metric, &start);
    let total = Ratio::from_integer(total);

    let instructions_per_iter = {
        let op_cost = config.regular_op_cost as u64;
        warmup_outcome.burnt_gas / op_cost
    };

    let per_instruction = total / (instructions_per_iter * n_iters);
    GasCost { value: per_instruction, metric: ctx.config.metric }
}

fn read_memory_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "read_memory_10b_10k", ExtCosts::read_memory_base, 10_000)
}
fn read_memory_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "read_memory_1Mib_10k", ExtCosts::read_memory_byte, 1024 * 1024 * 10_000)
}

fn write_memory_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "write_memory_10b_10k", ExtCosts::write_memory_base, 10_000)
}
fn write_memory_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "write_memory_1Mib_10k", ExtCosts::write_memory_byte, 1024 * 1024 * 10_000)
}

fn read_register_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "read_register_10b_10k", ExtCosts::read_register_base, 10_000)
}
fn read_register_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "read_register_1Mib_10k", ExtCosts::read_register_byte, 1024 * 1024 * 10_000)
}

fn write_register_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "write_register_10b_10k", ExtCosts::write_register_base, 10_000)
}
fn write_register_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "write_register_1Mib_10k", ExtCosts::write_register_byte, 1024 * 1024 * 10_000)
}

fn log_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "utf16_log_10b_10k", ExtCosts::log_base, 10_000)
}
fn log_byte(ctx: &mut Ctx) -> GasCost {
    // NOTE: We are paying per *output* byte here, hence 3/2 multiplier.
    fn_cost(ctx, "utf16_log_10kib_10k", ExtCosts::log_byte, (10 * 1024 * 3 / 2) * 10_000)
}

fn utf8_decoding_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "utf8_log_10b_10k", ExtCosts::utf8_decoding_base, 10_000)
}
fn utf8_decoding_byte(ctx: &mut Ctx) -> GasCost {
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

fn utf16_decoding_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "utf16_log_10b_10k", ExtCosts::utf16_decoding_base, 10_000)
}
fn utf16_decoding_byte(ctx: &mut Ctx) -> GasCost {
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

fn sha256_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "sha256_10b_10k", ExtCosts::sha256_base, 10_000)
}
fn sha256_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "sha256_10kib_10k", ExtCosts::sha256_byte, 10 * 1024 * 10_000)
}

fn keccak256_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "keccak256_10b_10k", ExtCosts::keccak256_base, 10_000)
}
fn keccak256_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "keccak256_10kib_10k", ExtCosts::keccak256_byte, 10 * 1024 * 10_000)
}

fn keccak512_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "keccak512_10b_10k", ExtCosts::keccak512_base, 10_000)
}
fn keccak512_byte(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "keccak512_10kib_10k", ExtCosts::keccak512_byte, 10 * 1024 * 10_000)
}

fn ripemd160_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "ripemd160_10b_10k", ExtCosts::ripemd160_base, 10_000)
}
fn ripemd160_block(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "ripemd160_10kib_10k", ExtCosts::ripemd160_block, (10 * 1024 / 64 + 1) * 10_000)
}

fn ecrecover_base(ctx: &mut Ctx) -> GasCost {
    fn_cost(ctx, "ecrecover_10k", ExtCosts::ecrecover_base, 10_000)
}

// Helpers

fn deploy_contract_cost(ctx: &mut Ctx, code: Vec<u8>) -> GasCost {
    let mut testbed = ctx.test_bed();

    let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
        let sender = tb.random_unused_account();
        let receiver = sender.clone();

        let actions = vec![Action::DeployContract(DeployContractAction { code: code.clone() })];
        tb.transaction_from_actions(sender, receiver, actions)
    };
    testbed.average_transaction_cost(&mut make_transaction)
}

fn noop_host_function_call_cost(ctx: &mut Ctx) -> GasCost {
    if let Some(cost) = ctx.cached.noop_host_function_call_cost.clone() {
        return cost;
    }

    let cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", Vec::new())
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    ctx.cached.noop_host_function_call_cost = Some(cost.clone());
    cost
}

fn fn_cost(ctx: &mut Ctx, method: &str, ext_cost: ExtCosts, count: u64) -> GasCost {
    let (total_cost, measured_count) = fn_cost_count(ctx, method, ext_cost);
    assert_eq!(measured_count, count);

    let base_cost = noop_host_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

fn fn_cost_count(ctx: &mut Ctx, method: &str, ext_cost: ExtCosts) -> (GasCost, u64) {
    let mut testbed = ctx.test_bed_with_contracts().block_size(2);

    let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
        let sender = tb.random_unused_account();
        tb.transaction_from_function_call(sender, method, Vec::new())
    };
    let (gas_cost, ext_costs) = testbed.average_transaction_cost_with_ext(&mut make_transaction);
    let ext_cost = ext_costs[&ext_cost];
    (gas_cost, ext_cost)
}

#[test]
fn smoke() {
    use crate::testbed_runners::GasMetric;
    use nearcore::get_default_home;

    let metrics = ["EcrecoverBase"];
    let config = Config {
        warmup_iters_per_block: 1,
        iter_per_block: 2,
        active_accounts: 20000,
        block_sizes: vec![100],
        state_dump_path: get_default_home().into(),
        metric: GasMetric::Time,
        vm_kind: near_vm_runner::VMKind::Wasmer0,
        metrics_to_measure: Some(metrics.iter().map(|it| it.to_string()).collect::<Vec<_>>())
            .filter(|it| !it.is_empty()),
    };
    let table = run(config);
    eprintln!("{}", table);
}
