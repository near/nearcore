mod support;

use std::convert::TryFrom;
use std::time::Instant;

use near_crypto::{KeyType, SecretKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use near_primitives::types::AccountId;
use rand::Rng;

use crate::testbed_runners::Config;
use crate::v2::support::{Ctx, GasCost};
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
    (Cost::DataReceiptCreationBase, data_receipt_creation_base),
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
        eprint!("{:<40} ", format!("{:?} ...", cost));
        let value = f(&mut ctx);
        res.add(cost, value.to_gas());
        eprintln!("{:.2?}", start.elapsed());
    }

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
        deploy_contract(ctx, code)
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
        deploy_contract(ctx, code)
    };

    let base_cost = action_deploy_contract_base(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn deploy_contract(ctx: &mut Ctx, code: Vec<u8>) -> GasCost {
    let mut testbed = ctx.test_bed();

    let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
        let sender = tb.random_unused_account();
        let receiver = sender.clone();

        let actions = vec![Action::DeployContract(DeployContractAction { code: code.clone() })];
        tb.transaction_from_actions(sender, receiver, actions)
    };
    testbed.average_transaction_cost(&mut make_transaction)
}

fn action_function_call_base(ctx: &mut Ctx) -> GasCost {
    if let Some(cost) = ctx.cached.action_function_call_base.clone() {
        return cost;
    }

    let total_cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::FunctionCall(FunctionCallAction {
                method_name: "noop".to_string(),
                args: vec![],
                gas: 10u64.pow(18),
                deposit: 0,
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    let cost = total_cost - base_cost;
    ctx.cached.action_function_call_base = Some(cost.clone());
    cost
}

fn action_function_call_per_byte(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::FunctionCall(FunctionCallAction {
                method_name: "noop".to_string(),
                args: vec![0; 1024 * 1024],
                gas: 10u64.pow(18),
                deposit: 0,
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_function_call_base(ctx);

    let bytes_per_transaction = 1024 * 1024;

    (total_cost - base_cost) / bytes_per_transaction
}

fn data_receipt_creation_base(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::FunctionCall(FunctionCallAction {
                method_name: "data_receipt_10b_1000".to_string(),
                args: vec![],
                gas: 10u64.pow(18),
                deposit: 0,
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = {
        let mut testbed = ctx.test_bed_with_contracts();

        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_unused_account();
            let receiver = sender.clone();

            let actions = vec![Action::FunctionCall(FunctionCallAction {
                method_name: "data_receipt_base_10b_1000".to_string(),
                args: vec![],
                gas: 10u64.pow(18),
                deposit: 0,
            })];
            tb.transaction_from_actions(sender, receiver, actions)
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    (total_cost - base_cost)
}

#[test]
fn smoke() {
    use crate::testbed_runners::GasMetric;
    use nearcore::get_default_home;

    let config = Config {
        warmup_iters_per_block: 1,
        iter_per_block: 2,
        active_accounts: 20000,
        block_sizes: vec![100],
        state_dump_path: get_default_home().into(),
        metric: GasMetric::Time,
        vm_kind: near_vm_runner::VMKind::Wasmer0,
        metrics_to_measure: Some(vec!["DataReceiptCreationBase".to_string()]),
    };
    let table = run(config);
    eprintln!("{}", table);
}
