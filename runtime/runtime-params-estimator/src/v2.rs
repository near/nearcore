mod support;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::time::Instant;

use near_crypto::{KeyType, PublicKey, SecretKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    SignedTransaction, StakeAction, TransferAction,
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
        let sender = tb.random_account();
        let receiver = tb.random_account();
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
            let sender = tb.random_account();
            let receiver = tb.random_account();
            tb.transaction_from_actions(
                sender,
                receiver,
                vec![Action::Transfer(TransferAction { deposit: 1 })],
            )
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
            let existing_account = tb.random_account();
            let new_account =
                AccountId::try_from(format!("{}_{}", existing_account, tb.rng().gen::<u64>()))
                    .unwrap();

            tb.transaction_from_actions(
                existing_account,
                new_account,
                vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::Transfer(TransferAction { deposit: 10u128.pow(26) }),
                ],
            )
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_delete_account(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut deleted_accounts = HashSet::new();
        let mut beneficiaries = HashSet::new();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let existing_account = tb
                .random_accounts()
                .find(|it| !deleted_accounts.contains(it) && !beneficiaries.contains(it))
                .unwrap();
            deleted_accounts.insert(existing_account.clone());

            let beneficiary_id =
                tb.random_accounts().find(|it| !deleted_accounts.contains(it)).unwrap();
            beneficiaries.insert(beneficiary_id.clone());

            tb.transaction_from_actions(
                existing_account.clone(),
                existing_account,
                vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
            )
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
}

fn action_add_full_access_key(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut used_accounts = HashSet::new();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_accounts().find(|it| !used_accounts.contains(it)).unwrap();
            used_accounts.insert(sender.clone());

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

        let mut used_accounts = HashSet::new();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_accounts().find(|it| !used_accounts.contains(it)).unwrap();
            used_accounts.insert(sender.clone());

            let receiver_id = tb.account(0).to_string();

            add_key_transaction(
                tb,
                sender,
                AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id,
                    method_names: vec!["method1".to_string()],
                }),
            )
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
        let mut used_accounts = HashSet::new();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let sender = tb.random_accounts().find(|it| !used_accounts.contains(it)).unwrap();
            used_accounts.insert(sender.clone());

            let receiver_id = tb.account(0).to_string();

            add_key_transaction(
                tb,
                sender,
                AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id,
                    method_names: many_methods.clone(),
                }),
            )
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

        let mut deleted_accounts = HashSet::new();
        let mut make_transaction = |mut tb: TransactionBuilder<'_, '_>| -> SignedTransaction {
            let existing_account =
                tb.random_accounts().find(|it| !deleted_accounts.contains(it)).unwrap();
            deleted_accounts.insert(existing_account.clone());

            let public_key =
                SecretKey::from_seed(KeyType::ED25519, existing_account.as_ref()).public_key();

            tb.transaction_from_actions(
                existing_account.clone(),
                existing_account,
                vec![Action::DeleteKey(DeleteKeyAction { public_key })],
            )
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
            let sender = tb.random_account();
            let receiver = sender.clone();

            let public_key: PublicKey =
                "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap();

            tb.transaction_from_actions(
                sender,
                receiver,
                vec![Action::Stake(StakeAction { stake: 1, public_key })],
            )
        };
        testbed.average_transaction_cost(&mut make_transaction)
    };

    let base_cost = action_sir_receipt_creation(ctx);

    total_cost - base_cost
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
        metrics_to_measure: Some(vec!["ActionStake".to_string()]),
    };
    let table = run(config);
    eprintln!("{}", table);
}
