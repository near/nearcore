mod support;

use std::collections::HashSet;
use std::convert::TryFrom;

use near_primitives::transaction::{
    Action, CreateAccountAction, DeleteAccountAction, SignedTransaction, TransferAction,
};
use near_primitives::types::AccountId;
use rand::Rng;

use crate::testbed_runners::Config;
use crate::v2::support::{Ctx, GasCost, TestBed};
use crate::{Cost, CostTable};

static ALL_COSTS: &[(Cost, fn(&mut Ctx) -> GasCost)] = &[
    (Cost::ActionReceiptCreation, action_receipt_creation),
    (Cost::ActionSirReceiptCreation, action_sir_receipt_creation),
    (Cost::ActionTransfer, action_transfer),
    (Cost::ActionCreateAccount, action_create_account),
    (Cost::ActionDeleteAccount, action_delete_account),
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

        let value = f(&mut ctx);
        res.add(cost, value.to_gas())
    }

    res
}

fn action_receipt_creation(ctx: &mut Ctx) -> GasCost {
    if let Some(cached) = ctx.cached.action_receipt_creation.clone() {
        return cached;
    }

    let mut testbed = ctx.test_bed();

    let mut make_transaction = |tb: &mut TestBed| -> SignedTransaction {
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

    let mut make_transaction = |tb: &mut TestBed<'_>| -> SignedTransaction {
        let sender = tb.random_account();
        let receiver = sender.clone();
        tb.transaction_from_actions(sender, receiver, vec![])
    };
    let cost = testbed.average_transaction_cost(&mut make_transaction);

    ctx.cached.action_sir_receipt_creation = Some(cost.clone());
    cost
}

fn action_transfer(ctx: &mut Ctx) -> GasCost {
    let mut testbed = ctx.test_bed();

    let mut make_transaction = |tb: &mut TestBed<'_>| -> SignedTransaction {
        let sender = tb.random_account();
        let receiver = tb.random_account();
        tb.transaction_from_actions(
            sender,
            receiver,
            vec![Action::Transfer(TransferAction { deposit: 1 })],
        )
    };
    testbed.average_transaction_cost(&mut make_transaction)
}

fn action_create_account(ctx: &mut Ctx) -> GasCost {
    let total_cost = {
        let mut testbed = ctx.test_bed();

        let mut make_transaction = |tb: &mut TestBed<'_>| -> SignedTransaction {
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
        let mut make_transaction = |tb: &mut TestBed<'_>| -> SignedTransaction {
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
        metrics_to_measure: None,
    };
    let table = run(config);
    eprintln!("{}", table);
}
