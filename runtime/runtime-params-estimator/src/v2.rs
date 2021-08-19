use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::time::Duration;
use std::{fmt, iter, ops};

use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, CreateAccountAction, DeleteAccountAction, SignedTransaction, TransferAction,
};
use near_primitives::types::{AccountId, Gas};
use rand::rngs::ThreadRng;
use rand::Rng;

use crate::cases::ratio_to_gas;
use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{end_count, get_account_id, start_count, Config, GasMetric};
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

#[derive(Default)]
struct CachedCosts {
    action_receipt_creation: Option<GasCost>,
    action_sir_receipt_creation: Option<GasCost>,
}

struct Ctx<'a> {
    config: &'a Config,
    cached: CachedCosts,
}

impl<'a> Ctx<'a> {
    fn new(config: &'a Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    fn test_bed(&mut self) -> TestBed<'_> {
        let inner = RuntimeTestbed::from_state_dump(&self.config.state_dump_path);
        TestBed { config: &self.config, inner, nonces: HashMap::new() }
    }
}

struct TestBed<'a> {
    config: &'a Config,
    inner: RuntimeTestbed,
    nonces: HashMap<AccountId, u64>,
}

impl TestBed<'_> {
    fn rng(&mut self) -> ThreadRng {
        rand::thread_rng()
    }

    fn nonce(&mut self, account_id: &AccountId) -> u64 {
        let nonce = self.nonces.entry(account_id.clone()).or_default();
        *nonce += 1;
        *nonce
    }

    fn random_account(&mut self) -> AccountId {
        self.random_accounts().next().unwrap()
    }

    fn random_accounts<'a>(&'a mut self) -> impl Iterator<Item = AccountId> + 'a {
        let active_accounts = self.config.active_accounts;
        let mut rng = self.rng();
        iter::repeat_with(move || {
            let account_index = rng.gen_range(0, active_accounts);
            get_account_id(account_index)
        })
    }

    fn transaction_from_actions(
        &mut self,
        sender: AccountId,
        receiver: AccountId,
        actions: Vec<Action>,
    ) -> SignedTransaction {
        let signer = InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, sender.as_ref());
        let nonce = self.nonce(&sender);

        SignedTransaction::from_actions(
            nonce as u64,
            sender.clone(),
            receiver,
            &signer,
            actions,
            CryptoHash::default(),
        )
    }

    fn average_transaction_cost(
        &mut self,
        make_transaction: &mut dyn FnMut(&mut Self) -> SignedTransaction,
    ) -> GasCost {
        let allow_failures = false;

        if self.config.warmup_iters_per_block > 0 {
            let block_size = 100;
            for _ in 0..self.config.warmup_iters_per_block {
                let block: Vec<_> =
                    iter::repeat_with(|| make_transaction(self)).take(block_size).collect();
                self.inner.process_block(&block, allow_failures);
            }
            self.inner.process_blocks_until_no_receipts(allow_failures);
        }

        let mut total = 0;
        let mut n = 0;
        for _ in 0..self.config.iter_per_block {
            let block_size = 100;
            let block: Vec<_> =
                iter::repeat_with(|| make_transaction(self)).take(block_size).collect();

            let start = start_count(self.config.metric);
            self.inner.process_block(&block, allow_failures);
            self.inner.process_blocks_until_no_receipts(allow_failures);
            let measured = end_count(self.config.metric, &start);

            total += measured;
            n += block_size as u64;
        }

        GasCost { value: total / n, metric: self.config.metric }
    }
}

#[derive(Clone)]
struct GasCost {
    value: u64,
    metric: GasMetric,
}

impl fmt::Debug for GasCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.metric {
            GasMetric::ICount => write!(f, "{}i", self.value),
            GasMetric::Time => fmt::Debug::fmt(&Duration::from_nanos(self.value), f),
        }
    }
}

impl ops::Sub for GasCost {
    type Output = GasCost;

    fn sub(self, rhs: GasCost) -> Self::Output {
        assert_eq!(self.metric, rhs.metric);
        GasCost { value: self.value - rhs.value, metric: self.metric }
    }
}

impl GasCost {
    fn to_gas(self) -> Gas {
        ratio_to_gas(self.metric, self.value.into())
    }
}

#[test]
fn smoke() {
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
