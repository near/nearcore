use std::collections::HashMap;
use std::time::Duration;
use std::{fmt, iter, ops};

use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, Gas};
use rand::prelude::ThreadRng;
use rand::Rng;

use crate::cases::ratio_to_gas;
use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{end_count, get_account_id, start_count, Config, GasMetric};

#[derive(Default)]
pub(crate) struct CachedCosts {
    pub(crate) action_receipt_creation: Option<GasCost>,
    pub(crate) action_sir_receipt_creation: Option<GasCost>,
}

pub(crate) struct Ctx<'a> {
    pub(crate) config: &'a Config,
    pub(crate) cached: CachedCosts,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(config: &'a Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    pub(crate) fn test_bed(&mut self) -> TestBed<'_> {
        let inner = RuntimeTestbed::from_state_dump(&self.config.state_dump_path);
        TestBed { config: &self.config, inner, nonces: HashMap::new() }
    }
}

pub(crate) struct TestBed<'a> {
    config: &'a Config,
    inner: RuntimeTestbed,
    nonces: HashMap<AccountId, u64>,
}

impl TestBed<'_> {
    pub(crate) fn rng(&mut self) -> ThreadRng {
        rand::thread_rng()
    }

    fn nonce(&mut self, account_id: &AccountId) -> u64 {
        let nonce = self.nonces.entry(account_id.clone()).or_default();
        *nonce += 1;
        *nonce
    }

    pub(crate) fn random_account(&mut self) -> AccountId {
        self.random_accounts().next().unwrap()
    }

    pub(crate) fn random_accounts<'a>(&'a mut self) -> impl Iterator<Item = AccountId> + 'a {
        let active_accounts = self.config.active_accounts;
        let mut rng = self.rng();
        iter::repeat_with(move || {
            let account_index = rng.gen_range(0, active_accounts);
            get_account_id(account_index)
        })
    }

    pub(crate) fn transaction_from_actions(
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

    pub(crate) fn average_transaction_cost(
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
pub(crate) struct GasCost {
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
    pub(crate) fn to_gas(self) -> Gas {
        ratio_to_gas(self.metric, self.value.into())
    }
}
