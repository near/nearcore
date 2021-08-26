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
    pub(crate) action_add_function_access_key_base: Option<GasCost>,
}

/// Global context shared by all cost calculating functions.
pub(crate) struct Ctx<'c> {
    pub(crate) config: &'c Config,
    pub(crate) cached: CachedCosts,
}

impl<'c> Ctx<'c> {
    pub(crate) fn new(config: &'c Config) -> Self {
        let cached = CachedCosts::default();
        Self { cached, config }
    }

    pub(crate) fn test_bed(&mut self) -> TestBed<'_> {
        let inner = RuntimeTestbed::from_state_dump(&self.config.state_dump_path);
        TestBed { config: &self.config, inner, nonces: HashMap::new() }
    }
}

/// A single isolated instance of near.
///
/// We use it to time processing a bunch of blocks.
pub(crate) struct TestBed<'c> {
    config: &'c Config,
    inner: RuntimeTestbed,
    nonces: HashMap<AccountId, u64>,
}

impl<'c> TestBed<'c> {
    fn nonce(&mut self, account_id: &AccountId) -> u64 {
        let nonce = self.nonces.entry(account_id.clone()).or_default();
        *nonce += 1;
        *nonce
    }

    pub(crate) fn average_transaction_cost<'a>(
        &'a mut self,
        make_transaction: &'a mut dyn FnMut(TransactionBuilder<'_, '_>) -> SignedTransaction,
    ) -> GasCost {
        let allow_failures = false;

        let total_iters = self.config.warmup_iters_per_block + self.config.iter_per_block;

        let mut total = 0;
        let mut n = 0;
        for iter in 0..total_iters {
            let block_size = 100;
            let block: Vec<_> = iter::repeat_with(|| {
                let tb = TransactionBuilder { testbed: self };
                make_transaction(tb)
            })
            .take(block_size)
            .collect();

            let start = start_count(self.config.metric);
            self.inner.process_block(&block, allow_failures);
            self.inner.process_blocks_until_no_receipts(allow_failures);
            let measured = end_count(self.config.metric, &start);

            let is_warmup = iter < self.config.warmup_iters_per_block;

            if !is_warmup {
                total += measured;
                n += block_size as u64;
            }
        }

        GasCost { value: total / n, metric: self.config.metric }
    }
}

/// A helper to create transaction for processing by a `TestBed`.
///
/// Physically, this *is* a `TestBed`, just with a restricted interface
/// specifically for creating a transaction struct.
pub(crate) struct TransactionBuilder<'a, 'c> {
    testbed: &'a mut TestBed<'c>,
}

impl<'a, 'c> TransactionBuilder<'a, 'c> {
    pub(crate) fn transaction_from_actions(
        self,
        sender: AccountId,
        receiver: AccountId,
        actions: Vec<Action>,
    ) -> SignedTransaction {
        let signer = InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, sender.as_ref());
        let nonce = self.testbed.nonce(&sender);

        SignedTransaction::from_actions(
            nonce as u64,
            sender.clone(),
            receiver,
            &signer,
            actions,
            CryptoHash::default(),
        )
    }

    pub(crate) fn rng(&mut self) -> ThreadRng {
        rand::thread_rng()
    }

    pub(crate) fn account(&mut self, idx: usize) -> AccountId {
        get_account_id(idx)
    }

    pub(crate) fn random_account(&mut self) -> AccountId {
        self.random_accounts().next().unwrap()
    }

    pub(crate) fn random_accounts<'b>(&'b mut self) -> impl Iterator<Item = AccountId> + 'b {
        let active_accounts = self.testbed.config.active_accounts;
        let mut rng = self.rng();
        iter::repeat_with(move || {
            let account_index = rng.gen_range(0, active_accounts);
            get_account_id(account_index)
        })
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

impl ops::Div<u64> for GasCost {
    type Output = GasCost;

    fn div(self, rhs: u64) -> Self::Output {
        GasCost { value: self.value / rhs, metric: self.metric }
    }
}

impl GasCost {
    pub(crate) fn to_gas(self) -> Gas {
        ratio_to_gas(self.metric, self.value.into())
    }
}
