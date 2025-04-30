use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_client::ProcessTxRequest;
use near_client::client_actor::ClientActorInner;
use near_crypto::Signer;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo, Balance, EpochId, Nonce, NumSeats};

use crate::setup::env::TestLoopEnv;
use crate::utils::transactions;

/// Allows running with validators rotating on each epoch.
pub struct RotatingValidatorsRunner {
    validators: Vec<Vec<Validator>>,
    validators_index: usize,
    max_stake: u128,

    assert_validation_rotation: bool,
    max_epoch_duration: Option<Duration>,
}

impl RotatingValidatorsRunner {
    /// Created new runner that allows running with rotating validators. It works by changing
    /// stakes on epoch change.
    pub fn new(max_stake: u128, validators: Vec<Vec<AccountId>>) -> Self {
        assert!(validators.len() > 1);
        let validators = validators
            .into_iter()
            .map(|vec| {
                vec.into_iter()
                    .map(|account| Validator { signer: create_user_test_signer(&account), account })
                    .collect()
            })
            .collect();
        Self {
            validators,
            validators_index: 1,
            max_stake,
            assert_validation_rotation: true,
            max_epoch_duration: None,
        }
    }

    fn restake_validators_txs(&self, env: &TestLoopEnv, index: usize) -> Vec<SignedTransaction> {
        let stake_txs: Vec<_> = self
            .validators
            .iter()
            .enumerate()
            .flat_map(|(i, vec)| {
                vec.iter().enumerate().map(move |(j, v)| {
                    if i == index {
                        // Using max_stake - i for stake allows some of the validators to be block
                        // and chunk producers while keeping others as chunk validators only.
                        v.stake_tx(&env, self.max_stake - u128::try_from(j).unwrap())
                    } else {
                        v.stake_tx(&env, 0)
                    }
                })
            })
            .collect();
        stake_txs
    }

    #[track_caller]
    fn check_validators_at_epoch_eq(
        &self,
        client: &near_client::Client,
        epoch_id: &EpochId,
        validators: &[Validator],
        msg: &str,
    ) {
        let current_validators: Vec<_> = client
            .epoch_manager
            .get_epoch_all_validators(&epoch_id)
            .unwrap()
            .into_iter()
            .map(|v| {
                let (account, _, _) = v.destructure();
                account
            })
            .sorted()
            .collect();
        let validators: Vec<_> = validators.iter().map(|v| v.account.clone()).sorted().collect();
        if self.assert_validation_rotation {
            assert_eq!(current_validators, validators, "{msg}");
        } else if current_validators != validators {
            tracing::warn!(
                "{msg}; wrong validators for epoch {epoch_id:?}: current_validators={current_validators:?}; want_validators={validators:?}"
            );
        }
    }

    fn client_actor_handle(env: &TestLoopEnv) -> TestLoopDataHandle<ClientActorInner> {
        env.node_datas[0].client_sender.actor_handle()
    }

    fn client<'a>(
        data: &'a TestLoopData,
        client_actor_handle: &TestLoopDataHandle<ClientActorInner>,
    ) -> &'a near_client::Client {
        &data.get(client_actor_handle).client
    }

    /// When running skips asserting that validators are being rotated. Can be useful for tests
    /// that cause blocks to be skipped making epoch switches less predictable.
    pub fn skip_assert_validators_rotation(&mut self) {
        self.assert_validation_rotation = false
    }

    pub fn run_for_an_epoch(&mut self, env: &mut TestLoopEnv) {
        self.run_for_an_epoch_with_condition(env, &mut |_| false);
    }

    /// Run for an epoch rotating validators.
    fn run_for_an_epoch_with_condition(
        &mut self,
        env: &mut TestLoopEnv,
        condition: &mut impl FnMut(&mut TestLoopData) -> bool,
    ) {
        let client_actor_handle = &Self::client_actor_handle(&env);
        let client = Self::client(&env.test_loop.data, client_actor_handle);
        let epoch_id = client.chain.head().unwrap().epoch_id;
        let next_epoch_id = client.chain.head().unwrap().next_epoch_id;

        self.assert_current_validators_are_known(client, epoch_id, next_epoch_id);

        let tx_processor_senders: Vec<_> =
            env.node_datas.iter().map(|data| data.rpc_handler_sender.clone()).collect();

        let txs = self.restake_validators_txs(&env, self.validators_index);
        for tx in &txs {
            // In case some nodes are misbehaving we are sending transaction to each one.
            for tx_processor_sender in &tx_processor_senders {
                let process_tx_request = ProcessTxRequest {
                    transaction: tx.clone(),
                    is_forwarded: false,
                    check_only: false,
                };
                tx_processor_sender.send(process_tx_request);
            }
        }

        let epoch_length = client.epoch_manager.get_epoch_config(&epoch_id).unwrap().epoch_length;
        env.test_loop.run_until(
            |test_loop_data| {
                if condition(test_loop_data) {
                    return true;
                }

                let client = Self::client(&test_loop_data, client_actor_handle);
                let epoch_id = client.chain.final_head().unwrap().epoch_id;
                let epoch_changed = epoch_id == next_epoch_id;
                epoch_changed
            },
            self.max_epoch_duration(epoch_length),
        );
        // If run_until finished early we cannot assert validator change.
        if condition(&mut env.test_loop.data) {
            return;
        }

        let client = Self::client(&env.test_loop.data, client_actor_handle);
        let next_epoch_id = client.chain.head().unwrap().next_epoch_id;

        self.check_validators_at_epoch_eq(
            client,
            &next_epoch_id,
            &self.validators[self.validators_index],
            "failed to switch validators",
        );
        self.validators_index = (self.validators_index + 1) % self.validators.len();
    }

    /// Sets amount of time we would wait for an epoch change.
    pub fn set_max_epoch_duration(&mut self, duration: Duration) {
        self.max_epoch_duration = Some(duration);
    }

    fn max_epoch_duration(&self, epoch_length: u64) -> Duration {
        self.max_epoch_duration
            .unwrap_or_else(|| Duration::seconds(i64::try_from(10 + epoch_length).unwrap()))
    }

    fn assert_current_validators_are_known(
        &self,
        client: &near_client::Client,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
    ) {
        let mut validators: HashSet<_> = client
            .epoch_manager
            .get_epoch_all_validators(&epoch_id)
            .unwrap()
            .into_iter()
            .chain(client.epoch_manager.get_epoch_all_validators(&next_epoch_id).unwrap())
            .map(|stake| stake.destructure().0)
            .collect();
        for known_validator in self.validators.iter().flatten() {
            validators.remove(&known_validator.account);
        }
        assert_eq!(
            validators.len(),
            0,
            "this or next epoch contain validators not know to the RotatingValidatorsRunner. {validators:?}"
        );
    }

    /// Run for an epoch at a time stopping once condition returns true.
    pub fn run_until(
        &mut self,
        env: &mut TestLoopEnv,
        mut condition: impl FnMut(&mut TestLoopData) -> bool,
        maximum_duration: Duration,
    ) {
        let deadline = env.test_loop.clock().now() + maximum_duration;
        loop {
            if env.test_loop.clock().now() >= deadline {
                panic!(
                    "RotatingValidatorsRunner run_until did not fulfill the condition within the given deadline"
                );
            }

            self.run_for_an_epoch_with_condition(env, &mut condition);
            if condition(&mut env.test_loop.data) {
                break;
            }
        }
    }

    /// Returns validator spec for genesis that is consistent with runner's expectations.
    pub fn genesis_validators_spec(
        &self,
        num_block_producer_seats: NumSeats,
        num_chunk_producer_seats: NumSeats,
        num_chunk_validator_only_seats: NumSeats,
    ) -> ValidatorsSpec {
        ValidatorsSpec::raw(
            self.validators[0]
                .iter()
                .enumerate()
                .map(|(i, v)| AccountInfo {
                    account_id: v.account.clone(),
                    public_key: v.signer.public_key(),
                    amount: self.max_stake - u128::try_from(i).unwrap(),
                })
                .collect(),
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_only_seats,
        )
    }

    /// Returns accounts of all validators.
    pub fn all_validators_accounts(&self) -> Vec<AccountId> {
        self.validators.iter().flatten().map(|v| v.account.clone()).collect()
    }
}

struct Validator {
    account: AccountId,
    signer: Signer,
}

impl Validator {
    fn stake_tx(&self, env: &TestLoopEnv, stake: Balance) -> SignedTransaction {
        let block_hash = transactions::get_shared_block_hash(&env.node_datas, &env.test_loop.data);
        SignedTransaction::stake(
            nonce(),
            self.account.clone(),
            &self.signer,
            stake,
            self.signer.public_key(),
            block_hash,
        )
    }
}

fn nonce() -> Nonce {
    static NONCE: AtomicU64 = AtomicU64::new(1);
    NONCE.fetch_add(1, Ordering::Relaxed)
}
