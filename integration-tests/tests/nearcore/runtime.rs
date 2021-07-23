mod tests {
    use std::collections::BTreeSet;
    use std::convert::TryInto;

    use near_primitives::num_rational::Rational;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_logger_utils::init_test_logger;
    use near_primitives::block::Tip;
    use near_primitives::challenge::SlashedValidator;
    use near_primitives::receipt::ReceiptResult;
    use near_primitives::runtime::config::RuntimeConfig;
    use near_primitives::transaction::{Action, DeleteAccountAction, StakeAction};
    use near_primitives::types::{BlockHeightDelta, Nonce, ValidatorId, ValidatorKickoutReason};
    use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
    use near_primitives::views::{
        AccountView, CurrentEpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
    };
    use near_store::create_store;

    use crate::get_store_path;
    use nearcore::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
    use testlib::nearcore_test_utils::GenesisExt;

    use super::*;

    use primitive_types::U256;

    fn stake(
        nonce: Nonce,
        signer: &dyn Signer,
        sender: &dyn ValidatorSigner,
        stake: Balance,
    ) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            sender.validator_id().clone(),
            sender.validator_id().clone(),
            &*signer,
            vec![Action::Stake(StakeAction { stake, public_key: sender.public_key() })],
            // runtime does not validate block history
            CryptoHash::default(),
        )
    }

    impl NightshadeRuntime {
        fn update(
            &self,
            state_root: &StateRoot,
            shard_id: ShardId,
            height: BlockHeight,
            block_timestamp: u64,
            prev_block_hash: &CryptoHash,
            block_hash: &CryptoHash,
            receipts: &[Receipt],
            transactions: &[SignedTransaction],
            last_proposals: ValidatorStakeIter,
            gas_price: Balance,
            gas_limit: Gas,
            challenges: &ChallengesResult,
        ) -> (StateRoot, Vec<ValidatorStake>, ReceiptResult) {
            let mut result = self
                .apply_transactions(
                    shard_id,
                    &state_root,
                    height,
                    block_timestamp,
                    prev_block_hash,
                    block_hash,
                    receipts,
                    transactions,
                    last_proposals,
                    gas_price,
                    gas_limit,
                    challenges,
                    CryptoHash::default(),
                    true,
                    false,
                    None,
                )
                .unwrap();
            let mut store_update = self.store.store_update();
            result.trie_changes.insertions_into(&mut store_update).unwrap();
            result.trie_changes.state_changes_into(&mut store_update);
            store_update.commit().unwrap();
            (result.new_root, result.validator_proposals, result.receipt_result)
        }
    }

    struct TestEnv {
        pub runtime: NightshadeRuntime,
        pub head: Tip,
        state_roots: Vec<StateRoot>,
        pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
        pub last_shard_proposals: HashMap<ShardId, Vec<ValidatorStake>>,
        pub last_proposals: Vec<ValidatorStake>,
        time: u64,
    }

    impl TestEnv {
        pub fn new(
            prefix: &str,
            validators: Vec<Vec<AccountId>>,
            epoch_length: BlockHeightDelta,
            initial_tracked_accounts: Vec<AccountId>,
            initial_tracked_shards: Vec<ShardId>,
            has_reward: bool,
        ) -> Self {
            let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
            let store = create_store(&get_store_path(dir.path()));
            let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
                acc.union(&x.iter().map(|x| x.as_str()).collect()).cloned().collect()
            });
            let validators_len = all_validators.len() as ValidatorId;
            let mut genesis = Genesis::test_sharded(
                all_validators.into_iter().collect(),
                validators_len,
                validators.iter().map(|x| x.len() as ValidatorId).collect(),
            );
            // No fees mode.
            genesis.config.runtime_config = RuntimeConfig::free();
            genesis.config.epoch_length = epoch_length;
            genesis.config.chunk_producer_kickout_threshold =
                genesis.config.block_producer_kickout_threshold;
            if !has_reward {
                genesis.config.max_inflation_rate = Rational::from_integer(0);
            }
            let genesis_total_supply = genesis.config.total_supply;
            let genesis_protocol_version = genesis.config.protocol_version;
            let runtime = NightshadeRuntime::new(
                dir.path(),
                store,
                &genesis,
                initial_tracked_accounts,
                initial_tracked_shards,
                None,
                None,
            );
            let (_store, state_roots) = runtime.genesis_state();
            let genesis_hash = hash(&vec![0]);
            runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash: CryptoHash::default(),
                    hash: genesis_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: 0,
                    last_finalized_height: 0,
                    last_finalized_block_hash: CryptoHash::default(),
                    proposals: vec![],
                    slashed_validators: vec![],
                    chunk_mask: vec![],
                    total_supply: genesis_total_supply,
                    latest_protocol_version: genesis_protocol_version,
                    timestamp_nanosec: 0,
                })
                .unwrap()
                .commit()
                .unwrap();
            Self {
                runtime,
                head: Tip {
                    last_block_hash: genesis_hash,
                    prev_block_hash: CryptoHash::default(),
                    height: 0,
                    epoch_id: EpochId::default(),
                    next_epoch_id: Default::default(),
                },
                state_roots,
                last_receipts: HashMap::default(),
                last_proposals: vec![],
                last_shard_proposals: HashMap::default(),
                time: 0,
            }
        }

        pub fn step(
            &mut self,
            transactions: Vec<Vec<SignedTransaction>>,
            chunk_mask: Vec<bool>,
            challenges_result: ChallengesResult,
        ) {
            let new_hash = hash(&vec![(self.head.height + 1) as u8]);
            let num_shards = self.runtime.num_shards();
            assert_eq!(transactions.len() as NumShards, num_shards);
            assert_eq!(chunk_mask.len() as NumShards, num_shards);
            let mut all_proposals = vec![];
            let mut new_receipts = HashMap::new();
            for i in 0..num_shards {
                let (state_root, proposals, receipts) = self.runtime.update(
                    &self.state_roots[i as usize],
                    i,
                    self.head.height + 1,
                    0,
                    &self.head.last_block_hash,
                    &new_hash,
                    self.last_receipts.get(&i).unwrap_or(&vec![]),
                    &transactions[i as usize],
                    ValidatorStakeIter::new(self.last_shard_proposals.get(&i).unwrap_or(&vec![])),
                    self.runtime.genesis_config.min_gas_price,
                    u64::max_value(),
                    &challenges_result,
                );
                self.state_roots[i as usize] = state_root;
                for (shard_id, mut shard_receipts) in receipts {
                    new_receipts
                        .entry(shard_id)
                        .or_insert_with(|| vec![])
                        .append(&mut shard_receipts);
                }
                all_proposals.append(&mut proposals.clone());
                self.last_shard_proposals.insert(i as ShardId, proposals);
            }
            self.runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash: self.head.last_block_hash,
                    hash: new_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: self.head.height + 1,
                    last_finalized_height: self.head.height.saturating_sub(1),
                    last_finalized_block_hash: self.head.last_block_hash,
                    proposals: self.last_proposals.clone(),
                    slashed_validators: challenges_result,
                    chunk_mask,
                    total_supply: self.runtime.genesis_config.total_supply,
                    latest_protocol_version: self.runtime.genesis_config.protocol_version,
                    timestamp_nanosec: self.time + 10u64.pow(9),
                })
                .unwrap()
                .commit()
                .unwrap();
            self.last_receipts = new_receipts;
            self.last_proposals = all_proposals;
            self.time += 10u64.pow(9);

            self.head = Tip {
                last_block_hash: new_hash,
                prev_block_hash: self.head.last_block_hash,
                height: self.head.height + 1,
                epoch_id: self.runtime.get_epoch_id_from_prev_block(&new_hash).unwrap(),
                next_epoch_id: self.runtime.get_next_epoch_id_from_prev_block(&new_hash).unwrap(),
            };
        }

        /// Step when there is only one shard
        pub fn step_default(&mut self, transactions: Vec<SignedTransaction>) {
            self.step(vec![transactions], vec![true], ChallengesResult::default());
        }

        pub fn view_account(&self, account_id: &str) -> AccountView {
            let shard_id = self.runtime.account_id_to_shard_id(&account_id.to_string());
            self.runtime
                .view_account(
                    shard_id,
                    self.state_roots[shard_id as usize],
                    &account_id.to_string(),
                )
                .unwrap()
                .into()
        }

        /// Compute per epoch per validator reward and per epoch protocol treasury reward
        pub fn compute_reward(
            &self,
            num_validators: usize,
            epoch_duration: u64,
        ) -> (Balance, Balance) {
            let num_seconds_per_year = 60 * 60 * 24 * 365;
            let num_ns_in_second = 1_000_000_000;
            let per_epoch_total_reward =
                (U256::from(*self.runtime.genesis_config.max_inflation_rate.numer() as u64)
                    * U256::from(self.runtime.genesis_config.total_supply)
                    * U256::from(epoch_duration)
                    / (U256::from(num_seconds_per_year)
                        * U256::from(
                            *self.runtime.genesis_config.max_inflation_rate.denom() as u128
                        )
                        * U256::from(num_ns_in_second)))
                .as_u128();
            let per_epoch_protocol_treasury = per_epoch_total_reward
                * *self.runtime.genesis_config.protocol_reward_rate.numer() as u128
                / *self.runtime.genesis_config.protocol_reward_rate.denom() as u128;
            let per_epoch_per_validator_reward =
                (per_epoch_total_reward - per_epoch_protocol_treasury) / num_validators as u128;
            (per_epoch_per_validator_reward, per_epoch_protocol_treasury)
        }
    }

    /// Start with 2 validators with default stake X.
    /// 1. Validator 0 stakes 2 * X
    /// 2. Validator 0 creates new account Validator 2 with 3 * X in balance
    /// 3. Validator 2 stakes 2 * X
    /// 4. Validator 1 gets unstaked because not enough stake.
    /// 5. At the end Validator 0 and 2 with 2 * X are validators. Validator 1 has stake returned to balance.
    #[test]
    fn test_validator_rotation() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_rotation",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.`
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE * 2);
        let new_account = format!("test{}", num_nodes + 1);
        let new_validator =
            InMemoryValidatorSigner::from_seed(&new_account, KeyType::ED25519, &new_account);
        let new_signer = InMemorySigner::from_seed(&new_account, KeyType::ED25519, &new_account);
        let create_account_transaction = SignedTransaction::create_account(
            2,
            block_producers[0].validator_id().clone(),
            new_account,
            TESTING_INIT_STAKE * 3,
            new_signer.public_key(),
            &signer,
            CryptoHash::default(),
        );
        env.step_default(vec![staking_transaction, create_account_transaction]);
        env.step_default(vec![]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, 2 * TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5);

        let stake_transaction =
            stake(env.head.height * 1_000_000, &new_signer, &new_validator, TESTING_INIT_STAKE * 2);
        env.step_default(vec![stake_transaction]);
        env.step_default(vec![]);

        // Roll steps for 3 epochs to pass.
        for _ in 5..=9 {
            env.step_default(vec![]);
        }

        let epoch_id = env.runtime.get_epoch_id_from_prev_block(&env.head.last_block_hash).unwrap();
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<HashMap<_, _>>(),
            vec![("test3".to_string(), false), ("test1".to_string(), false)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );

        let test1_acc = env.view_account("test1");
        // Staked 2 * X, sent 3 * X to test3.
        assert_eq!(
            (test1_acc.amount, test1_acc.locked),
            (TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
        );
        let test2_acc = env.view_account("test2");
        // Become fishermen instead
        assert_eq!(
            (test2_acc.amount, test2_acc.locked),
            (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
        );
        let test3_acc = env.view_account("test3");
        // Got 3 * X, staking 2 * X of them.
        assert_eq!(
            (test3_acc.amount, test3_acc.locked),
            (TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
        );
    }

    /// One validator tries to decrease their stake in epoch T. Make sure that the stake return happens in epoch T+3.
    #[test]
    fn test_validator_stake_change() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);

        let desired_stake = 2 * TESTING_INIT_STAKE / 3;
        let staking_transaction = stake(1, &signer, &block_producers[0], desired_stake);
        env.step_default(vec![staking_transaction]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=4 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 5..=7 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - desired_stake);
        assert_eq!(account.locked, desired_stake);
    }

    #[test]
    fn test_validator_stake_change_multiple_times() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction =
            stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 1);
        let staking_transaction1 =
            stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 2);
        let staking_transaction2 =
            stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let staking_transaction =
            stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE + 1);
        let staking_transaction1 =
            stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 2);
        let staking_transaction2 =
            stake(3, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 1);
        let staking_transaction3 =
            stake(1, &signers[3], &block_producers[3], TESTING_INIT_STAKE - 1);
        env.step_default(vec![
            staking_transaction,
            staking_transaction1,
            staking_transaction2,
            staking_transaction3,
        ]);

        for _ in 3..=8 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 9..=12 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 13..=16 {
            env.step_default(vec![]);
        }

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);

        let account = env.view_account(&block_producers[2].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
    }

    #[test]
    fn test_stake_in_last_block_of_an_epoch() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_stake_change_multiple_times",
            vec![validators.clone()],
            5,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let staking_transaction =
            stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
        env.step_default(vec![staking_transaction]);
        for _ in 2..10 {
            env.step_default(vec![]);
        }
        let staking_transaction =
            stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let staking_transaction = stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
        env.step_default(vec![staking_transaction]);
        for _ in 13..=16 {
            env.step_default(vec![]);
        }
        let account = env.view_account(&block_producers[0].validator_id());
        let return_stake = (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2)
            - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
        assert_eq!(
            account.amount,
            TESTING_INIT_BALANCE - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2) + return_stake
        );
        assert_eq!(account.locked, TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2 - return_stake);
    }

    #[test]
    fn test_verify_validator_signature() {
        let validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let env = TestEnv::new(
            "verify_validator_signature_failure",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            true,
        );
        let data = [0; 32];
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let signature = signer.sign(&data);
        assert!(env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &validators[0],
                &data,
                &signature
            )
            .unwrap());
    }

    #[test]
    fn test_state_sync() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![], false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE + 1);
        env.step_default(vec![staking_transaction]);
        env.step_default(vec![]);
        let state_part = env.runtime.obtain_state_part(0, &env.state_roots[0], 0, 1).unwrap();
        let root_node = env.runtime.get_state_root_node(0, &env.state_roots[0]).unwrap();
        let mut new_env =
            TestEnv::new("test_state_sync", vec![validators.clone()], 2, vec![], vec![], false);
        for i in 1..=2 {
            let prev_hash = hash(&[new_env.head.height as u8]);
            let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
            let proposals = if i == 1 {
                vec![ValidatorStake::new(
                    block_producers[0].validator_id().clone(),
                    block_producers[0].public_key(),
                    TESTING_INIT_STAKE + 1,
                )]
            } else {
                vec![]
            };
            new_env
                .runtime
                .add_validator_proposals(BlockHeaderInfo {
                    prev_hash,
                    hash: cur_hash,
                    random_value: [0; 32].as_ref().try_into().unwrap(),
                    height: i,
                    last_finalized_height: i.saturating_sub(2),
                    last_finalized_block_hash: prev_hash,
                    proposals: new_env.last_proposals,
                    slashed_validators: vec![],
                    chunk_mask: vec![true],
                    total_supply: new_env.runtime.genesis_config.total_supply,
                    latest_protocol_version: new_env.runtime.genesis_config.protocol_version,
                    timestamp_nanosec: new_env.time,
                })
                .unwrap()
                .commit()
                .unwrap();
            new_env.head.height = i;
            new_env.head.last_block_hash = cur_hash;
            new_env.head.prev_block_hash = prev_hash;
            new_env.last_proposals = proposals;
            new_env.time += 10u64.pow(9);
        }
        assert!(new_env.runtime.validate_state_root_node(&root_node, &env.state_roots[0]));
        let mut root_node_wrong = root_node.clone();
        root_node_wrong.memory_usage += 1;
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        root_node_wrong.data = vec![123];
        assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
        assert!(!new_env.runtime.validate_state_part(&StateRoot::default(), 0, 1, &state_part));
        new_env.runtime.validate_state_part(&env.state_roots[0], 0, 1, &state_part);
        let epoch_id = &new_env.head.epoch_id;
        new_env
            .runtime
            .apply_state_part(0, &env.state_roots[0], 0, 1, &state_part, epoch_id)
            .unwrap();
        new_env.state_roots[0] = env.state_roots[0].clone();
        for _ in 3..=5 {
            new_env.step_default(vec![]);
        }

        let account = new_env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
        assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

        let account = new_env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
    }

    /// Test two shards: the first shard has 2 validators (test1, test4) and the second shard
    /// has 4 validators (test1, test2, test3, test4). Test that kickout and stake change
    /// work properly.
    #[test]
    fn test_multiple_shards() {
        init_test_logger();
        let num_nodes = 4;
        let first_shard_validators = (0..2).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let second_shard_validators =
            (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let validators = second_shard_validators.clone();
        let mut env = TestEnv::new(
            "test_multiple_shards",
            vec![first_shard_validators, second_shard_validators],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE - 1);
        let first_account_shard_id = env.runtime.account_id_to_shard_id(&"test1".to_string());
        let transactions = if first_account_shard_id == 0 {
            vec![vec![staking_transaction], vec![]]
        } else {
            vec![vec![], vec![staking_transaction]]
        };
        env.step(transactions, vec![false, true], ChallengesResult::default());
        for _ in 2..10 {
            env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        }
        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE);

        for _ in 10..14 {
            env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        }
        let account = env.view_account(&block_producers[3].validator_id());
        assert_eq!(account.locked, 0);

        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
    }

    #[test]
    fn test_get_validator_info() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_get_validator_info",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[0], KeyType::ED25519, &validators[0]);
        let staking_transaction = stake(1, &signer, &block_producers[0], 0);
        env.step_default(vec![staking_transaction]);
        assert!(env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::EpochId(env.head.epoch_id.clone()))
            .is_err());
        env.step_default(vec![]);
        let mut current_epoch_validator_info = vec![
            CurrentEpochValidatorInfo {
                account_id: "test1".to_string(),
                public_key: block_producers[0].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: 1,
                num_expected_blocks: 1,
            },
            CurrentEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                is_slashed: false,
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
                num_produced_blocks: 1,
                num_expected_blocks: 1,
            },
        ];
        let next_epoch_validator_info = vec![
            NextEpochValidatorInfo {
                account_id: "test1".to_string(),
                public_key: block_producers[0].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
            NextEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            },
        ];
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response,
            EpochValidatorInfo {
                current_validators: current_epoch_validator_info.clone(),
                next_validators: next_epoch_validator_info.clone(),
                current_fishermen: vec![],
                next_fishermen: vec![],
                current_proposals: vec![ValidatorStake::new(
                    "test1".to_string(),
                    block_producers[0].public_key(),
                    0,
                )
                .into()],
                prev_epoch_kickout: Default::default(),
                epoch_start_height: 1,
                epoch_height: 1,
            }
        );
        env.step_default(vec![]);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();

        current_epoch_validator_info[1].num_produced_blocks = 0;
        current_epoch_validator_info[1].num_expected_blocks = 0;
        assert_eq!(response.current_validators, current_epoch_validator_info);
        assert_eq!(
            response.next_validators,
            vec![NextEpochValidatorInfo {
                account_id: "test2".to_string(),
                public_key: block_producers[1].public_key(),
                stake: TESTING_INIT_STAKE,
                shards: vec![0],
            }
            .into()]
        );
        assert!(response.current_proposals.is_empty());
        assert_eq!(
            response.prev_epoch_kickout,
            vec![ValidatorKickoutView {
                account_id: "test1".to_string(),
                reason: ValidatorKickoutReason::Unstaked
            }]
        );
        assert_eq!(response.epoch_start_height, 3);
    }

    #[test]
    fn test_care_about_shard() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_get_validator_info",
            vec![validators.clone(), vec![validators[0].clone()]],
            2,
            vec![validators[1].clone()],
            vec![],
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signer = InMemorySigner::from_seed(&validators[1], KeyType::ED25519, &validators[1]);
        let staking_transaction = stake(1, &signer, &block_producers[1], 0);
        env.step(
            vec![vec![staking_transaction], vec![]],
            vec![true, true],
            ChallengesResult::default(),
        );
        env.step(vec![vec![], vec![]], vec![true, true], ChallengesResult::default());
        assert!(env.runtime.cares_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(!env.runtime.cares_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            1,
            true
        ));
        assert!(env.runtime.cares_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(env.runtime.cares_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            1,
            true
        ));

        assert!(env.runtime.will_care_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(env.runtime.will_care_about_shard(
            Some(&validators[0]),
            &env.head.last_block_hash,
            1,
            true
        ));
        assert!(env.runtime.will_care_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            0,
            true
        ));
        assert!(!env.runtime.will_care_about_shard(
            Some(&validators[1]),
            &env.head.last_block_hash,
            1,
            true
        ));
    }

    #[test]
    fn test_challenges() {
        let mut env = TestEnv::new(
            "test_challenges",
            vec![vec!["test1".to_string(), "test2".to_string()]],
            2,
            vec![],
            vec![],
            true,
        );
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test2".to_string(), false)]);
        assert_eq!(env.view_account("test2").locked, 0);
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test2".to_string(), true), ("test1".to_string(), false)]
        );
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2", KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".to_string(),
                &msg,
                &signature,
            )
            .unwrap());
        // Run for 3 epochs, to finalize the given block and make sure that slashed stake actually correctly propagates.
        for _ in 0..6 {
            env.step(vec![vec![]], vec![true], vec![]);
        }
    }

    /// Test that in case of a double sign, not all stake is slashed if the double signed stake is
    /// less than 33% and all stake is slashed if the stake is more than 33%
    #[test]
    fn test_double_sign_challenge_not_all_slashed() {
        init_test_logger();
        let num_nodes = 3;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 3, vec![], vec![], false);
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let signer = InMemorySigner::from_seed(&validators[2], KeyType::ED25519, &validators[2]);
        let staking_transaction = stake(1, &signer, &block_producers[2], TESTING_INIT_STAKE / 3);
        env.step(
            vec![vec![staking_transaction]],
            vec![true],
            vec![SlashedValidator::new("test2".to_string(), true)],
        );
        assert_eq!(env.view_account("test2").locked, TESTING_INIT_STAKE);
        assert_eq!(
            env.runtime
                .get_epoch_block_producers_ordered(&env.head.epoch_id, &env.head.last_block_hash)
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![
                ("test3".to_string(), false),
                ("test2".to_string(), true),
                ("test1".to_string(), false)
            ]
        );
        let msg = vec![0, 1, 2];
        let signer = InMemorySigner::from_seed("test2", KeyType::ED25519, "test2");
        let signature = signer.sign(&msg);
        assert!(!env
            .runtime
            .verify_validator_signature(
                &env.head.epoch_id,
                &env.head.last_block_hash,
                &"test2".to_string(),
                &msg,
                &signature,
            )
            .unwrap());

        for _ in 2..11 {
            env.step(vec![vec![]], vec![true], vec![]);
        }
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test3".to_string(), true)]);
        let account = env.view_account("test3");
        assert_eq!(account.locked, TESTING_INIT_STAKE / 3);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 11..14 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test3");
        let slashed = (TESTING_INIT_STAKE / 3) * 3 / 4;
        let remaining = TESTING_INIT_STAKE / 3 - slashed;
        assert_eq!(account.locked, remaining);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3);

        for _ in 14..=20 {
            env.step_default(vec![]);
        }

        let account = env.view_account("test2");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test3");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE / 3 + remaining);
    }

    /// Test that double sign from multiple accounts may result in all of their stake slashed.
    #[test]
    fn test_double_sign_challenge_all_slashed() {
        init_test_logger();
        let num_nodes = 5;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 5, vec![], vec![], false);
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test1".to_string(), true)]);
        env.step(vec![vec![]], vec![true], vec![SlashedValidator::new("test2".to_string(), true)]);
        let msg = vec![0, 1, 2];
        for i in 0..=1 {
            let signature = signers[i].sign(&msg);
            assert!(!env
                .runtime
                .verify_validator_signature(
                    &env.head.epoch_id,
                    &env.head.last_block_hash,
                    &format!("test{}", i + 1),
                    &msg,
                    &signature,
                )
                .unwrap());
        }

        for _ in 3..17 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test1");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test2");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    }

    /// Test that if double sign occurs in the same epoch as other type of challenges all stake
    /// is slashed.
    #[test]
    fn test_double_sign_with_other_challenges() {
        init_test_logger();
        let num_nodes = 3;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env =
            TestEnv::new("test_challenges", vec![validators.clone()], 5, vec![], vec![], false);
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".to_string(), true),
                SlashedValidator::new("test2".to_string(), false),
            ],
        );
        env.step(
            vec![vec![]],
            vec![true],
            vec![
                SlashedValidator::new("test1".to_string(), false),
                SlashedValidator::new("test2".to_string(), true),
            ],
        );

        for _ in 3..11 {
            env.step_default(vec![]);
        }
        let account = env.view_account("test1");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account = env.view_account("test2");
        assert_eq!(account.locked, 0);
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    }

    /// Run 4 validators. Two of them first change their stake to below validator threshold but above
    /// fishermen threshold. Make sure their balance is correct. Then one fisherman increases their
    /// stake to become a validator again while the other one decreases to below fishermen threshold.
    /// Check that the first one becomes a validator and the second one gets unstaked completely.
    #[test]
    fn test_fishermen_stake() {
        init_test_logger();
        let num_nodes = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_fishermen_stake",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let fishermen_stake = TESTING_INIT_STAKE / 10 + 1;

        let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], fishermen_stake);
        env.step_default(vec![staking_transaction, staking_transaction1]);
        let account = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=13 {
            env.step_default(vec![]);
        }
        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, fishermen_stake);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - fishermen_stake);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response
                .current_fishermen
                .into_iter()
                .map(|fishermen| fishermen.take_account_id())
                .collect::<Vec<_>>(),
            vec!["test1", "test2"]
        );
        let staking_transaction = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
        let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction, staking_transaction2]);

        for _ in 13..=25 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, TESTING_INIT_STAKE);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

        let account1 = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account1.locked, 0);
        assert_eq!(account1.amount, TESTING_INIT_BALANCE);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert!(response.current_fishermen.is_empty());
    }

    /// Test that when fishermen unstake they get their tokens back.
    #[test]
    fn test_fishermen_unstake() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_fishermen_unstake",
            vec![validators.clone()],
            2,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let fishermen_stake = TESTING_INIT_STAKE / 10 + 1;

        let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
        env.step_default(vec![staking_transaction]);
        for _ in 2..9 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, fishermen_stake);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE - fishermen_stake);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert_eq!(
            response
                .current_fishermen
                .into_iter()
                .map(|fishermen| fishermen.take_account_id())
                .collect::<Vec<_>>(),
            vec!["test1"]
        );
        let staking_transaction = stake(2, &signers[0], &block_producers[0], 0);
        env.step_default(vec![staking_transaction]);
        for _ in 10..17 {
            env.step_default(vec![]);
        }

        let account0 = env.view_account(&block_producers[0].validator_id());
        assert_eq!(account0.locked, 0);
        assert_eq!(account0.amount, TESTING_INIT_BALANCE);
        let response = env
            .runtime
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
            .unwrap();
        assert!(response.current_fishermen.is_empty());
    }

    /// Enable reward and make sure that validators get reward proportional to their stake.
    #[test]
    fn test_validator_reward() {
        init_test_logger();
        let num_nodes = 4;
        let epoch_length = 4;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_reward",
            vec![validators.clone()],
            epoch_length,
            vec![],
            vec![],
            true,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        for _ in 0..5 {
            env.step_default(vec![]);
        }

        let (validator_reward, protocol_treasury_reward) =
            env.compute_reward(num_nodes, epoch_length * 10u64.pow(9));
        for i in 0..4 {
            let account = env.view_account(&block_producers[i].validator_id());
            assert_eq!(account.locked, TESTING_INIT_STAKE + validator_reward);
        }

        let protocol_treasury_account =
            env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
        assert_eq!(
            protocol_treasury_account.amount,
            TESTING_INIT_BALANCE + protocol_treasury_reward
        );
    }

    #[test]
    fn test_delete_account_after_unstake() {
        init_test_logger();
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_validator_delete_account",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction1]);
        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
        assert_eq!(account.locked, TESTING_INIT_STAKE);
        for _ in 2..=5 {
            env.step_default(vec![]);
        }
        let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 1);
        env.step_default(vec![staking_transaction2]);
        for _ in 7..=13 {
            env.step_default(vec![]);
        }
        let account = env.view_account(&block_producers[1].validator_id());
        assert_eq!(account.locked, 0);

        let delete_account_transaction = SignedTransaction::from_actions(
            4,
            signers[1].account_id.clone(),
            signers[1].account_id.clone(),
            &signers[1] as &dyn Signer,
            vec![Action::DeleteAccount(DeleteAccountAction {
                beneficiary_id: signers[0].account_id.clone(),
            })],
            // runtime does not validate block history
            CryptoHash::default(),
        );
        env.step_default(vec![delete_account_transaction]);
        for _ in 15..=17 {
            env.step_default(vec![]);
        }
    }

    #[test]
    fn test_proposal_deduped() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_proposal_deduped",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction1 =
            stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 100);
        let staking_transaction2 =
            stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 10);
        env.step_default(vec![staking_transaction1, staking_transaction2]);
        assert_eq!(env.last_proposals.len(), 1);
        assert_eq!(env.last_proposals[0].stake(), TESTING_INIT_STAKE - 10);
    }

    #[test]
    fn test_insufficient_stake() {
        let num_nodes = 2;
        let validators = (0..num_nodes).map(|i| format!("test{}", i + 1)).collect::<Vec<_>>();
        let mut env = TestEnv::new(
            "test_proposal_deduped",
            vec![validators.clone()],
            4,
            vec![],
            vec![],
            false,
        );
        let block_producers: Vec<_> = validators
            .iter()
            .map(|id| InMemoryValidatorSigner::from_seed(id, KeyType::ED25519, id))
            .collect();
        let signers: Vec<_> = validators
            .iter()
            .map(|id| InMemorySigner::from_seed(id, KeyType::ED25519, id))
            .collect();

        let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 100);
        let staking_transaction2 =
            stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE / 10 - 1);
        env.step_default(vec![staking_transaction1, staking_transaction2]);
        assert!(env.last_proposals.is_empty());
        let staking_transaction3 = stake(3, &signers[1], &block_producers[1], 0);
        env.step_default(vec![staking_transaction3]);
        assert_eq!(env.last_proposals.len(), 1);
        assert_eq!(env.last_proposals[0].stake(), 0);
    }
}
