use super::*;
use crate::types::{
    BlockType, ChainConfig, MaybePinnedMemtrieRoot, RuntimeStorageConfig,
    StatePartValidationResult, StateRootNodeValidationResult,
};
use crate::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode};
use assert_matches::assert_matches;
use borsh::BorshDeserialize;
use itertools::Itertools;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::time::Clock;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_chain_configs::{
    DEFAULT_GC_NUM_EPOCHS_TO_KEEP, DEFAULT_STATE_PARTS_COMPRESSION_LEVEL, Genesis,
    MutableConfigValue, default_produce_chunk_add_transactions_time_limit,
    default_view_access_keys_limit,
};
use near_crypto::{InMemorySigner, KeyType, PublicKeyHandle, Signature, Signer};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_o11y::testonly::init_test_logger;
use near_parameters::config::ExternalCallsConfig;
use near_pool::types::PooledGroup;
use near_pool::{InsertTransactionResult, PoolIteratorWrapper, TransactionPool};
use near_primitives::account::{AccessKeyPermission, AccountContract};
use near_primitives::action::UniversalStateInitAction;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::block::Tip;
use near_primitives::congestion_info::{
    BlockCongestionInfo, CongestionInfo, CongestionInfoV1, ExtendedCongestionInfo,
};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::RngSeed;
use near_primitives::errors::InvalidTxError;
use near_primitives::num_rational::Rational32;
use near_primitives::receipt::{ActionReceipt, ReceiptV0};
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{
    Action, DeleteAccountAction, DeployContractAction, ECC_METHOD_NAME, EccTransaction,
    ExecutionStatus, SignedTransaction, StakeAction, Transaction, TransactionEnvelope,
    TransactionNonce, TransactionV0, TransferAction, ValidatedEcc, ValidatedTransactionEnvelope,
};
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::Gas;
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{
    BlockHeightDelta, Nonce, StateChangeCause, ValidatorId, ValidatorInfoIdentifier,
    ValidatorKickoutReason,
};
use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use near_primitives::utils::derive_universal_account_id;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    AccountView, CurrentEpochValidatorInfo, EpochValidatorInfo, NextEpochValidatorInfo,
    ValidatorKickoutView,
};
use near_runtime::PostStateNodes;
use near_runtime::SignedValidPeriodTransactions;
use near_runtime::config::ecc_cost;
use near_store::flat::{FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata};
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::test_populate_trie;
use near_store::trie::AccessOptions;
use near_store::trie::NewTrieNodes;
use near_store::{NodeStorage, PartialStorage, get_account, get_genesis_state_roots, set_account};
use near_vm_runner::FilesystemContractRuntimeCache;
use num_rational::Ratio;
use primitive_types::U256;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::mpsc;
use std::thread;
use tempfile::TempDir;

const TEST_SEED: RngSeed = [3; 32];
const NUM_TEST_SIGNERS: usize = 4;

struct TestEnvConfig {
    epoch_length: BlockHeightDelta,
    has_reward: bool,
    minimum_stake_divisor: Option<u64>,
    zero_fees: bool,
    create_flat_storage: bool,
    runtime_config_store: Option<RuntimeConfigStore>,
}

/// Environment to test runtime behavior separate from Chain.
/// Runtime operates in a mock chain where i-th block is attached to (i-1)-th one, has height `i` and hash
/// `hash([i])`.
struct TestEnv {
    pub epoch_manager: Arc<EpochManagerHandle>,
    pub runtime: Arc<NightshadeRuntime>,
    pub head: Tip,
    state_roots: Vec<StateRoot>,
    pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
    pub last_shard_proposals: HashMap<ShardId, Vec<ValidatorStake>>,
    pub last_proposals: Vec<ValidatorStake>,
    time: u64,
    // Keep the store and compiled contract cache directories alive for the test.
    _dir: TempDir,
}

impl TestEnv {
    pub fn new(
        validators: Vec<Vec<AccountId>>,
        epoch_length: BlockHeightDelta,
        has_reward: bool,
    ) -> Self {
        Self::new_with_config(
            validators,
            TestEnvConfig {
                epoch_length,
                has_reward,
                minimum_stake_divisor: None,
                zero_fees: true,
                create_flat_storage: true,
                runtime_config_store: None,
            },
        )
    }

    fn new_with_config(validators: Vec<Vec<AccountId>>, config: TestEnvConfig) -> Self {
        let (dir, opener) = NodeStorage::test_opener();
        let store = opener.open().unwrap().get_hot_store();
        let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
            acc.union(&x.iter().cloned().collect()).cloned().collect()
        });
        let validators_len = all_validators.len() as ValidatorId;
        let mut genesis = Genesis::test_sharded_new_version(
            all_validators.into_iter().collect(),
            validators_len,
            validators.len() as u64,
        );
        // No fees mode.
        genesis.config.epoch_length = config.epoch_length;
        genesis.config.transaction_validity_period = config.epoch_length * 2;
        genesis.config.chunk_producer_kickout_threshold =
            genesis.config.block_producer_kickout_threshold;
        genesis.config.chunk_validator_only_kickout_threshold =
            genesis.config.block_producer_kickout_threshold;
        if !config.has_reward {
            genesis.config.max_inflation_rate = Ratio::from_integer(0);
        }
        if let Some(minimum_stake_divisor) = config.minimum_stake_divisor {
            genesis.config.minimum_stake_divisor = minimum_stake_divisor;
        }
        let genesis_total_supply = genesis.config.total_supply;
        let genesis_protocol_version = genesis.config.protocol_version;

        let runtime_config_store = config.runtime_config_store.unwrap_or_else(|| {
            if config.zero_fees { RuntimeConfigStore::free() } else { RuntimeConfigStore::test() }
        });

        let compiled_contract_cache = FilesystemContractRuntimeCache::new(
            &dir.as_ref(),
            None::<&str>,
            "contract.cache",
            FilesystemContractRuntimeCache::MAX_DISK_CACHE_BYTES,
        )
        .unwrap();

        initialize_genesis_state(store.clone(), &genesis, Some(dir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let runtime = NightshadeRuntime::new(
            store.clone(),
            compiled_contract_cache.handle(),
            &genesis.config,
            epoch_manager.clone(),
            None,
            default_view_access_keys_limit(),
            None,
            Some(runtime_config_store),
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            Default::default(),
            StateSnapshotConfig::enabled(dir.path().join("data")),
            DEFAULT_STATE_PARTS_COMPRESSION_LEVEL,
            RuntimeOptions::default(),
        );
        let state_roots = get_genesis_state_roots(&store).unwrap();
        let genesis_hash = hash(&[0]);

        let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        if config.create_flat_storage {
            // Create flat storage. Naturally it happens on Chain creation, but here we test only Runtime behavior
            // and use a mock chain, so we need to initialize flat storage manually.
            let flat_storage_manager = runtime.get_flat_storage_manager();
            for shard_uid in shard_layout.shard_uids() {
                let mut store_update = store.store_update();
                flat_storage_manager.set_flat_storage_for_genesis(
                    &mut store_update.flat_store_update(),
                    shard_uid,
                    &genesis_hash,
                    0,
                );
                store_update.commit();
                assert!(matches!(
                    flat_storage_manager.get_flat_storage_status(shard_uid),
                    near_store::flat::FlatStorageStatus::Ready(_)
                ));
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
        }

        epoch_manager
            .record_block_info(
                BlockInfo::new(
                    genesis_hash,
                    0,
                    0,
                    CryptoHash::default(),
                    CryptoHash::default(),
                    vec![],
                    vec![],
                    genesis_total_supply,
                    genesis_protocol_version,
                    0,
                    ChunkEndorsementsBitmap::from_endorsements(vec![
                        vec![
                            true;
                            validators_len as usize
                        ];
                        shard_layout.num_shards()
                            as usize
                    ]),
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .store_update
            .commit();
        Self {
            epoch_manager,
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
            _dir: dir,
        }
    }

    pub fn apply_new_chunk<T: Into<TransactionEnvelope>>(
        &self,
        shard_id: ShardId,
        transactions: Vec<T>,
        receipts: &[Receipt],
    ) -> ApplyChunkResult {
        // TODO(congestion_control): pass down prev block info and read congestion info from there
        // For now, just use default.
        // TODO(bandwidth_scheduler) - pass bandwidth requests from prev_block
        let prev_block_hash = self.head.last_block_hash;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).unwrap();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let state_root = self.state_roots[shard_index];
        let gas_limit = Gas::MAX;
        let height = self.head.height + 1;
        let block_timestamp = 0;
        let gas_price = self.runtime.genesis_config.min_gas_price;
        let shard_ids = self.epoch_manager.shard_ids(&epoch_id).unwrap();
        let shards_congestion_info = shard_ids
            .into_iter()
            .map(|shard_id| (shard_id, ExtendedCongestionInfo::default()))
            .collect();
        let congestion_info = BlockCongestionInfo::new(shards_congestion_info);
        let transaction_validity = vec![true; transactions.len()];
        let transactions = SignedValidPeriodTransactions::new(transactions, transaction_validity);
        self.runtime
            .apply_chunk(
                RuntimeStorageConfig::new(state_root, true),
                ApplyChunkReason::UpdateTrackedShard,
                ApplyChunkShardContext {
                    shard_uid,
                    last_validator_proposals: ValidatorStakeIter::new(
                        self.last_shard_proposals.get(&shard_id).unwrap_or(&vec![]),
                    ),
                    gas_limit,
                    is_new_chunk: true,
                    on_post_state_ready: None,
                    memtrie_pin: MaybePinnedMemtrieRoot::no_memtries(),
                },
                ApplyChunkBlockContext {
                    block_type: BlockType::Normal,
                    height,
                    prev_block_hash,
                    prev_block_height: self.head.height,
                    prev_block_timestamp: 0,
                    block_timestamp,
                    gas_price,
                    random_seed: CryptoHash::default(),
                    congestion_info,
                    bandwidth_requests: BlockBandwidthRequests::empty(),
                },
                receipts,
                transactions,
            )
            .unwrap()
    }

    fn update_runtime(
        &self,
        shard_id: ShardId,
        new_block_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        receipts: &[Receipt],
    ) -> (CryptoHash, Vec<ValidatorStake>, Vec<Receipt>) {
        let mut apply_result = self.apply_new_chunk(shard_id, transactions, receipts);
        let mut store_update = self.runtime.store().store_update();
        let flat_state_changes =
            FlatStateChanges::from_state_changes(&apply_result.trie_changes.state_changes());
        apply_result.trie_changes.insertions_into(&mut store_update.trie_store_update());
        apply_result
            .trie_changes
            .state_changes_into(&new_block_hash, &mut store_update.trie_store_update());

        let prev_block_hash = self.head.last_block_hash;
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).unwrap_or_default();
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id).unwrap();
        if let Some(flat_storage) =
            self.runtime.get_flat_storage_manager().get_flat_storage_for_shard(shard_uid)
        {
            let delta = FlatStateDelta {
                changes: flat_state_changes,
                metadata: FlatStateDeltaMetadata {
                    block: near_store::flat::BlockInfo {
                        hash: new_block_hash,
                        height: self.head.height + 1,
                        prev_hash: prev_block_hash,
                    },
                    prev_block_with_changes: None,
                },
            };
            let new_store_update = flat_storage.add_delta(delta).unwrap();
            store_update.merge(new_store_update.into());
        }
        store_update.commit();

        (apply_result.new_root, apply_result.validator_proposals, apply_result.outgoing_receipts)
    }

    pub fn step(&mut self, transactions: Vec<Vec<SignedTransaction>>, chunk_mask: Vec<bool>) {
        let new_height = self.head.height + 1;
        let new_hash = hash(&[new_height as u8]);
        let shard_ids = self.epoch_manager.shard_ids(&self.head.epoch_id).unwrap();
        let shard_layout = self.epoch_manager.get_shard_layout(&self.head.epoch_id).unwrap();
        let chunk_endorsements = ChunkEndorsementsBitmap::from_endorsements(
            shard_ids
                .iter()
                .map(|shard_id| {
                    let assignments = self
                        .epoch_manager
                        .get_chunk_validator_assignments(&self.head.epoch_id, *shard_id, new_height)
                        .unwrap();
                    vec![true; assignments.assignments().iter().len()]
                })
                .collect(),
        );
        assert_eq!(transactions.len(), shard_ids.len());
        assert_eq!(chunk_mask.len(), shard_ids.len());
        let mut all_proposals = vec![];
        let mut all_receipts = vec![];
        for shard_id in shard_ids {
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let (state_root, proposals, receipts) = self.update_runtime(
                shard_id,
                new_hash,
                transactions[shard_index].clone(),
                self.last_receipts.get(&shard_id).map_or(&[], |v| v.as_slice()),
            );
            self.state_roots[shard_index] = state_root;
            all_receipts.extend(receipts);
            all_proposals.append(&mut proposals.clone());
            self.last_shard_proposals.insert(shard_id, proposals);
        }
        self.epoch_manager
            .record_block_info(
                BlockInfo::new(
                    new_hash,
                    new_height,
                    self.head.height.saturating_sub(1),
                    self.head.last_block_hash,
                    self.head.last_block_hash,
                    self.last_proposals.clone(),
                    chunk_mask,
                    self.runtime.genesis_config.total_supply,
                    self.runtime.genesis_config.protocol_version,
                    self.time + 10u64.pow(9),
                    chunk_endorsements,
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .store_update
            .commit();
        let shard_layout = self.epoch_manager.get_shard_layout_from_prev_block(&new_hash).unwrap();
        let mut new_receipts = HashMap::<_, Vec<Receipt>>::new();
        for receipt in all_receipts {
            let shard_id = receipt.receiver_shard_id(&shard_layout).unwrap();
            new_receipts.entry(shard_id).or_default().push(receipt);
        }
        self.last_receipts = new_receipts;
        self.last_proposals = all_proposals;
        self.time += 10u64.pow(9);

        self.head = Tip {
            last_block_hash: new_hash,
            prev_block_hash: self.head.last_block_hash,
            height: self.head.height + 1,
            epoch_id: self
                .epoch_manager
                .get_epoch_id_from_prev_block(&self.head.last_block_hash)
                .unwrap(),
            next_epoch_id: self
                .epoch_manager
                .get_next_epoch_id_from_prev_block(&self.head.last_block_hash)
                .unwrap(),
        };
    }

    /// Step when there is only one shard
    pub fn step_default(&mut self, transactions: Vec<SignedTransaction>) {
        self.step(vec![transactions], vec![true]);
    }

    pub fn view_account(&self, account_id: &AccountId) -> AccountView {
        let shard_layout = self.epoch_manager.get_shard_layout(&self.head.epoch_id).unwrap();
        let shard_id = shard_layout.account_id_to_shard_id(account_id);
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let shard_uid =
            shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &self.head.epoch_id).unwrap();
        self.runtime
            .view_account(&shard_uid, self.state_roots[shard_index], account_id)
            .unwrap()
            .into()
    }

    /// Compute per epoch per validator reward and per epoch protocol treasury reward
    pub fn compute_reward(&self, num_validators: usize, epoch_duration: u64) -> (Balance, Balance) {
        let num_seconds_per_year = 60 * 60 * 24 * 365;
        let num_ns_in_second = 1_000_000_000;
        let per_epoch_total_reward = Balance::from_yoctonear(
            (U256::from(*self.runtime.genesis_config.max_inflation_rate.numer() as u64)
                * U256::from(self.runtime.genesis_config.total_supply.as_yoctonear())
                * U256::from(epoch_duration)
                / (U256::from(num_seconds_per_year)
                    * U256::from(*self.runtime.genesis_config.max_inflation_rate.denom() as u128)
                    * U256::from(num_ns_in_second)))
            .as_u128(),
        );
        let per_epoch_protocol_treasury = per_epoch_total_reward
            .checked_mul(
                (*self.runtime.genesis_config.protocol_reward_rate.numer()).try_into().unwrap(),
            )
            .unwrap()
            .checked_div(
                (*self.runtime.genesis_config.protocol_reward_rate.denom()).try_into().unwrap(),
            )
            .unwrap();
        let per_epoch_per_validator_reward = per_epoch_total_reward
            .checked_sub(per_epoch_protocol_treasury)
            .unwrap()
            .checked_div(num_validators.try_into().unwrap())
            .unwrap();
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
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.`
    let staking_transaction =
        stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE.checked_mul(2).unwrap());
    let new_account = AccountId::try_from(format!("test{}", num_nodes + 1)).unwrap();
    let new_validator = create_test_signer(new_account.as_str());
    let new_signer: Signer = InMemorySigner::test_signer(&new_account);
    let create_account_transaction = SignedTransaction::create_account(
        2,
        block_producers[0].validator_id().clone(),
        new_account,
        TESTING_INIT_STAKE.checked_mul(3).unwrap(),
        new_signer.public_key(),
        &signer,
        CryptoHash::default(),
    );
    let test2_stake_amount = Balance::from_near(3600);
    let transactions = {
        // With the new validator selection algorithm, test2 needs to have less stake to
        // become a fisherman.
        let signer = InMemorySigner::test_signer(&validators[1]);
        vec![
            staking_transaction,
            create_account_transaction,
            stake(1, &signer, &block_producers[1], test2_stake_amount),
        ]
    };
    env.step_default(transactions);
    env.step_default(vec![]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_mul(2).unwrap());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE.checked_mul(5).unwrap()).unwrap()
    );

    let stake_transaction = stake(
        env.head.height * 1_000_000,
        &new_signer,
        &new_validator,
        TESTING_INIT_STAKE.checked_mul(2).unwrap(),
    );
    env.step_default(vec![stake_transaction]);
    env.step_default(vec![]);

    // Roll steps for 3 epochs to pass.
    for _ in 5..=9 {
        env.step_default(vec![]);
    }

    let epoch_id =
        env.epoch_manager.get_epoch_id_from_prev_block(&env.head.last_block_hash).unwrap();
    assert_eq!(
        env.epoch_manager
            .get_epoch_block_producers_ordered(&epoch_id)
            .unwrap()
            .iter()
            .map(|x| x.account_id().clone())
            .collect::<HashSet<_>>(),
        vec!["test3".parse().unwrap(), "test1".parse().unwrap()]
            .into_iter()
            .collect::<HashSet<_>>()
    );

    let test1_acc = env.view_account(&"test1".parse().unwrap());
    // Staked 2 * X, sent 3 * X to test3.
    assert_eq!(
        (test1_acc.amount, test1_acc.locked),
        (
            TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE.checked_mul(5).unwrap()).unwrap(),
            TESTING_INIT_STAKE.checked_mul(2).unwrap()
        )
    );
    let test2_acc = env.view_account(&"test2".parse().unwrap());
    // Become fishermen instead
    assert_eq!((test2_acc.amount, test2_acc.locked), (TESTING_INIT_BALANCE, Balance::ZERO));
    let test3_acc = env.view_account(&"test3".parse().unwrap());
    // Got 3 * X, staking 2 * X of them.
    assert_eq!(
        (test3_acc.amount, test3_acc.locked),
        (TESTING_INIT_STAKE, TESTING_INIT_STAKE.checked_mul(2).unwrap())
    );
}

/// One validator tries to decrease their stake in epoch T. Make sure that the stake return happens in epoch T+3.
#[test]
fn test_validator_stake_change() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);

    let desired_stake = TESTING_INIT_STAKE.checked_mul(2).unwrap().checked_div(3).unwrap();
    let staking_transaction = stake(1, &signer, &block_producers[0], desired_stake);
    env.step_default(vec![staking_transaction]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=4 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 5..=7 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(desired_stake).unwrap());
    assert_eq!(account.locked, desired_stake);
}

#[test]
fn test_validator_stake_change_multiple_times() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction = stake(
        1,
        &signers[0],
        &block_producers[0],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(1)).unwrap(),
    );
    let staking_transaction1 = stake(
        2,
        &signers[0],
        &block_producers[0],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(2)).unwrap(),
    );
    let staking_transaction2 = stake(
        1,
        &signers[1],
        &block_producers[1],
        TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap(),
    );
    env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let staking_transaction = stake(
        3,
        &signers[0],
        &block_producers[0],
        TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap(),
    );
    let staking_transaction1 = stake(
        2,
        &signers[1],
        &block_producers[1],
        TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(2)).unwrap(),
    );
    let staking_transaction2 = stake(
        3,
        &signers[1],
        &block_producers[1],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(1)).unwrap(),
    );
    let staking_transaction3 = stake(
        1,
        &signers[3],
        &block_producers[3],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(1)).unwrap(),
    );
    env.step_default(vec![
        staking_transaction,
        staking_transaction1,
        staking_transaction2,
        staking_transaction3,
    ]);

    for _ in 3..=8 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_sub(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap());

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 9..=12 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_sub(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap());

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 13..=16 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_sub(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap());

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_add(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(1)).unwrap());

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_add(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(1)).unwrap());
}

#[test]
fn test_stake_in_last_block_of_an_epoch() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 5, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let staking_transaction = stake(
        1,
        &signers[0],
        &block_producers[0],
        TESTING_INIT_STAKE.checked_add(TESTING_INIT_STAKE.checked_div(6).unwrap()).unwrap(),
    );
    env.step_default(vec![staking_transaction]);
    for _ in 2..10 {
        env.step_default(vec![]);
    }
    let staking_transaction = stake(
        2,
        &signers[0],
        &block_producers[0],
        TESTING_INIT_STAKE.checked_add(TESTING_INIT_STAKE.checked_div(2).unwrap()).unwrap(),
    );
    env.step_default(vec![staking_transaction]);
    env.step_default(vec![]);
    let staking_transaction = stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
    env.step_default(vec![staking_transaction]);
    for _ in 13..=16 {
        env.step_default(vec![]);
    }
    let account = env.view_account(block_producers[0].validator_id());
    let return_stake = TESTING_INIT_STAKE
        .checked_add(TESTING_INIT_STAKE.checked_div(2).unwrap())
        .unwrap()
        .checked_sub(
            TESTING_INIT_STAKE.checked_add(TESTING_INIT_STAKE.checked_div(6).unwrap()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(
                TESTING_INIT_STAKE.checked_add(TESTING_INIT_STAKE.checked_div(2).unwrap()).unwrap()
            )
            .unwrap()
            .checked_add(return_stake)
            .unwrap()
    );
    assert_eq!(
        account.locked,
        TESTING_INIT_STAKE
            .checked_add(TESTING_INIT_STAKE.checked_div(2).unwrap())
            .unwrap()
            .checked_sub(return_stake)
            .unwrap()
    );
}

#[test]
fn test_verify_validator_signature() {
    let validators =
        (0..2).map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap()).collect::<Vec<_>>();
    let env = TestEnv::new(vec![validators.clone()], 2, true);
    let data = [0; 32];
    let signer = InMemorySigner::test_signer(&validators[0]);
    let signature = signer.sign(&data);
    assert!(
        env.epoch_manager
            .verify_validator_signature(&env.head.epoch_id, &validators[0], &data, &signature)
            .unwrap()
    );
}

// TODO (#7327): enable test when flat storage will support state sync.
#[ignore]
#[test]
fn test_state_sync() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    let staking_transaction = stake(
        1,
        &signer,
        &block_producers[0],
        TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap(),
    );
    env.step_default(vec![staking_transaction]);
    env.step_default(vec![]);
    let block_hash = hash(&[env.head.height as u8]);

    let shard_layout = env.epoch_manager.get_shard_layout(&env.head.epoch_id).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let state_part = env
        .runtime
        .obtain_state_part(shard_id, &block_hash, &env.state_roots[0], StatePartId::new(0, 1))
        .unwrap();
    let root_node =
        env.runtime.get_state_root_node(shard_id, &block_hash, &env.state_roots[0]).unwrap();
    let mut new_env = TestEnv::new(vec![validators], 2, false);
    for i in 1..=2 {
        let prev_hash = hash(&[new_env.head.height as u8]);
        let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
        let proposals = if i == 1 {
            vec![ValidatorStake::new(
                block_producers[0].validator_id().clone(),
                block_producers[0].public_key(),
                TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap(),
            )]
        } else {
            vec![]
        };
        new_env
            .epoch_manager
            .record_block_info(
                BlockInfo::new(
                    cur_hash,
                    i,
                    i.saturating_sub(2),
                    prev_hash,
                    prev_hash,
                    new_env.last_proposals,
                    vec![true],
                    new_env.runtime.genesis_config.total_supply,
                    new_env.runtime.genesis_config.protocol_version,
                    new_env.time,
                    ChunkEndorsementsBitmap::from_endorsements(vec![
                        vec![true; num_nodes as usize];
                        shard_layout.num_shards()
                            as usize
                    ]),
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .store_update
            .commit();
        new_env.head.height = i;
        new_env.head.last_block_hash = cur_hash;
        new_env.head.prev_block_hash = prev_hash;
        new_env.last_proposals = proposals;
        new_env.time += 10u64.pow(9);
    }
    assert!(matches!(
        new_env.runtime.validate_state_root_node(&root_node, &env.state_roots[0]),
        StateRootNodeValidationResult::Valid
    ));
    let mut root_node_wrong = root_node;
    root_node_wrong.memory_usage += 1;
    assert!(matches!(
        new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]),
        StateRootNodeValidationResult::Invalid
    ));
    root_node_wrong.data = std::sync::Arc::new([123]);
    assert!(matches!(
        new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]),
        StateRootNodeValidationResult::Invalid
    ));
    assert!(matches!(
        new_env.runtime.validate_state_part(
            ShardId::new(0),
            &Trie::EMPTY_ROOT,
            StatePartId::new(0, 1),
            &state_part
        ),
        StatePartValidationResult::Invalid
    ));
    new_env.runtime.validate_state_part(
        ShardId::new(0),
        &env.state_roots[0],
        StatePartId::new(0, 1),
        &state_part,
    );
    let epoch_id = &new_env.head.epoch_id;
    new_env
        .runtime
        .apply_state_part(
            shard_id,
            &env.state_roots[0],
            StatePartId::new(0, 1),
            &state_part,
            epoch_id,
        )
        .unwrap();
    new_env.state_roots[0] = env.state_roots[0];
    for _ in 3..=5 {
        new_env.step_default(vec![]);
    }

    let account = new_env.view_account(block_producers[0].validator_id());
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_sub(Balance::from_yoctonear(1))
            .unwrap()
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE.checked_add(Balance::from_yoctonear(1)).unwrap());

    let account = new_env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_get_validator_info() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    let staking_transaction = stake(1, &signer, &block_producers[0], Balance::ZERO);
    let mut expected_blocks = [0, 0];
    let mut expected_chunks = [0, 0];
    let mut expected_endorsements = [0, 0];
    let update_validator_stats =
        |env: &mut TestEnv,
         expected_blocks: &mut [u64; 2],
         expected_chunks: &mut [u64; 2],
         expected_endorsements: &mut [u64; 2]| {
            let epoch_id = env.head.epoch_id;
            let height = env.head.height;

            let shard_layout = env.epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let shard_id = shard_layout.shard_ids().next().unwrap();

            let em = env.runtime.epoch_manager.clone();
            let bp = em.get_block_producer_info(&epoch_id, height).unwrap();
            let cp_key = ChunkProductionKey { epoch_id, height_created: height, shard_id };
            let cp = em.get_chunk_producer_info(&cp_key).unwrap();
            let stateless_validators =
                em.get_chunk_validator_assignments(&epoch_id, shard_id, height).ok();

            if let Some(vs) = stateless_validators {
                if vs.contains(&validators[0]) {
                    expected_endorsements[0] += 1;
                }
                if vs.contains(&validators[1]) {
                    expected_endorsements[1] += 1;
                }
            }

            if bp.account_id() == "test1" {
                expected_blocks[0] += 1;
            } else {
                expected_blocks[1] += 1;
            }

            if cp.account_id() == "test1" {
                expected_chunks[0] += 1;
            } else {
                expected_chunks[1] += 1;
            }
        };
    env.step_default(vec![staking_transaction]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );
    assert!(
        env.epoch_manager
            .get_validator_info(ValidatorInfoIdentifier::EpochId(env.head.epoch_id))
            .is_err()
    );
    env.step_default(vec![]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );

    let shard_layout = env.epoch_manager.get_shard_layout(&env.head.epoch_id).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let mut current_epoch_validator_info = vec![
        CurrentEpochValidatorInfo {
            account_id: "test1".parse().unwrap(),
            public_key: block_producers[0].public_key(),
            is_slashed: false,
            stake: TESTING_INIT_STAKE,
            shards_produced: vec![shard_id],
            shards_endorsed: vec![shard_id],
            num_produced_blocks: expected_blocks[0],
            num_expected_blocks: expected_blocks[0],
            num_produced_chunks: expected_chunks[0],
            num_expected_chunks: expected_chunks[0],
            num_produced_chunks_per_shard: vec![expected_chunks[0]],
            num_expected_chunks_per_shard: vec![expected_chunks[0]],
            num_produced_endorsements: expected_endorsements[0],
            num_expected_endorsements: expected_endorsements[0],
            num_expected_endorsements_per_shard: vec![expected_endorsements[0]],
            num_produced_endorsements_per_shard: vec![expected_endorsements[0]],
        },
        CurrentEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            is_slashed: false,
            stake: TESTING_INIT_STAKE,
            shards_produced: vec![shard_id],
            shards_endorsed: vec![shard_id],
            num_produced_blocks: expected_blocks[1],
            num_expected_blocks: expected_blocks[1],
            num_produced_chunks: expected_chunks[1],
            num_expected_chunks: expected_chunks[1],
            num_produced_chunks_per_shard: vec![expected_chunks[1]],
            num_expected_chunks_per_shard: vec![expected_chunks[1]],
            num_produced_endorsements: expected_endorsements[1],
            num_expected_endorsements: expected_endorsements[1],
            num_expected_endorsements_per_shard: vec![expected_endorsements[1]],
            num_produced_endorsements_per_shard: vec![expected_endorsements[1]],
        },
    ];
    let next_epoch_validator_info = vec![
        NextEpochValidatorInfo {
            account_id: "test1".parse().unwrap(),
            public_key: block_producers[0].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![shard_id],
        },
        NextEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![shard_id],
        },
    ];
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert_eq!(
        response,
        EpochValidatorInfo {
            current_validators: current_epoch_validator_info.clone(),
            next_validators: next_epoch_validator_info,
            current_fishermen: vec![],
            next_fishermen: vec![],
            current_proposals: vec![
                ValidatorStake::new(
                    "test1".parse().unwrap(),
                    block_producers[0].public_key(),
                    Balance::ZERO,
                )
                .into()
            ],
            prev_epoch_kickout: Default::default(),
            epoch_start_height: 1,
            epoch_height: 1,
            validator_reward_paid_prev_epoch: HashMap::from([(
                "near".parse().unwrap(),
                Balance::ZERO,
            )]),
        }
    );
    expected_blocks = [0, 0];
    expected_chunks = [0, 0];
    expected_endorsements = [0, 0];
    env.step_default(vec![]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();

    current_epoch_validator_info[0].num_produced_blocks = expected_blocks[0];
    current_epoch_validator_info[0].num_expected_blocks = expected_blocks[0];
    current_epoch_validator_info[0].num_produced_chunks = expected_chunks[0];
    current_epoch_validator_info[0].num_expected_chunks = expected_chunks[0];
    current_epoch_validator_info[0].num_produced_chunks_per_shard = vec![expected_chunks[0]];
    current_epoch_validator_info[0].num_expected_chunks_per_shard = vec![expected_chunks[0]];
    current_epoch_validator_info[0].num_produced_endorsements = expected_endorsements[0];
    current_epoch_validator_info[0].num_expected_endorsements = expected_endorsements[0];
    current_epoch_validator_info[0].num_produced_endorsements_per_shard =
        vec![expected_endorsements[0]];
    current_epoch_validator_info[0].num_expected_endorsements_per_shard =
        vec![expected_endorsements[0]];
    current_epoch_validator_info[1].num_produced_blocks = expected_blocks[1];
    current_epoch_validator_info[1].num_expected_blocks = expected_blocks[1];
    current_epoch_validator_info[1].num_produced_chunks = expected_chunks[1];
    current_epoch_validator_info[1].num_expected_chunks = expected_chunks[1];
    current_epoch_validator_info[1].num_produced_chunks_per_shard = vec![];
    current_epoch_validator_info[1].num_expected_chunks_per_shard = vec![];
    current_epoch_validator_info[1].num_produced_endorsements = expected_endorsements[1];
    current_epoch_validator_info[1].num_expected_endorsements = expected_endorsements[1];
    current_epoch_validator_info[1].num_produced_endorsements_per_shard =
        vec![expected_endorsements[1]];
    current_epoch_validator_info[1].num_expected_endorsements_per_shard =
        vec![expected_endorsements[1]];
    current_epoch_validator_info[1].shards_produced = vec![];
    assert_eq!(response.current_validators, current_epoch_validator_info);
    assert_eq!(
        response.next_validators,
        vec![NextEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![ShardId::new(0)],
        }]
    );
    assert!(response.current_proposals.is_empty());
    assert_eq!(
        response.prev_epoch_kickout,
        vec![ValidatorKickoutView {
            account_id: "test1".parse().unwrap(),
            reason: ValidatorKickoutReason::Unstaked
        }]
    );
    assert_eq!(response.epoch_start_height, 3);
}

/// Run 4 validators. Two of them first change their stake to below validator threshold but above
/// fishermen threshold. Make sure their balance is correct. Then one fisherman increases their
/// stake to become a validator again while the other one decreases to below fishermen threshold.
/// Check that the first one becomes a validator and the second one gets unstaked completely.
#[test]
fn test_fishermen_stake() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: 4,
            has_reward: false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            minimum_stake_divisor: Some(20000),
            zero_fees: true,
            create_flat_storage: true,
            runtime_config_store: None,
        },
    );
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let fishermen_stake = Balance::from_near(3300).checked_add(Balance::from_yoctonear(1)).unwrap();

    let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], fishermen_stake);
    env.step_default(vec![staking_transaction, staking_transaction1]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=13 {
        env.step_default(vec![]);
    }
    let account0 = env.view_account(block_producers[0].validator_id());
    assert!(account0.locked.is_zero());
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
    let staking_transaction = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], Balance::ZERO);
    env.step_default(vec![staking_transaction, staking_transaction2]);

    for _ in 13..=25 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert_eq!(account0.locked, TESTING_INIT_STAKE);
    assert_eq!(account0.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());

    let account1 = env.view_account(block_producers[1].validator_id());
    assert!(account1.locked.is_zero());
    assert_eq!(account1.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
}

/// Test that when fishermen unstake they get their tokens back.
#[test]
fn test_fishermen_unstake() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: 2,
            has_reward: false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            minimum_stake_divisor: Some(20000),
            zero_fees: true,
            create_flat_storage: true,
            runtime_config_store: None,
        },
    );
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let fishermen_stake = Balance::from_near(3300).checked_add(Balance::from_yoctonear(1)).unwrap();

    let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
    env.step_default(vec![staking_transaction]);
    for _ in 2..9 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert!(account0.locked.is_zero());
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
    let staking_transaction = stake(2, &signers[0], &block_producers[0], Balance::ZERO);
    env.step_default(vec![staking_transaction]);
    for _ in 10..17 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert!(account0.locked.is_zero());
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
}

/// Enable reward and make sure that validators get reward proportional to their stake.
#[test]
fn test_validator_reward() {
    init_test_logger();
    let num_nodes = 4;
    let epoch_length = 40;
    let validators =
        (0..num_nodes).map(|i| format!("test{}", i + 1).parse().unwrap()).collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], epoch_length, true);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();

    for _ in 0..(epoch_length + 1) {
        env.step_default(vec![]);
    }

    let (validator_reward, protocol_treasury_reward) =
        env.compute_reward(num_nodes, epoch_length * 10u64.pow(9));
    for i in 0..4 {
        let account = env.view_account(block_producers[i].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE.checked_add(validator_reward).unwrap());
    }

    let protocol_treasury_account =
        env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
    assert_eq!(
        protocol_treasury_account.amount,
        TESTING_INIT_BALANCE.checked_add(protocol_treasury_reward).unwrap()
    );
}

#[test]
fn test_delete_account_after_unstake() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], Balance::ZERO);
    env.step_default(vec![staking_transaction1]);
    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap());
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=5 {
        env.step_default(vec![]);
    }
    let staking_transaction2 =
        stake(2, &signers[1], &block_producers[1], Balance::from_yoctonear(1));
    env.step_default(vec![staking_transaction2]);
    for _ in 7..=13 {
        env.step_default(vec![]);
    }
    let account = env.view_account(block_producers[1].validator_id());
    assert!(account.locked.is_zero());

    let delete_account_transaction = SignedTransaction::from_actions(
        4,
        signers[1].get_account_id(),
        signers[1].get_account_id(),
        &signers[1],
        vec![Action::DeleteAccount(DeleteAccountAction {
            beneficiary_id: signers[0].get_account_id(),
        })],
        // runtime does not validate block history
        CryptoHash::default(),
    );
    env.step_default(vec![delete_account_transaction]);
    for _ in 15..=17 {
        env.step_default(vec![]);
    }
}

/// An external call in the first chunk of a new epoch sees the chunk's parent block, which is
/// the last block of the previous epoch: its height, its epoch height, and the validator stakes
/// of its epoch. The contract's account is a validator whose stake changes at that boundary.
#[test]
fn test_ecc_in_first_chunk_of_epoch_sees_parent_epoch() {
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        return;
    }
    init_test_logger();
    let validators: Vec<AccountId> = vec!["test1".parse().unwrap()];
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let contract_id = validators[0].clone();
    let signer = InMemorySigner::test_signer(&contract_id);
    // The ECC method returns the block height, the epoch height and the contract account's
    // validator stake it sees, in that order.
    let code = near_test_contracts::wat_contract(&format!(
        r#"(module
  (import "env" "block_index" (func $block_index (result i64)))
  (import "env" "epoch_height" (func $epoch_height (result i64)))
  (import "env" "validator_stake" (func $validator_stake (param i64 i64 i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)
  (data (i32.const 100) "{contract_id}")
  (func (export "{ECC_METHOD_NAME}")
    (i64.store (i32.const 0) (call $block_index))
    (i64.store (i32.const 8) (call $epoch_height))
    (call $validator_stake (i64.const {account_id_len}) (i64.const 100) (i64.const 16))
    (call $value_return (i64.const 32) (i64.const 0)))
)"#,
        account_id_len = contract_id.len(),
    ));
    let deploy = SignedTransaction::from_actions(
        1,
        contract_id.clone(),
        contract_id.clone(),
        &signer,
        vec![Action::DeployContract(DeployContractAction { code })],
        // runtime does not validate block history
        CryptoHash::default(),
    );
    // A stake proposal takes effect two epochs later, at the boundary the ECC is applied at.
    let block_producer = create_test_signer(contract_id.as_str());
    let new_stake = TESTING_INIT_STAKE.checked_mul(2).unwrap().checked_div(3).unwrap();
    env.step_default(vec![deploy, stake(2, &signer, &block_producer, new_stake)]);
    // The epoch height doesn't change at the first epoch boundary after genesis, so step to the
    // end of the epoch after it.
    for _ in 0..2 {
        env.step_default(vec![]);
        while !env.epoch_manager.is_next_block_epoch_start(&env.head.last_block_hash).unwrap() {
            env.step_default(vec![]);
        }
    }

    let parent = env.head.last_block_hash;
    let parent_epoch_id = env.epoch_manager.get_epoch_id(&parent).unwrap();
    let parent_epoch_height =
        env.epoch_manager.get_epoch_info(&parent_epoch_id).unwrap().epoch_height();
    let chunk_epoch_height = env.epoch_manager.get_epoch_height_from_prev_block(&parent).unwrap();
    assert_ne!(chunk_epoch_height, parent_epoch_height);
    let chunk_epoch_id = env.epoch_manager.get_epoch_id_from_prev_block(&parent).unwrap();
    let stake_in = |epoch_id| {
        env.epoch_manager.get_validator_by_account_id(epoch_id, &contract_id).unwrap().stake()
    };
    let parent_stake = stake_in(&parent_epoch_id);
    assert_eq!(parent_stake, TESTING_INIT_STAKE);
    assert_eq!(stake_in(&chunk_epoch_id), new_stake);

    let ecc = EccTransaction::new(contract_id, vec![]);
    let apply_result = env.apply_new_chunk(ShardId::new(0), vec![ecc], &[]);
    let [outcome] = apply_result.outcomes.as_slice() else {
        panic!("expected one outcome, got {:?}", apply_result.outcomes);
    };
    let mut seen = env.head.height.to_le_bytes().to_vec();
    seen.extend(parent_epoch_height.to_le_bytes());
    seen.extend(parent_stake.as_yoctonear().to_le_bytes());
    assert_eq!(outcome.outcome.status, ExecutionStatus::SuccessValue(seen));
}

#[test]
fn test_proposal_deduped() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 = stake(
        1,
        &signers[1],
        &block_producers[1],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(100)).unwrap(),
    );
    let staking_transaction2 = stake(
        2,
        &signers[1],
        &block_producers[1],
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(10)).unwrap(),
    );
    env.step_default(vec![staking_transaction1, staking_transaction2]);
    assert_eq!(env.last_proposals.len(), 1);
    assert_eq!(
        env.last_proposals[0].stake(),
        TESTING_INIT_STAKE.checked_sub(Balance::from_yoctonear(10)).unwrap()
    );
}

#[test]
fn test_insufficient_stake() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 =
        stake(1, &signers[1], &block_producers[1], Balance::from_yoctonear(100));
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], Balance::from_near(100));
    env.step_default(vec![staking_transaction1, staking_transaction2]);
    assert!(env.last_proposals.is_empty());
    let staking_transaction3 = stake(3, &signers[1], &block_producers[1], Balance::ZERO);
    env.step_default(vec![staking_transaction3]);
    assert_eq!(env.last_proposals.len(), 1);
    assert!(env.last_proposals[0].stake().is_zero());
}

/// Check that flat state is included into trie and is not included into view trie, because we can't apply flat
/// state optimization to view calls.
#[test]
fn test_flat_state_usage() {
    let env = TestEnv::new(vec![vec!["test1".parse().unwrap()]], 4, false);
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let state_root = Trie::EMPTY_ROOT;

    let trie = env.runtime.get_trie_for_shard(shard_id, &prev_hash, state_root, true).unwrap();
    assert!(trie.has_flat_storage_chunk_view());

    let trie = env.runtime.get_view_trie_for_shard(shard_id, &prev_hash, state_root).unwrap();
    assert!(!trie.has_flat_storage_chunk_view());
}

/// Check that querying trie and flat state gives the same result.
#[test]
fn test_trie_and_flat_state_equality() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let transfer_tx = SignedTransaction::from_actions(
        4,
        signers[0].get_account_id(),
        validators[1].clone(),
        &signers[0],
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(10) })],
        // runtime does not validate block history
        CryptoHash::default(),
    );
    env.step_default(vec![transfer_tx]);
    for _ in 1..=5 {
        env.step_default(vec![]);
    }

    // Extract account in two ways:
    // - using state trie, which should use flat state after enabling it in the protocol
    // - using view state, which should never use flat state
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let state_root = env.state_roots[0];
    let state = env.runtime.get_trie_for_shard(shard_id, &prev_hash, state_root, true).unwrap();
    let view_state = env.runtime.get_view_trie_for_shard(shard_id, &prev_hash, state_root).unwrap();
    let trie_key = TrieKey::Account { account_id: validators[1].clone() };
    let key = trie_key.to_vec();

    let state_value = state.get(&key, AccessOptions::DEFAULT).unwrap().unwrap();
    let account = Account::try_from_slice(&state_value).unwrap();
    assert_eq!(
        account.amount(),
        TESTING_INIT_BALANCE
            .checked_sub(TESTING_INIT_STAKE)
            .unwrap()
            .checked_add(Balance::from_yoctonear(10))
            .unwrap()
    );

    let view_state_value = view_state.get(&key, AccessOptions::DEFAULT).unwrap().unwrap();
    assert_eq!(state_value, view_state_value);
}

/// Check that mainnet genesis hash still matches, to make sure that we're still backwards compatible.
#[test]
fn test_genesis_hash() {
    let genesis = near_mainnet_res::mainnet_genesis();
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let store = near_store::test_utils::create_test_store();

    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test_with_runtime_config_store(
        tempdir.path(),
        store.clone(),
        FilesystemContractRuntimeCache::new(
            tempdir.path(),
            None::<&str>,
            "contract.cache",
            FilesystemContractRuntimeCache::MAX_DISK_CACHE_BYTES,
        )
        .expect("filesystem contract cache")
        .handle(),
        &genesis.config,
        epoch_manager.clone(),
        RuntimeConfigStore::new(),
    );

    let state_roots =
        get_genesis_state_roots(runtime.store()).expect("genesis should be initialized.");
    let (block, _chunks) = Chain::make_genesis_block(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &chain_genesis,
        state_roots,
    )
    .unwrap();
    assert_eq!(block.header().hash().to_string(), "EPnLgE7iEq9s7yTkos96M3cWymH5avBAPm3qx3NXqR8H");

    let epoch_manager = EpochManager::new_arc_handle(store, &genesis.config, None);
    let epoch_info = epoch_manager.get_epoch_info(&EpochId::default()).unwrap();
    // Verify the order of the block producers.
    assert_eq!(
        [
            1, 0, 1, 0, 0, 3, 3, 2, 2, 3, 0, 2, 0, 0, 1, 1, 1, 1, 3, 2, 3, 2, 0, 3, 3, 3, 0, 3, 1,
            3, 1, 0, 1, 2, 3, 0, 1, 0, 0, 0, 2, 2, 2, 3, 3, 3, 3, 1, 2, 0, 1, 0, 1, 0, 3, 2, 1, 2,
            0, 1, 3, 3, 1, 2, 1, 2, 1, 0, 2, 3, 1, 2, 1, 2, 3, 2, 0, 3, 3, 2, 0, 0, 2, 3, 0, 3, 0,
            2, 3, 1, 1, 2, 1, 0, 1, 2, 2, 1, 2, 0
        ],
        epoch_info.block_producers_settlement()
    );
}

/// Creates a signed transaction between each pair of `signers`,
/// where transaction outcomes from a single signer differ by nonce.
/// The transactions are then shuffled and used to fill a transaction pool.
fn generate_transaction_pool(signers: &[Signer], block_hash: CryptoHash) -> TransactionPool {
    let mut rng = StdRng::from_seed(TEST_SEED);
    let signer_count = signers.len();
    let num_rounds = signer_count - 1;

    let mut transactions = vec![];
    for round in 1..=num_rounds {
        for i in 0..signer_count {
            let transaction = SignedTransaction::send_money(
                round.try_into().unwrap(),
                signers[i].get_account_id(),
                signers[(i + round) % signer_count].get_account_id(),
                &signers[i],
                Balance::from_yoctonear(round.try_into().unwrap()),
                block_hash,
            );
            let validated_tx = ValidatedTransaction::new_for_test(transaction);
            transactions.push(validated_tx);
        }
    }
    transactions.shuffle(&mut rng);

    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    for transaction in transactions {
        assert_eq!(pool.insert_transaction(transaction), InsertTransactionResult::Success);
    }
    pool
}

fn get_test_env_with_chain_and_pool() -> (TestEnv, Chain, TransactionPool) {
    get_test_env_with_chain_and_pool_for(None)
}

fn get_test_env_with_chain_and_pool_for(
    runtime_config_store: Option<RuntimeConfigStore>,
) -> (TestEnv, Chain, TransactionPool) {
    let validators = (0..NUM_TEST_SIGNERS)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let chain_genesis = ChainGenesis::new(&GenesisConfig::test(Clock::real()));
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: chain_genesis.epoch_length,
            has_reward: false,
            minimum_stake_divisor: None,
            zero_fees: false,
            create_flat_storage: false,
            runtime_config_store,
        },
    );

    let chain = Chain::new(
        Clock::real(),
        env.epoch_manager.clone(),
        ShardTracker::new_empty(env.epoch_manager.clone()),
        env.runtime.clone(),
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
        Default::default(),
        Default::default(),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
        None,
    )
    .unwrap();

    // Make sure `chain` and test `env` use the same genesis hash.
    env.head = Tip::clone(&chain.chain_store().head().unwrap());
    // Produce a single block, so that `prev_block_hash` is valid.
    env.step_default(vec![]);

    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let transaction_pool = generate_transaction_pool(&signers, env.head.prev_block_hash);
    (env, chain, transaction_pool)
}

/// The signed transactions among the prepared ones.
fn signed_txs(txs: &[ValidatedTransactionEnvelope]) -> impl Iterator<Item = &ValidatedTransaction> {
    txs.iter().filter_map(|tx| match tx {
        ValidatedTransactionEnvelope::Signed(tx) => Some(tx),
        ValidatedTransactionEnvelope::Unsigned(_) => None,
    })
}

/// The time ECCs get in the tests, so long that no run ends up late.
const TEST_ECC_TIME_LIMIT: Duration = Duration::seconds(10);

/// The time limits of the tests: `total` for the whole preparation, and time for ECCs that no run
/// of a test goes over, so the tests do not depend on how fast the machine is.
fn time_limits(total: Option<Duration>) -> PrepareTransactionsTimeLimits {
    PrepareTransactionsTimeLimits { total, ecc: TEST_ECC_TIME_LIMIT }
}

/// The context to prepare transactions for the chain's head.
fn block_context(
    env: &TestEnv,
    congestion_info: BlockCongestionInfo,
    next_epoch_id: EpochId,
) -> PrepareTransactionsBlockContext {
    let prev_hash = env.head.prev_block_hash;
    let epoch_id = env.epoch_manager.get_epoch_id(&prev_hash).unwrap();
    PrepareTransactionsBlockContext {
        next_gas_price: env.runtime.genesis_config.min_gas_price,
        height: env.head.height,
        timestamp: 0,
        epoch_id,
        epoch_height: env.epoch_manager.get_epoch_info(&epoch_id).unwrap().epoch_height(),
        next_epoch_id,
        congestion_info,
        gas_limit: env.runtime.genesis_config.gas_limit,
    }
}

fn prepare_transactions(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    storage_config: RuntimeStorageConfig,
) -> Result<PreparedTransactions, Error> {
    let limits = time_limits(default_produce_chunk_add_transactions_time_limit());
    prepare_transactions_with_limits(env, chain, transaction_groups, storage_config, limits)
        .map(|(prepared, _skipped)| prepared)
}

/// Like `prepare_transactions`, with the given time limits, and the skipped transactions.
fn prepare_transactions_with_limits(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    storage_config: RuntimeStorageConfig,
    time_limits: PrepareTransactionsTimeLimits,
) -> Result<(PreparedTransactions, SkippedTransactions), Error> {
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let block = chain.get_block(&prev_hash).unwrap();
    let congestion_info = block.block_congestion_info();
    let next_epoch_id = env.epoch_manager.get_epoch_id_from_prev_block(&prev_hash)?;

    env.runtime.prepare_transactions(
        storage_config,
        shard_id,
        block_context(env, congestion_info, next_epoch_id),
        transaction_groups,
        &mut |tx: &SignedTransaction| -> bool {
            chain
                .chain_store()
                .check_transaction_validity_period(&block.header(), tx.transaction.block_hash())
                .is_ok()
        },
        &|_| true,
        time_limits,
    )
}

fn prepare_transactions_extra(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    skip_tx_hashes: HashSet<CryptoHash>,
    validate_tx_ttl: &dyn Fn(&SignedTransaction) -> bool,
    check_pending: &mut dyn FnMut(&SignedTransaction) -> PendingTxCheckResult,
    cancel: Option<Arc<AtomicBool>>,
) -> Result<(PreparedTransactions, SkippedTransactions), Error> {
    prepare_transactions_extra_with_time_limit(
        env,
        chain,
        transaction_groups,
        skip_tx_hashes,
        validate_tx_ttl,
        check_pending,
        cancel,
        default_produce_chunk_add_transactions_time_limit(),
    )
}

fn prepare_transactions_extra_with_time_limit(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    skip_tx_hashes: HashSet<CryptoHash>,
    validate_tx_ttl: &dyn Fn(&SignedTransaction) -> bool,
    check_pending: &mut dyn FnMut(&SignedTransaction) -> PendingTxCheckResult,
    cancel: Option<Arc<AtomicBool>>,
    time_limit: Option<Duration>,
) -> Result<(PreparedTransactions, SkippedTransactions), Error> {
    prepare_transactions_extra_in_state(
        env,
        chain,
        transaction_groups,
        skip_tx_hashes,
        validate_tx_ttl,
        check_pending,
        cancel,
        time_limits(time_limit),
        None,
        |_| {},
        |_| {},
    )
}

/// Like `prepare_transactions_extra_with_time_limit`, with the state changed by `change_state` and
/// the block context by `change_context`. `parent_nodes` is as for `prepare_transactions_extra`.
fn prepare_transactions_extra_in_state(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    skip_tx_hashes: HashSet<CryptoHash>,
    validate_tx_ttl: &dyn Fn(&SignedTransaction) -> bool,
    check_pending: &mut dyn FnMut(&SignedTransaction) -> PendingTxCheckResult,
    cancel: Option<Arc<AtomicBool>>,
    time_limits: PrepareTransactionsTimeLimits,
    parent_nodes: Option<ParentPostStateNodes>,
    change_state: impl FnOnce(&mut TrieUpdate),
    change_context: impl FnOnce(&mut PrepareTransactionsBlockContext),
) -> Result<(PreparedTransactions, SkippedTransactions), Error> {
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();
    let block = chain.get_block(&prev_hash).unwrap();
    let congestion_info = block.block_congestion_info();
    let next_epoch_id = env.epoch_manager.get_epoch_id_from_prev_block(&prev_hash)?;

    let mut trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
    trie = trie.recording_reads_new_recorder();
    let mut state_update = TrieUpdate::new(trie);
    change_state(&mut state_update);
    let mut block_context = block_context(env, congestion_info, next_epoch_id);
    change_context(&mut block_context);

    env.runtime.prepare_transactions_extra(
        state_update,
        shard_id,
        block_context,
        transaction_groups,
        &mut |tx: &SignedTransaction| -> bool {
            chain
                .chain_store()
                .check_transaction_validity_period(&block.header(), tx.transaction.block_hash())
                .is_ok()
        },
        validate_tx_ttl,
        skip_tx_hashes,
        check_pending,
        time_limits,
        cancel,
        parent_nodes,
    )
}

/// Whether the producer packs ECCs in this build: not before the feature, and not under Spice yet.
/// If not, logs that the test is skipped.
fn eccs_are_packed() -> bool {
    let packed = ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        && !ProtocolFeature::Spice.enabled(PROTOCOL_VERSION);
    if !packed {
        init_test_logger();
        tracing::info!("skipping: ECCs are not packed in this build");
    }
    packed
}

fn ecc(contract_id: &str, tag: u8) -> ValidatedEcc {
    ValidatedEcc::new_for_test(EccTransaction::new(contract_id.parse().unwrap(), vec![tag]))
}

fn pool_with_eccs(eccs: impl IntoIterator<Item = ValidatedEcc>) -> TransactionPool {
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    for ecc in eccs {
        assert_eq!(pool.insert_transaction(ecc), InsertTransactionResult::Success);
    }
    pool
}

/// Prepares the transactions of the pool, in a state where the accounts `with_contract` hold a
/// contract and the other ones do not.
fn prepare_eccs(
    env: &TestEnv,
    chain: &Chain,
    pool: &mut TransactionPool,
    skip_tx_hashes: HashSet<CryptoHash>,
    with_contract: &[&str],
) -> (PreparedTransactions, SkippedTransactions) {
    let change_state = give_contracts(with_contract);
    prepare_eccs_with_context(env, chain, pool, skip_tx_hashes, change_state, |_| {})
}

fn prepare_eccs_with_context(
    env: &TestEnv,
    chain: &Chain,
    pool: &mut TransactionPool,
    skip_tx_hashes: HashSet<CryptoHash>,
    change_state: impl FnOnce(&mut TrieUpdate),
    change_context: impl FnOnce(&mut PrepareTransactionsBlockContext),
) -> (PreparedTransactions, SkippedTransactions) {
    prepare_eccs_with_limits(
        env,
        chain,
        pool,
        skip_tx_hashes,
        time_limits(None),
        change_state,
        change_context,
    )
}

fn prepare_eccs_with_limits(
    env: &TestEnv,
    chain: &Chain,
    pool: &mut TransactionPool,
    skip_tx_hashes: HashSet<CryptoHash>,
    time_limits: PrepareTransactionsTimeLimits,
    change_state: impl FnOnce(&mut TrieUpdate),
    change_context: impl FnOnce(&mut PrepareTransactionsBlockContext),
) -> (PreparedTransactions, SkippedTransactions) {
    prepare_transactions_extra_in_state(
        env,
        chain,
        &mut pool.pool_iterator(),
        skip_tx_hashes,
        &|_| true,
        &mut PendingTxCheckResult::always_admit(),
        None,
        time_limits,
        None,
        change_state,
        change_context,
    )
    .unwrap()
}

/// The tag of the ECCs that `ecc_test_contract` refuses.
const REFUSED_TAG: u8 = 255;

/// The tag of the ECCs whose run in `ecc_test_contract` fails with an error of the runtime, not a
/// refusal of the contract: it asks for the total stake of the epoch of the parent block.
const FAILING_TAG: u8 = 254;

/// A contract whose ECC method reads a key of its storage, refuses the call with the payload
/// `[REFUSED_TAG]`, asks for the total stake with `[FAILING_TAG]`, and accepts any other one.
fn ecc_test_contract() -> Vec<u8> {
    near_test_contracts::wat_contract(&format!(
        r#"(module
  (import "env" "input" (func $input (param i64)))
  (import "env" "read_register" (func $read_register (param i64 i64)))
  (import "env" "storage_read" (func $storage_read (param i64 i64 i64) (result i64)))
  (import "env" "validator_total_stake" (func $validator_total_stake (param i64)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "k")
  (func (export "{ECC_METHOD_NAME}")
    (drop (call $storage_read (i64.const 1) (i64.const 1024) (i64.const 1)))
    (call $input (i64.const 0))
    (call $read_register (i64.const 0) (i64.const 0))
    (if (i32.eq (i32.load8_u (i32.const 0)) (i32.const {REFUSED_TAG})) (then unreachable))
    (if (i32.eq (i32.load8_u (i32.const 0)) (i32.const {FAILING_TAG}))
      (then (call $validator_total_stake (i64.const 2048))))
  )
)"#
    ))
}

/// A change of the state that gives the accounts the contract from `ecc_test_contract`.
fn give_contracts(account_ids: &[&str]) -> impl FnOnce(&mut TrieUpdate) + use<> {
    let account_ids = account_ids.iter().map(|id| id.parse::<AccountId>().unwrap()).collect_vec();
    move |state_update| {
        let code = ContractCode::new(ecc_test_contract(), None);
        for account_id in account_ids {
            let mut account = get_account(state_update, &account_id).unwrap().unwrap();
            account.set_contract(AccountContract::Local(*code.hash())).unwrap();
            set_account(state_update, account_id.clone(), &account);
            state_update.set_code(account_id, &code);
            state_update.record_contract_deploy(code.clone_for_tests());
        }
    }
}

/// A change of the state that gives each account its contract. It puts no code behind it.
fn give_account_contracts(
    contracts: Vec<(&str, AccountContract)>,
) -> impl FnOnce(&mut TrieUpdate) + use<> {
    let contracts = contracts
        .into_iter()
        .map(|(account_id, contract)| (account_id.parse::<AccountId>().unwrap(), contract))
        .collect_vec();
    move |state_update| {
        for (account_id, contract) in contracts {
            let mut account = get_account(state_update, &account_id).unwrap().unwrap();
            account.set_contract(contract).unwrap();
            set_account(state_update, account_id, &account);
        }
    }
}

/// What the ECCs among `txs` are: contract and tag.
fn packed_eccs(txs: &[ValidatedTransactionEnvelope]) -> Vec<(String, u8)> {
    txs.iter()
        .filter_map(|tx| match tx {
            ValidatedTransactionEnvelope::Unsigned(ecc) => {
                Some((ecc.contract_id().to_string(), ecc.payload()[0]))
            }
            ValidatedTransactionEnvelope::Signed(_) => None,
        })
        .sorted()
        .collect()
}

/// The tags of the ECCs in the pool, by contract, taken out of the pool.
fn drain_eccs_from_pool(pool: &mut TransactionPool) -> Vec<(String, Vec<u8>)> {
    let mut iter = pool.pool_iterator();
    let mut eccs = vec![];
    while let Some(group) = iter.next() {
        if let PooledGroup::Ecc(group) = group {
            let mut tags = vec![];
            let mut contract_id = None;
            while let Some(ecc) = group.next() {
                contract_id = Some(ecc.contract_id().to_string());
                tags.push(ecc.payload()[0]);
            }
            eccs.extend(contract_id.map(|contract_id| (contract_id, tags)));
        }
    }
    eccs.sort();
    eccs
}

/// The producer takes the first ECC to arrive for each contract, and leaves the rest of them in
/// the pool.
#[test]
fn test_prepare_transactions_packs_one_ecc_per_contract() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([
        ecc("test1", 1),
        ecc("test1", 2),
        ecc("test1", 3),
        ecc("test2", 1),
        ecc("test2", 2),
    ]);

    let (prepared, skipped) =
        prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1", "test2"]);

    assert_eq!(
        packed_eccs(&prepared.transactions),
        vec![("test1".to_string(), 1), ("test2".to_string(), 1)]
    );
    assert!(skipped.0.is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::NoMoreTxsInPool);
    assert_eq!(
        drain_eccs_from_pool(&mut pool),
        vec![("test1".to_string(), vec![2, 3]), ("test2".to_string(), vec![2])]
    );
}

/// An ECC of about 1 KB, which starts with `tag`.
fn kilobyte_ecc(contract_id: &str, tag: u8) -> ValidatedEcc {
    ValidatedEcc::new_for_test(EccTransaction::new(contract_id.parse().unwrap(), vec![tag; 1000]))
}

/// A test env whose ECCs' share of a chunk's transaction bytes holds two `kilobyte_ecc`s, but not
/// three.
fn get_test_env_with_room_for_two_kilobyte_eccs() -> (TestEnv, Chain, TransactionPool) {
    let ecc_size = kilobyte_ecc("test1", 1).get_size();
    let mut runtime_config = RuntimeConfig::test();
    let share_size = 2 * ecc_size + ecc_size / 2;
    let combined_size = runtime_config.witness_config.combined_transactions_size_limit;
    runtime_config.external_calls_config.chunk_transactions_size_share =
        Rational32::new(i32::try_from(share_size).unwrap(), i32::try_from(combined_size).unwrap());
    let ecc_config = &runtime_config.external_calls_config;
    assert_eq!(ecc_config.transactions_size_share(&runtime_config.witness_config), share_size);
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    get_test_env_with_chain_and_pool_for(Some(store))
}

/// Once the chunk's ECCs take their share of its transaction bytes, the rest stay in the pool,
/// and the next chunk takes them.
#[test]
fn test_prepare_transactions_leaves_eccs_past_the_size_share() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_room_for_two_kilobyte_eccs();
    let contracts = ["test1", "test2", "test3", "test4"];
    let mut pool = pool_with_eccs(contracts.map(|contract_id| kilobyte_ecc(contract_id, 1)));

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &contracts);

    let first_chunk = packed_eccs(&prepared.transactions);
    assert_eq!(first_chunk.len(), 2);
    // The ones left go into the next chunk.
    let (prepared, _) = prepare_eccs(
        &env,
        &chain,
        &mut pool,
        prepared.transactions.iter().map(|tx| tx.get_hash()).collect(),
        &contracts,
    );
    let next_chunk = packed_eccs(&prepared.transactions);
    assert_eq!(next_chunk.len(), 2);
    assert!(next_chunk.iter().all(|ecc| !first_chunk.contains(ecc)));
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

/// Once the chunk holds `max_calls_per_chunk` ECCs, the rest stay in the pool, and the next chunk
/// takes them.
#[test]
fn test_prepare_transactions_leaves_eccs_past_the_max_calls() {
    if !eccs_are_packed() {
        return;
    }
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.external_calls_config.max_calls_per_chunk = 2;
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    let (env, chain, _) = get_test_env_with_chain_and_pool_for(Some(store));
    let contracts = ["test1", "test2", "test3", "test4"];
    let mut pool = pool_with_eccs(contracts.map(|contract_id| ecc(contract_id, 1)));

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &contracts);

    let first_chunk = packed_eccs(&prepared.transactions);
    assert_eq!(first_chunk.len(), 2);
    // The ones left go into the next chunk.
    let (prepared, _) = prepare_eccs(
        &env,
        &chain,
        &mut pool,
        prepared.transactions.iter().map(|tx| tx.get_hash()).collect(),
        &contracts,
    );
    let next_chunk = packed_eccs(&prepared.transactions);
    assert_eq!(next_chunk.len(), 2);
    assert!(next_chunk.iter().all(|ecc| !first_chunk.contains(ecc)));
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

/// The ECCs of a chunk take no more than their share of its transaction bytes, or the chunk
/// would be invalid. The ones past the share wait in the pool, and signed transactions still go
/// into the chunk.
#[test]
fn test_prepare_transactions_keeps_eccs_to_their_size_share() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, mut pool) = get_test_env_with_room_for_two_kilobyte_eccs();
    let num_signed_txs = pool.len();
    let contracts = ["test1", "test2", "test3", "test4"];
    for contract_id in contracts {
        let ecc = kilobyte_ecc(contract_id, 1);
        assert_eq!(pool.insert_transaction(ecc), InsertTransactionResult::Success);
    }

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &contracts);

    assert_eq!(packed_eccs(&prepared.transactions).len(), 2);
    assert_eq!(signed_txs(&prepared.transactions).count(), num_signed_txs);
    assert_eq!(drain_eccs_from_pool(&mut pool).len(), 2);
}

/// An ECC that does not fit in what the chunk has left for transactions waits in the pool, and
/// the packing goes on without it.
#[test]
fn test_prepare_transactions_leaves_an_ecc_that_does_not_fit() {
    if !eccs_are_packed() {
        return;
    }
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.combined_transactions_size_limit = 1000;
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    let (env, chain, mut pool) = get_test_env_with_chain_and_pool_for(Some(store));
    let big_ecc =
        ValidatedEcc::new_for_test(EccTransaction::new("test1".parse().unwrap(), vec![1; 2000]));
    assert_eq!(pool.insert_transaction(big_ecc), InsertTransactionResult::Success);

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1"]);

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert!(signed_txs(&prepared.transactions).count() > 0);
    assert_eq!(drain_eccs_from_pool(&mut pool).len(), 1);
}

/// A cancelled preparation stops at the first group of ECCs it visits, as at any other group:
/// the limits are checked before the visit.
#[test]
fn test_prepare_transactions_cancel_stops_at_ecc_groups() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test2", 1)]);

    let (prepared, skipped) = prepare_transactions_extra_in_state(
        &env,
        &chain,
        &mut pool.pool_iterator(),
        HashSet::new(),
        &|_| true,
        &mut PendingTxCheckResult::always_admit(),
        Some(Arc::new(AtomicBool::new(true))),
        time_limits(None),
        None,
        give_contracts(&["test1", "test2"]),
        |_| {},
    )
    .unwrap();

    assert!(prepared.transactions.is_empty());
    assert!(skipped.0.is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::Cancelled);
    assert_eq!(pool.len(), 2);
}

/// An ECC is refused for a contract that does not exist, one that holds no code, and one that
/// cannot pay the fees at the given gas price.
#[test]
fn test_can_accept_ecc_checks_contract_and_fees() {
    let env = TestEnv::new_with_config(
        vec![vec!["test1".parse().unwrap()]],
        TestEnvConfig {
            epoch_length: 5,
            has_reward: false,
            minimum_stake_divisor: None,
            zero_fees: false,
            create_flat_storage: false,
            runtime_config_store: None,
        },
    );
    let shard_layout = env.epoch_manager.get_shard_layout(&env.head.epoch_id).unwrap();
    let min_gas_price = env.runtime.genesis_config.min_gas_price;
    let can_accept = |contract_id: &str, gas_price| {
        let ecc =
            ValidatedEcc::new_for_test(EccTransaction::new(contract_id.parse().unwrap(), vec![1]));
        env.runtime.can_accept_ecc(
            &shard_layout,
            gas_price,
            env.state_roots[0],
            &ecc,
            PROTOCOL_VERSION,
        )
    };

    assert!(matches!(
        can_accept("nobody", min_gas_price),
        Err(InvalidTxError::ContractNotFound { .. })
    ));
    assert!(matches!(
        can_accept("test1", min_gas_price),
        Err(InvalidTxError::ContractNotFound { .. })
    ));
    assert!(matches!(
        can_accept("test1", Balance::from_near(1)),
        Err(InvalidTxError::NotEnoughBalance { .. })
    ));
}

/// A chunk with no room for ECCs, as their share of its transaction bytes is zero, gets none,
/// and they all stay in the pool. Signed transactions are not affected.
#[test]
fn test_prepare_transactions_leaves_eccs_in_pool_when_there_is_no_room() {
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.external_calls_config.chunk_transactions_size_share = Rational32::new(0, 1);
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    let (env, chain, mut pool) = get_test_env_with_chain_and_pool_for(Some(store));
    let num_signed_txs = pool.len();
    for contract_id in ["test1", "test2"] {
        assert_eq!(pool.insert_transaction(ecc(contract_id, 1)), InsertTransactionResult::Success);
    }

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1", "test2"]);

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(signed_txs(&prepared.transactions).count(), num_signed_txs);
    assert_eq!(
        drain_eccs_from_pool(&mut pool),
        vec![("test1".to_string(), vec![1]), ("test2".to_string(), vec![1])]
    );
}

/// An ECC to an account with no contract is dropped, as nobody can run it.
#[test]
fn test_prepare_transactions_drops_eccs_to_accounts_without_contract() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool =
        pool_with_eccs([ecc("test1", 1), ecc("test2", 1), ecc("test2", 2), ecc("nobody", 1)]);

    let (prepared, skipped) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1"]);

    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 1)]);
    assert!(skipped.0.is_empty());
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

/// ECCs to accounts that use a global contract, by hash or by the account that deployed it, are
/// packed like the ones to accounts with a local contract.
#[test]
fn test_prepare_transactions_packs_eccs_to_accounts_with_global_contracts() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test2", 1)]);
    let code = ContractCode::new(ecc_test_contract(), None);
    let deployer: AccountId = "deployer.near".parse().unwrap();
    let by_hash = AccountContract::Global(*code.hash());
    let by_account = AccountContract::GlobalByAccount(deployer.clone());
    // The runs load the code of the global contracts from the state.
    let global_contracts = [
        GlobalContractCodeIdentifier::CodeHash(*code.hash()),
        GlobalContractCodeIdentifier::AccountId(deployer),
    ];
    let give_account_contracts =
        give_account_contracts(vec![("test1", by_hash), ("test2", by_account)]);
    let change_state = move |state_update: &mut TrieUpdate| {
        give_account_contracts(state_update);
        for identifier in global_contracts {
            state_update.set(TrieKey::GlobalContractCode { identifier }, code.code().to_vec());
        }
        state_update.record_contract_deploy(code.clone_for_tests());
    };

    let (prepared, skipped) =
        prepare_eccs_with_context(&env, &chain, &mut pool, HashSet::new(), change_state, |_| {});

    assert_eq!(
        packed_eccs(&prepared.transactions),
        vec![("test1".to_string(), 1), ("test2".to_string(), 1)]
    );
    assert!(skipped.0.is_empty());
}

/// An ECC that went into the previous chunk, which is not in a block yet, is not packed again.
/// The producer takes the next one, and the pool gets back both in the order they came.
#[test]
fn test_prepare_transactions_skips_eccs_of_the_previous_chunk() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test1", 2), ecc("test1", 3)]);
    let in_previous_chunk = ecc("test1", 1).get_hash();

    let (prepared, skipped) =
        prepare_eccs(&env, &chain, &mut pool, HashSet::from([in_previous_chunk]), &["test1"]);

    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 2)]);
    assert_eq!(skipped.0.iter().map(|tx| tx.get_hash()).collect_vec(), vec![in_previous_chunk]);
    assert_eq!(pool.len(), 1);
    // Put back the way the producer does.
    pool.reintroduce_prepared(prepared.transactions, skipped.0);
    assert_eq!(drain_eccs_from_pool(&mut pool), vec![("test1".to_string(), vec![1, 2, 3])]);
}

/// The producer packs an ECC that its contract accepts, and drops one that the contract refuses,
/// without giving up the contract's place in the chunk: the next ECC to the same contract goes in.
#[test]
fn test_prepare_transactions_packs_accepted_eccs_and_drops_refused_ones() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test2", REFUSED_TAG), ecc("test2", 2)]);

    let (prepared, skipped) =
        prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1", "test2"]);

    assert_eq!(
        packed_eccs(&prepared.transactions),
        vec![("test1".to_string(), 1), ("test2".to_string(), 2)]
    );
    assert!(skipped.0.is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::NoMoreTxsInPool);
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

/// A test env whose ECCs have so small a share of a chunk's compute, or of its storage proof,
/// that one ECC takes all of it. `set_shares` gets the config and the amounts the shares are of.
fn get_test_env_with_tiny_ecc_shares(
    set_shares: impl FnOnce(&mut ExternalCallsConfig, Gas, u64),
) -> (TestEnv, Chain, TransactionPool) {
    let mut runtime_config = RuntimeConfig::test();
    let gas_limit = Gas::from_teragas(1000);
    let proof_limit = runtime_config.witness_config.main_storage_proof_size_soft_limit;
    set_shares(&mut runtime_config.external_calls_config, gas_limit, proof_limit);
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    get_test_env_with_chain_and_pool_for(Some(store))
}

/// A share of `amount` that is a unit or more, but less than what any ECC uses.
fn tiny_share(amount: u64) -> Rational32 {
    Rational32::new(1, i32::try_from(amount).unwrap_or(i32::MAX))
}

fn set_tiny_compute_share(config: &mut ExternalCallsConfig, gas_limit: Gas, _: u64) {
    config.chunk_compute_share = tiny_share(gas_limit.as_gas());
}

fn set_tiny_storage_proof_share(config: &mut ExternalCallsConfig, _: Gas, proof_limit: u64) {
    config.chunk_storage_proof_share = tiny_share(proof_limit);
}

/// Changes the shares of ECCs in a config to tiny ones, given the amounts they are shares of.
type SetTinyShares = fn(&mut ExternalCallsConfig, Gas, u64);

fn tiny_share_cases() -> [(&'static str, SetTinyShares); 2] {
    [("compute", set_tiny_compute_share), ("storage proof", set_tiny_storage_proof_share)]
}

/// Once the ECCs of a chunk use up their share of its compute or of its storage proof, the producer
/// takes no more of them, and they stay in the pool. Apply refuses a call past a share, so the
/// producer must not pack one.
#[test]
fn test_prepare_transactions_stops_at_a_full_share() {
    if !eccs_are_packed() {
        return;
    }
    let contracts = ["test1", "test2", "test3"];
    let tests = tiny_share_cases();
    for (name, set_shares) in tests {
        let (env, chain, _) = get_test_env_with_tiny_ecc_shares(set_shares);
        let mut pool = pool_with_eccs(contracts.map(|contract_id| ecc(contract_id, 1)));

        let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &contracts);

        assert_eq!(packed_eccs(&prepared.transactions).len(), 1, "{name}");
        assert_eq!(drain_eccs_from_pool(&mut pool).len(), 2, "{name}");
    }
}

/// An ECC that its contract refuses is not in the chunk, so it uses none of the chunk's shares:
/// an ECC after it is still taken.
#[test]
fn test_prepare_transactions_refused_ecc_does_not_use_the_shares() {
    if !eccs_are_packed() {
        return;
    }
    let contracts = ["test1", "test2", "test3", "test4"];
    let tests = tiny_share_cases();
    for (name, set_shares) in tests {
        let (env, chain, _) = get_test_env_with_tiny_ecc_shares(set_shares);
        let mut pool = pool_with_eccs([
            ecc("test1", REFUSED_TAG),
            ecc("test2", REFUSED_TAG),
            ecc("test3", 1),
            ecc("test4", 1),
        ]);

        let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &contracts);

        let packed = packed_eccs(&prepared.transactions);
        assert_eq!(packed.len(), 1, "{name}");
        assert_eq!(packed[0].1, 1, "{name}");
    }
}

/// What the apply of the parent chunk does for `prepare_eccs_early`.
#[derive(Clone, Copy)]
enum ParentApply {
    /// It finalizes the parent's writes and sends the nodes they add.
    Finalizes,
    /// It sends the nodes only after this long, while the preparation waits for them.
    FinalizesAfter(StdDuration),
    /// It fails before it sends them.
    Fails,
    /// It has not sent them by the time the preparation gives up waiting.
    Stalls,
    /// It has not sent them when the preparation is cancelled, this long after it starts.
    StallsUntilCancelled(StdDuration),
}

/// Prepares ECCs as early preparation does. The state of the preparation holds `parent_writes`,
/// the writes of the parent chunk, as pending changes. The nodes that the writes add to the trie
/// are sent as `parent_apply` says, though they are never stored.
fn prepare_eccs_early(
    env: &TestEnv,
    chain: &Chain,
    pool: &mut TransactionPool,
    parent_apply: ParentApply,
    time_limits: PrepareTransactionsTimeLimits,
    parent_writes: impl Fn(&mut TrieUpdate),
) -> (PreparedTransactions, SkippedTransactions) {
    let cause = StateChangeCause::InitialState;
    let shard_uid = test_shard_uid(env);
    let (sender, receiver) = mpsc::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    let nodes = || {
        let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
        let mut update = TrieUpdate::new(trie.recording_reads_new_recorder());
        parent_writes(&mut update);
        update.commit(cause.clone());
        let trie_changes = update.finalize().unwrap().trie_changes;
        let nodes = NewTrieNodes::from_changes(&trie_changes);
        PostStateNodes { state_root: trie_changes.new_root, nodes }
    };
    match parent_apply {
        ParentApply::Finalizes => sender.send(nodes()).unwrap(),
        ParentApply::FinalizesAfter(delay) => {
            let (nodes, sender) = (nodes(), sender.clone());
            thread::spawn(move || {
                thread::sleep(delay);
                let _ = sender.send(nodes);
            });
        }
        ParentApply::StallsUntilCancelled(delay) => {
            let cancel = Arc::clone(&cancel);
            thread::spawn(move || {
                thread::sleep(delay);
                cancel.store(true, Ordering::Relaxed);
            });
        }
        ParentApply::Fails | ParentApply::Stalls => {}
    }
    // The sender stays until the preparation is over, unless the parent's apply failed.
    let sender = (!matches!(parent_apply, ParentApply::Fails)).then_some(sender);
    let result = prepare_transactions_extra_in_state(
        env,
        chain,
        &mut pool.pool_iterator(),
        HashSet::new(),
        &|_| true,
        &mut PendingTxCheckResult::always_admit(),
        Some(cancel),
        time_limits,
        Some(ParentPostStateNodes { shard_uid, receiver }),
        |state| {
            parent_writes(state);
            state.commit(cause);
        },
        |_| {},
    )
    .unwrap();
    drop(sender);
    result
}

/// Early preparation runs ECCs on the state that the parent chunk leaves, as apply of the next
/// chunk finds it. Reading what the parent wrote from its pending changes costs nothing, while
/// apply reads it from the trie, so an ECC that reads too much of it is refused there. Here a
/// value that is over what an ECC's run may record is refused by the run, and the ECC is dropped.
#[test]
fn test_prepare_transactions_early_drops_an_ecc_that_reads_too_much_of_the_parent_writes() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let config = env.runtime.get_runtime_config(PROTOCOL_VERSION);
    let value = vec![1; config.external_calls_config.max_call_recorded_size * 3 / 2];
    let key = TrieKey::ContractData { account_id: "test1".parse().unwrap(), key: b"k".to_vec() };
    let mut pool = pool_with_eccs([ecc("test1", 1)]);

    let (prepared, skipped) = prepare_eccs_early(
        &env,
        &chain,
        &mut pool,
        ParentApply::Finalizes,
        time_limits(None),
        |state| {
            give_contracts(&["test1"])(state);
            state.set(key.clone(), value.clone());
        },
    );

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert!(skipped.0.is_empty());
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

/// An ECC whose contract the parent chunk deployed, with its account, code and data in the nodes
/// that the parent's apply sent, runs and is packed.
#[test]
fn test_prepare_transactions_early_packs_an_ecc_to_a_contract_the_parent_deployed() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let key = TrieKey::ContractData { account_id: "test1".parse().unwrap(), key: b"k".to_vec() };
    let mut pool = pool_with_eccs([ecc("test1", 1)]);

    let (prepared, skipped) = prepare_eccs_early(
        &env,
        &chain,
        &mut pool,
        ParentApply::Finalizes,
        time_limits(None),
        |state| {
            give_contracts(&["test1"])(state);
            state.set(key.clone(), b"value".to_vec());
        },
    );

    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 1)]);
    assert!(skipped.0.is_empty());
}

/// An ECC waits for the nodes of the parent chunk's apply, and runs once they come, however late.
#[test]
fn test_prepare_transactions_early_waits_for_the_parent_nodes() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1)]);
    let delay = StdDuration::from_millis(150);

    let start = Instant::now();
    let (prepared, skipped) = prepare_eccs_early(
        &env,
        &chain,
        &mut pool,
        ParentApply::FinalizesAfter(delay),
        time_limits(None),
        |state| give_contracts(&["test1"])(state),
    );

    assert!(start.elapsed() >= delay);
    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 1)]);
    assert!(skipped.0.is_empty());
}

/// An ECC that has no nodes of the parent chunk to run on goes back to the pool, and the
/// preparation goes on without ECCs, with the signed transactions it has. That is so if the
/// parent's apply fails, if the nodes do not come before the time for ECCs is up, even when the
/// preparation as a whole has no time limit, and if the preparation is cancelled while it waits.
#[test]
fn test_prepare_transactions_early_puts_back_eccs_without_the_parent_nodes() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let short_ecc_time =
        PrepareTransactionsTimeLimits { total: None, ecc: Duration::milliseconds(300) };
    let cases = [
        (ParentApply::Fails, time_limits(None), PrepareTransactionsLimit::NoMoreTxsInPool),
        (ParentApply::Stalls, short_ecc_time, PrepareTransactionsLimit::Time),
        (
            ParentApply::StallsUntilCancelled(StdDuration::from_millis(50)),
            time_limits(None),
            PrepareTransactionsLimit::Cancelled,
        ),
    ];
    for (parent_apply, limits, limited_by) in cases {
        let mut pool = pool_with_eccs([ecc("test1", 1)]);

        let (prepared, skipped) =
            prepare_eccs_early(&env, &chain, &mut pool, parent_apply, limits, |state| {
                give_contracts(&["test1"])(state)
            });

        assert!(packed_eccs(&prepared.transactions).is_empty());
        assert_eq!(
            skipped.0.iter().map(|tx| tx.get_hash()).collect_vec(),
            [ecc("test1", 1).get_hash()]
        );
        assert_eq!(prepared.limited_by, limited_by);
    }
}

/// The shard of the test environment.
fn test_shard_uid(env: &TestEnv) -> ShardUId {
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    shard_layout.shard_uids().next().unwrap()
}

/// Saves the contract from `ecc_test_contract` for the accounts in the state of the environment,
/// with a value under the key the contract reads. Unlike `give_contracts`, which only changes the
/// state of a preparation, it puts nodes in the trie for the runs of the contract to read.
#[cfg(feature = "test_features")]
fn deploy_ecc_contracts(env: &mut TestEnv, account_ids: &[&str]) {
    let shard_uid = test_shard_uid(env);
    let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
    let mut state_update = TrieUpdate::new(trie);
    let code = ContractCode::new(ecc_test_contract(), None);
    for account_id in account_ids {
        let account_id: AccountId = account_id.parse().unwrap();
        let mut account = get_account(&state_update, &account_id).unwrap().unwrap();
        account.set_contract(AccountContract::Local(*code.hash())).unwrap();
        set_account(&mut state_update, account_id.clone(), &account);
        state_update.set_code(account_id.clone(), &code);
        let key = TrieKey::ContractData { account_id, key: b"k".to_vec() };
        state_update.set(key, b"value".to_vec());
    }
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = env.runtime.tries.store_update();
    env.state_roots[0] = env.runtime.tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();
}

/// A call whose run fails with an error of the runtime fails the preparation, as such an error
/// fails the application of a chunk. It is not a refusal of the contract. Here the contract asks for
/// the total stake of an epoch that does not exist.
#[test]
fn test_prepare_transactions_fails_when_the_run_of_an_ecc_fails() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", FAILING_TAG), ecc("test2", 1)]);

    let result = prepare_transactions_extra_in_state(
        &env,
        &chain,
        &mut pool.pool_iterator(),
        HashSet::new(),
        &|_| true,
        &mut PendingTxCheckResult::always_admit(),
        None,
        time_limits(None),
        None,
        give_contracts(&["test1", "test2"]),
        |context| context.epoch_id = EpochId(CryptoHash::hash_bytes(b"no such epoch")),
    );

    assert_matches!(result, Err(Error::EpochOutOfBounds(_)));
}

/// The size of the state that checking that the accounts hold a contract reads, which is all a
/// preparation reads for ECCs that their contracts refuse.
#[cfg(feature = "test_features")]
fn recorded_size_of_contract_checks(env: &TestEnv, contracts: &[&str]) -> usize {
    let shard_uid = test_shard_uid(env);
    let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
    let state_update = TrieUpdate::new(trie.recording_reads_new_recorder());
    let wrapper = TrieUpdateWitnessSizeWrapper::new(state_update);
    for contract_id in contracts {
        assert!(account_holds_contract(&wrapper, &contract_id.parse().unwrap()).unwrap());
    }
    wrapper.recorded_storage_size()
}

/// The reads of a refused ECC stay out of the storage proof. Otherwise a later ECC would get the
/// nodes the refused one read for free, and fit at the producer but not at apply.
#[cfg(feature = "test_features")]
#[test]
fn test_prepare_transactions_leaves_the_reads_of_refused_eccs_out_of_the_storage_proof() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (mut env, chain, _) = get_test_env_with_chain_and_pool();
    let contracts = ["test1", "test2"];
    deploy_ecc_contracts(&mut env, &contracts);
    let mut refused = pool_with_eccs(contracts.map(|contract_id| ecc(contract_id, REFUSED_TAG)));
    let mut accepted = pool_with_eccs(contracts.map(|contract_id| ecc(contract_id, 1)));

    let (prepared_refused, _) = prepare_eccs(&env, &chain, &mut refused, HashSet::new(), &[]);
    let (prepared_accepted, _) = prepare_eccs(&env, &chain, &mut accepted, HashSet::new(), &[]);

    assert!(packed_eccs(&prepared_refused.transactions).is_empty());
    assert_eq!(packed_eccs(&prepared_accepted.transactions).len(), 2);
    // Nothing but the check of the contracts is in the proof of the refused ECCs. The accepted
    // ones add what their runs read: the code length and the value of the contract.
    let checks = recorded_size_of_contract_checks(&env, &contracts);
    assert_eq!(prepared_refused.recorded_storage_size, checks);
    assert!(prepared_accepted.recorded_storage_size > checks);
}

/// The preparation stops taking ECCs once what it read is over the soft limit of the storage
/// proof, as it stops taking signed transactions.
#[test]
fn test_prepare_transactions_stops_taking_eccs_at_the_storage_proof_limit() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.new_transactions_validation_state_size_soft_limit = 1;
    let store = RuntimeConfigStore::with_one_config(runtime_config);
    let (env, chain, _) = get_test_env_with_chain_and_pool_for(Some(store));
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test2", 1)]);

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1", "test2"]);

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::StorageProofSize);
    assert_eq!(drain_eccs_from_pool(&mut pool).len(), 2);
}

/// Signed transactions go into the chunk before any ECC, whatever the order in the pool.
#[test]
fn test_prepare_transactions_packs_signed_transactions_before_eccs() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, mut pool) = get_test_env_with_chain_and_pool();
    for contract_id in ["test1", "test2", "test3", "test4"] {
        assert_eq!(pool.insert_transaction(ecc(contract_id, 1)), InsertTransactionResult::Success);
    }
    let num_signed_txs = pool.len() - 4;

    let (prepared, _) = prepare_eccs(
        &env,
        &chain,
        &mut pool,
        HashSet::new(),
        &["test1", "test2", "test3", "test4"],
    );

    let kinds = prepared
        .transactions
        .iter()
        .map(|tx| matches!(tx, ValidatedTransactionEnvelope::Signed(_)))
        .collect_vec();
    assert_eq!(kinds.len(), num_signed_txs + 4);
    assert!(kinds[..num_signed_txs].iter().all(|is_signed| *is_signed));
    assert!(kinds[num_signed_txs..].iter().all(|is_signed| !is_signed));
}

/// An ECC runs on the balance its contract has left after the signed transactions that are ahead of
/// it in the chunk.
#[test]
fn test_prepare_transactions_runs_an_ecc_on_the_balance_signed_transactions_left() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let shard_uid = test_shard_uid(&env);
    let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
    let contract_id: AccountId = "test1".parse().unwrap();
    let balance = get_account(&TrieUpdate::new(trie), &contract_id).unwrap().unwrap().amount();
    let config = env.runtime.get_runtime_config(PROTOCOL_VERSION);
    let gas_price = env.runtime.genesis_config.min_gas_price;
    let ecc_fee = ecc_cost(config, ecc("test1", 1).to_ecc(), gas_price).unwrap().burnt_amount;

    // A transfer that leaves the contract one yocto short of the fees of the ECC.
    let signer = InMemorySigner::test_signer(&contract_id);
    let transfer = |deposit: Balance| {
        SignedTransaction::send_money(
            1,
            contract_id.clone(),
            "test2".parse().unwrap(),
            &signer,
            deposit,
            env.head.prev_block_hash,
        )
    };
    let fees = tx_cost(config, &transfer(Balance::ZERO).transaction, gas_price).unwrap().total_cost;
    let left = ecc_fee.checked_sub(Balance::from_yoctonear(1)).unwrap();
    let deposit = balance.checked_sub(fees).unwrap().checked_sub(left).unwrap();

    let mut with_transfer = pool_with_eccs([ecc("test1", 1)]);
    with_transfer.insert_transaction(ValidatedTransaction::new_for_test(transfer(deposit)));
    let mut without_transfer = pool_with_eccs([ecc("test1", 1)]);

    let (prepared, _) = prepare_eccs(&env, &chain, &mut with_transfer, HashSet::new(), &["test1"]);
    assert_eq!(signed_txs(&prepared.transactions).count(), 1);
    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert!(drain_eccs_from_pool(&mut with_transfer).is_empty());

    let (prepared, _) =
        prepare_eccs(&env, &chain, &mut without_transfer, HashSet::new(), &["test1"]);
    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 1)]);
}

/// A signed transaction of the contract that preparation rejects is not in the chunk. The ECC
/// goes on and runs on the account as it is in the state, and is packed.
#[test]
fn test_prepare_transactions_runs_an_ecc_after_a_rejected_signed_transaction_of_its_contract() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let trie = env.runtime.tries.get_trie_for_shard(test_shard_uid(&env), env.state_roots[0]);
    let contract_id: AccountId = "test1".parse().unwrap();
    let balance = get_account(&TrieUpdate::new(trie), &contract_id).unwrap().unwrap().amount();
    // A transfer of more than the contract has.
    let transfer = SignedTransaction::send_money(
        1,
        contract_id.clone(),
        "test2".parse().unwrap(),
        &InMemorySigner::test_signer(&contract_id),
        balance.checked_add(Balance::from_yoctonear(1)).unwrap(),
        env.head.prev_block_hash,
    );
    let mut pool = pool_with_eccs([ecc("test1", 1)]);
    pool.insert_transaction(ValidatedTransaction::new_for_test(transfer));

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1"]);

    assert_eq!(signed_txs(&prepared.transactions).count(), 0);
    assert_eq!(packed_eccs(&prepared.transactions), vec![("test1".to_string(), 1)]);
}

#[cfg(feature = "test_features")]
fn wait_until_ecc_thread_is_free(runtime: &NightshadeRuntime) {
    while runtime.ecc_thread.is_busy() {
        thread::yield_now();
    }
}

/// How long the producer waits for the run of an ECC that is held, which is late whatever the
/// time. It is the time the producer has to reach the run, so it is generous, and it is also how
/// long a test waits.
#[cfg(feature = "test_features")]
const HELD_ECC_WAIT: Duration = Duration::milliseconds(500);

/// An ECC that is not done by the deadline is left out of the chunk, and runs on in the
/// background. If it had the whole time for ECCs to itself it is dropped, as it would not finish
/// next time either. Until it is done the next chunks take no ECCs, and then they take them again.
#[cfg(feature = "test_features")]
#[test]
fn test_prepare_transactions_leaves_out_a_late_ecc_and_takes_no_others_while_it_runs() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let held = ecc("test1", 1);
    let held_hash = held.get_hash();
    env.runtime.test_held_eccs.hold(&held_hash);
    let contracts = ["test1", "test2"];
    let prepare = |pool: &mut TransactionPool, limits| {
        let change_state = give_contracts(&contracts);
        prepare_eccs_with_limits(&env, &chain, pool, HashSet::new(), limits, change_state, |_| {})
    };
    // Short, only for the runs that are held. A run that is not must not be late.
    let held_limits = PrepareTransactionsTimeLimits { total: None, ecc: HELD_ECC_WAIT };

    let mut pool = pool_with_eccs([held]);
    let (prepared, skipped) = prepare(&mut pool, held_limits);
    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::Time);
    // It had the whole time to itself, so it is not put back.
    assert!(skipped.0.is_empty());
    assert_eq!(pool.len(), 0);
    assert!(env.runtime.ecc_thread.is_busy());

    // The run is going on, and no other ECC is taken, nor lost.
    let mut pool = pool_with_eccs([ecc("test2", 1)]);
    let (prepared, skipped) = prepare(&mut pool, held_limits);
    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert!(skipped.0.is_empty());
    assert_eq!(drain_eccs_from_pool(&mut pool), vec![("test2".to_string(), vec![1])]);

    // Once it is done, ECCs are taken again.
    env.runtime.test_held_eccs.release(&held_hash);
    wait_until_ecc_thread_is_free(&env.runtime);
    let mut pool = pool_with_eccs([ecc("test2", 1)]);
    let (prepared, _) = prepare(&mut pool, time_limits(None));
    assert_eq!(packed_eccs(&prepared.transactions), vec![("test2".to_string(), 1)]);
}

/// A late ECC that started late, because the signed transactions left less time than ECCs may
/// take, is put back: it may finish in time when it has more.
#[cfg(feature = "test_features")]
#[test]
fn test_prepare_transactions_puts_back_a_late_ecc_that_started_late() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let held = ecc("test1", 1);
    let held_hash = held.get_hash();
    env.runtime.test_held_eccs.hold(&held_hash);
    // The time of the whole preparation ends before the time for ECCs does.
    let limits =
        PrepareTransactionsTimeLimits { total: Some(HELD_ECC_WAIT), ecc: Duration::seconds(60) };
    let mut pool = pool_with_eccs([held]);

    let (prepared, skipped) = prepare_eccs_with_limits(
        &env,
        &chain,
        &mut pool,
        HashSet::new(),
        limits,
        give_contracts(&["test1"]),
        |_| {},
    );

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::Time);
    assert_eq!(skipped.0.iter().map(|tx| tx.get_hash()).collect_vec(), vec![held_hash]);
    assert_eq!(pool.len(), 0);

    env.runtime.test_held_eccs.release(&held_hash);
    wait_until_ecc_thread_is_free(&env.runtime);
}

/// `prepare_transactions` hands back what it skips, like `prepare_transactions_extra`: the
/// producer needs it to put back an ECC that was late, or that did not get to run, since it is
/// the one that takes the ECC out of the pool. Without a list of skipped ECCs it would lose them.
#[cfg(feature = "test_features")]
#[test]
fn test_prepare_transactions_returns_the_late_ecc_that_started_late() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (mut env, chain, _) = get_test_env_with_chain_and_pool();
    deploy_ecc_contracts(&mut env, &["test1"]);
    let held = ecc("test1", 1);
    let held_hash = held.get_hash();
    env.runtime.test_held_eccs.hold(&held_hash);
    let limits =
        PrepareTransactionsTimeLimits { total: Some(HELD_ECC_WAIT), ecc: Duration::seconds(60) };
    let mut pool = pool_with_eccs([held]);
    let storage_config = RuntimeStorageConfig {
        state_root: env.state_roots[0],
        use_flat_storage: true,
        source: StorageDataSource::Db,
        state_patch: Default::default(),
    };

    let (prepared, skipped) = prepare_transactions_with_limits(
        &env,
        &chain,
        &mut pool.pool_iterator(),
        storage_config,
        limits,
    )
    .unwrap();

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(skipped.0.iter().map(|tx| tx.get_hash()).collect_vec(), vec![held_hash]);
    assert_eq!(pool.len(), 0);

    env.runtime.test_held_eccs.release(&held_hash);
    wait_until_ecc_thread_is_free(&env.runtime);
}

/// With no time for ECCs, none are taken, and they stay in the pool.
#[test]
fn test_prepare_transactions_takes_no_eccs_without_time_for_them() {
    if !eccs_are_packed() {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let (env, chain, mut pool) = get_test_env_with_chain_and_pool();
    let num_signed_txs = pool.len();
    assert_eq!(pool.insert_transaction(ecc("test1", 1)), InsertTransactionResult::Success);
    let limits = PrepareTransactionsTimeLimits { total: None, ecc: Duration::ZERO };

    let (prepared, _) = prepare_eccs_with_limits(
        &env,
        &chain,
        &mut pool,
        HashSet::new(),
        limits,
        give_contracts(&["test1"]),
        |_| {},
    );

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(signed_txs(&prepared.transactions).count(), num_signed_txs);
    assert_eq!(drain_eccs_from_pool(&mut pool), vec![("test1".to_string(), vec![1])]);
}

/// ECCs get their share of the time limit, or of the default limit if there is none, and a share
/// of nothing or of the whole is kept within those bounds.
#[test]
fn test_time_limits_of_eccs() {
    let ms = Duration::milliseconds;
    let limits = |total, share| PrepareTransactionsTimeLimits::new(total, share).ecc;
    assert_eq!(limits(Some(ms(200)), Rational32::new(1, 2)), ms(100));
    assert_eq!(limits(Some(ms(300)), Rational32::new(1, 3)), ms(100));
    assert_eq!(limits(Some(ms(200)), Rational32::new(0, 1)), Duration::ZERO);
    assert_eq!(limits(Some(ms(200)), Rational32::new(-1, 2)), Duration::ZERO);
    assert_eq!(limits(Some(ms(200)), Rational32::new(1, 1)), ms(200));
    assert_eq!(limits(Some(ms(200)), Rational32::new(3, 2)), ms(200));
    assert_eq!(limits(None, Rational32::new(1, 2)), ms(100));
    assert_eq!(limits(None, Rational32::new(0, 1)), Duration::ZERO);
    assert_eq!(PrepareTransactionsTimeLimits::new(None, Rational32::new(1, 2)).total, None);
}

/// TODO(spice): the producer does not pack ECCs under Spice yet, so they all stay in the pool.
#[test]
fn test_prepare_transactions_does_not_pack_eccs_under_spice() {
    if !ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: Spice is not enabled in this build");
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1), ecc("test2", 1)]);

    let (prepared, _) = prepare_eccs(&env, &chain, &mut pool, HashSet::new(), &["test1", "test2"]);

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert_eq!(pool.len(), 2);
}

/// ECCs of a contract in a congested shard are dropped, as signed transactions to it are.
#[test]
fn test_prepare_transactions_drops_eccs_to_a_congested_shard() {
    if !eccs_are_packed() {
        return;
    }
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let mut pool = pool_with_eccs([ecc("test1", 1)]);
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let full_congestion = CongestionInfo::V1(CongestionInfoV1 {
        delayed_receipts_gas: u128::from(u64::MAX),
        buffered_receipts_gas: 0,
        receipt_bytes: 0,
        allowed_shard: 0,
    });
    let congested = BlockCongestionInfo::new(
        [(shard_id, ExtendedCongestionInfo::new(full_congestion, 0))].into(),
    );

    let (prepared, skipped) = prepare_eccs_with_context(
        &env,
        &chain,
        &mut pool,
        HashSet::new(),
        give_contracts(&["test1"]),
        |context| context.congestion_info = congested,
    );

    assert!(packed_eccs(&prepared.transactions).is_empty());
    assert!(skipped.0.is_empty());
    assert!(drain_eccs_from_pool(&mut pool).is_empty());
}

#[test]
fn test_prepare_transactions_duplicate_nonces() {
    let (env, chain, mut transaction_pool) = get_test_env_with_chain_and_pool();

    // Insert a transaction with a duplicate (public key, nonce) pair into the pool.
    let mut iter = transaction_pool.pool_iterator();
    let PooledGroup::Signed(group) = iter.next().unwrap() else {
        panic!("expected a group of signed transactions")
    };
    let first_tx = group.peek_next().unwrap();
    let duplicate_nonce_tx = SignedTransaction::send_money(
        first_tx.nonce().nonce(),
        first_tx.signer_id().clone(),
        first_tx.receiver_id().clone(),
        &InMemorySigner::test_signer(&first_tx.signer_id()),
        Balance::from_yoctonear(9999),
        *first_tx.to_tx().block_hash(),
    );
    drop(iter);

    let storage_config = RuntimeStorageConfig {
        state_root: env.state_roots[0],
        use_flat_storage: true,
        source: StorageDataSource::Db,
        state_patch: Default::default(),
    };
    let insert_result =
        transaction_pool.insert_transaction(ValidatedTransaction::new_for_test(duplicate_nonce_tx));
    assert_eq!(insert_result, InsertTransactionResult::Success);
    let mut iter = transaction_pool.pool_iterator();
    let txs = prepare_transactions(&env, &chain, &mut iter, storage_config).unwrap();

    // Collect (public key, nonce) pairs to check for duplicates.
    let mut pk_nonce_set = HashSet::new();
    for tx in signed_txs(&txs.transactions) {
        let pk_nonce = (tx.public_key(), tx.nonce().nonce());
        assert!(
            pk_nonce_set.insert(pk_nonce),
            "Duplicate transaction with public key {:?} and nonce {} found in prepared transactions",
            tx.public_key(),
            tx.nonce().nonce()
        );
    }
}

/// When the same account has transactions under two different public keys,
/// the signer cache must share account state (balance) across keys.
/// Otherwise each key's group sees the full balance independently.
#[test]
fn test_prepare_transactions_shared_balance_across_keys() {
    let (mut env, chain, _) = get_test_env_with_chain_and_pool();

    let account_id: AccountId = "test1".parse().unwrap();
    let signer1 = InMemorySigner::test_signer(&account_id);
    let signer2 =
        InMemorySigner::from_seed(account_id.clone(), near_crypto::KeyType::ED25519, "second_key");
    let block_hash = env.head.prev_block_hash;

    // Add a second full-access key for the same account directly in the trie.
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&block_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let shard_uid =
        shard_id_to_uid(env.epoch_manager.as_ref(), shard_id, &env.head.epoch_id).unwrap();
    {
        let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
        let mut state_update = TrieUpdate::new(trie);
        near_store::set_access_key(
            &mut state_update,
            account_id.clone(),
            signer2.public_key(),
            &AccessKey::full_access(),
        );
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = env.runtime.tries.store_update();
        env.state_roots[0] =
            env.runtime.tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }

    // Each transfer exceeds half the available balance, so only one can succeed
    // if the balance is shared. With independent balances both would succeed.
    let available = TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap();
    let transfer_amount =
        available.checked_div(2).unwrap().checked_add(Balance::from_near(1)).unwrap();
    let receiver: AccountId = "test2".parse().unwrap();
    let tx1 = SignedTransaction::send_money(
        1,
        account_id.clone(),
        receiver.clone(),
        &signer1,
        transfer_amount,
        block_hash,
    );
    let tx2 = SignedTransaction::send_money(
        1,
        account_id,
        receiver,
        &signer2,
        transfer_amount,
        block_hash,
    );

    const TEST_SEED: RngSeed = [3; 32];
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    pool.insert_transaction(ValidatedTransaction::new_for_test(tx1));
    pool.insert_transaction(ValidatedTransaction::new_for_test(tx2));

    let storage_config = RuntimeStorageConfig {
        state_root: env.state_roots[0],
        use_flat_storage: true,
        source: StorageDataSource::Db,
        state_patch: Default::default(),
    };
    let mut iter = pool.pool_iterator();
    let result = prepare_transactions(&env, &chain, &mut iter, storage_config).unwrap();

    assert_eq!(
        result.transactions.len(),
        1,
        "expected only one transfer to be included when both share the same account balance"
    );
}

/// The chunk producer resolves signers through `SignerOverlay`, which reports a
/// missing access key for *every* key on an uninitialized account. Only one that
/// really is a self-signed state init may be charged against the account's bootstrap nonce.
#[test]
fn test_prepare_transactions_rejects_unauthorized_bootstrap() {
    init_test_logger();
    let (mut env, chain, _) = get_test_env_with_chain_and_pool();

    // The account id commits to `committed` and to nothing else, so `attacker`
    // is exactly the key the id is not a promise about.
    let unused: AccountId = "unused.near".parse().unwrap();
    let committed = InMemorySigner::from_seed(unused.clone(), KeyType::ED25519, "committed");
    let attacker = InMemorySigner::from_seed(unused, KeyType::ED25519, "attacker");
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(committed.public_key())]),
    });
    let raw_state_init = state_init.to_raw();
    let account_id = derive_universal_account_id(&raw_state_init);

    // Stands in for `initial_nonce_value(creation_height, ..)`. The account is
    // written straight into the trie, so only its relation to the transaction
    // nonce matters, and it has to stay under the height-derived upper bound.
    const BOOTSTRAP_NONCE: Nonce = 1_000;

    let block_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&block_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let shard_uid =
        shard_id_to_uid(env.epoch_manager.as_ref(), shard_id, &env.head.epoch_id).unwrap();
    {
        let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
        let mut state_update = TrieUpdate::new(trie);
        set_account(
            &mut state_update,
            account_id.clone(),
            &Account::new_uninitialized(Balance::from_near(10), 100, BOOTSTRAP_NONCE),
        );
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = env.runtime.tries.store_update();
        env.state_roots[0] =
            env.runtime.tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }

    let prepare = |tx: SignedTransaction| {
        let storage_config = RuntimeStorageConfig {
            state_root: env.state_roots[0],
            use_flat_storage: true,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
        };
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
        let mut iter = pool.pool_iterator();
        prepare_transactions(&env, &chain, &mut iter, storage_config).unwrap()
    };

    // Signed by a key the account id does not commit to, and carrying no state
    // init at all. All the overlay can see is that the key is missing.
    let unauthorized = SignedTransaction::from_actions(
        BOOTSTRAP_NONCE + 1,
        account_id.clone(),
        account_id.clone(),
        &attacker,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
        block_hash,
    );
    assert!(
        prepare(unauthorized).transactions.is_empty(),
        "a transaction that is not a self-signed state init must not take the bootstrap path"
    );

    // The genuine article still goes through, so the check does not over-reject.
    let authorized = SignedTransaction::from_actions(
        BOOTSTRAP_NONCE + 1,
        account_id.clone(),
        account_id,
        &committed,
        vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: raw_state_init,
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    assert_eq!(
        prepare(authorized).transactions.len(),
        1,
        "a genuine self-signed state init must still be included"
    );
}

/// A self-signed state init is charged with strict nonce semantics whatever it
/// declared, so a gapped one has to be *held* in the pool for a later chunk
/// rather than popped and discarded. V0 cannot declare a mode at all, and that
/// is the shape which would otherwise slip past the gap check entirely.
///
/// Nothing else on the same uninitialized account gets that treatment: the
/// account's nonce cannot authorize any other transaction, so holding one would
/// only keep junk around until its TTL expired.
#[test]
fn test_prepare_transactions_gap_check_holds_only_bootstrap() {
    init_test_logger();
    let (mut env, chain, _) = get_test_env_with_chain_and_pool();

    let unused: AccountId = "unused.near".parse().unwrap();
    let committed = InMemorySigner::from_seed(unused.clone(), KeyType::ED25519, "committed");
    let attacker = InMemorySigner::from_seed(unused, KeyType::ED25519, "attacker");
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(committed.public_key())]),
    });
    let raw_state_init = state_init.to_raw();
    let account_id = derive_universal_account_id(&raw_state_init);

    // As in the test above: stands in for `initial_nonce_value(creation_height, ..)`.
    const BOOTSTRAP_NONCE: Nonce = 1_000;

    let block_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&block_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let shard_uid =
        shard_id_to_uid(env.epoch_manager.as_ref(), shard_id, &env.head.epoch_id).unwrap();
    {
        let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
        let mut state_update = TrieUpdate::new(trie);
        set_account(
            &mut state_update,
            account_id.clone(),
            &Account::new_uninitialized(Balance::from_near(10), 100, BOOTSTRAP_NONCE),
        );
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = env.runtime.tries.store_update();
        env.state_roots[0] =
            env.runtime.tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }

    // The one admissible bootstrap nonce is `BOOTSTRAP_NONCE + 1`, so this
    // leaves a gap. The pool iterator returns whatever it did not pop, which is
    // how a held transaction is told apart from a discarded one.
    let gapped_nonce = BOOTSTRAP_NONCE + 2;
    let prepare = |tx: SignedTransaction| {
        let storage_config = RuntimeStorageConfig {
            state_root: env.state_roots[0],
            use_flat_storage: true,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
        };
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
        let included = {
            let mut iter = pool.pool_iterator();
            prepare_transactions(&env, &chain, &mut iter, storage_config)
                .unwrap()
                .transactions
                .len()
        };
        (included, pool.len())
    };

    let bootstrap = SignedTransaction::from_actions(
        gapped_nonce,
        account_id.clone(),
        account_id.clone(),
        &committed,
        vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: raw_state_init,
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    assert_eq!(
        prepare(bootstrap),
        (0, 1),
        "a gapped V0 self-signed state init must be held in the pool, not discarded"
    );

    // Strict, so it reaches the gap check, but the account's nonce is not its to
    // be measured against. It must be popped and left for full validation.
    let not_a_bootstrap = SignedTransaction::from_actions_v1_strict(
        TransactionNonce::from_nonce(gapped_nonce),
        account_id.clone(),
        account_id,
        &attacker,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
        block_hash,
    );
    assert_eq!(
        prepare(not_a_bootstrap),
        (0, 0),
        "a transaction that is not a bootstrap must not be held on the account's nonce"
    );
}

/// One account's flood of rejected transactions must not use up the whole
/// selection budget. Rejected transactions advance no gas/size budget, so the
/// `time_limit` bounds them, and the per-visit group cap
/// (`MAX_TXS_PER_GROUP_PER_VISIT`) lets the loop reach that check and give
/// other accounts a turn.
///
/// With one small time limit, checks that:
///  - preparation stops on the time limit (within a small cap), not by draining the flood,
///  - transactions from other accounts are still included (fairness),
///  - the flood is drained more than one visit's worth, but not fully.
#[test]
fn test_prepare_transactions_flood_respects_time_limit_and_fairness() {
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let block_hash = env.head.prev_block_hash;

    let flooder: AccountId = "test1".parse().unwrap();
    let flooder_signer = InMemorySigner::test_signer(&flooder);

    // Each transfer exceeds half the available balance, so only the lowest-nonce
    // transaction is funded and every following one is rejected on balance.
    let available = TESTING_INIT_BALANCE.checked_sub(TESTING_INIT_STAKE).unwrap();
    let big_transfer =
        available.checked_div(2).unwrap().checked_add(Balance::from_near(1)).unwrap();

    // Empty signatures, so the pile is cheap to build.
    let flood_tx = |nonce: u64| -> ValidatedTransaction {
        let tx = Transaction::V0(TransactionV0 {
            signer_id: flooder.clone(),
            public_key: flooder_signer.public_key(),
            nonce,
            receiver_id: flooder.clone(),
            block_hash,
            actions: vec![Action::Transfer(TransferAction { deposit: big_transfer })],
        });
        ValidatedTransaction::new_for_test(SignedTransaction::new(
            Signature::empty(KeyType::ED25519),
            tx,
        ))
    };

    let build_flood_pool = |size: u64| -> TransactionPool {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        for nonce in 1..=size {
            pool.insert_transaction(flood_tx(nonce));
        }
        pool
    };

    let cap = MAX_TXS_PER_GROUP_PER_VISIT as u64;

    // Measure the ~cost of rejecting one transaction. Sizing the load/timeouts
    // relative to this keeps the test machine-independent.
    let warmup_size = 4 * cap;
    let mut warmup_pool = build_flood_pool(warmup_size);
    let warmup_start = std::time::Instant::now();
    {
        let mut warmup_iter = warmup_pool.pool_iterator();
        prepare_transactions_extra_with_time_limit(
            &env,
            &chain,
            &mut warmup_iter,
            HashSet::new(),
            &|_| true,
            &mut PendingTxCheckResult::always_admit(),
            None,
            None, // no time limit => drains the whole warmup flood
        )
        .unwrap();
    }
    let time_to_reject_tx = warmup_start.elapsed() / warmup_size as u32;

    // Real flood: far more than the ~8 visits the time budget below allows, so
    // it can't be drained within the deadline.
    let flood_size = 64 * cap;
    let mut pool = build_flood_pool(flood_size);

    // A few valid transactions from other accounts submitted alongside the
    // flood. Each transfers a negligible amount, so all of them are funded.
    let honest_accounts = ["test2", "test3", "test4"];
    const HONEST_TXS_PER_ACCOUNT: u64 = 2;
    let mut honest_count = 0u64;
    for name in honest_accounts {
        let account: AccountId = name.parse().unwrap();
        let signer = InMemorySigner::test_signer(&account);
        for nonce in 1..=HONEST_TXS_PER_ACCOUNT {
            let tx = SignedTransaction::send_money(
                nonce,
                account.clone(),
                flooder.clone(),
                &signer,
                Balance::from_yoctonear(1),
                block_hash,
            );
            pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
            honest_count += 1;
        }
    }

    // Time budget of ~1/8 needed to drain the flood (but still ~8x of single visit full of rejections)
    let time_limit =
        Some(Duration::nanoseconds(time_to_reject_tx.as_nanos() as i64 * 8 * cap as i64));

    let result = {
        let mut pool_iter = pool.pool_iterator();
        prepare_transactions_extra_with_time_limit(
            &env,
            &chain,
            &mut pool_iter,
            HashSet::new(),
            &|_| true,
            &mut PendingTxCheckResult::always_admit(),
            None,
            time_limit,
        )
        .unwrap()
        .0
    };

    // Property 1: preparation stopped on the time limit, not because the flood was exhausted.
    assert!(
        matches!(result.limited_by, PrepareTransactionsLimit::Time),
        "expected to stop on the time limit, stopped on {:?}",
        result.limited_by,
    );

    // Property 2 (fairness): transactions from the other accounts are
    // included despite the flood, plus exactly the one funded flood tx.
    let included_honest =
        signed_txs(&result.transactions).filter(|tx| tx.signer_id() != &flooder).count() as u64;
    assert_eq!(included_honest, honest_count, "all honest transactions should be included");
    let included_flood =
        signed_txs(&result.transactions).filter(|tx| tx.signer_id() == &flooder).count();
    assert_eq!(included_flood, 1, "exactly one flood transaction is funded");

    // Property 3 (a bit into the implementation): check the count pulled was more than one visit's worth.
    let flood_examined = flood_size - pool.len() as u64;
    assert!(
        flood_examined > cap,
        "expected to pull more than one visit from the flood, pulled {flood_examined}",
    );
    assert!(
        flood_examined < flood_size,
        "expected the flood to not be fully drained, pulled {flood_examined}",
    );
}

/// The time limit must also bound a flood that fits inside a *single* group
/// visit. `test_prepare_transactions_flood_respects_time_limit_and_fairness`
/// covers the case where the flood exceeds `MAX_TXS_PER_GROUP_PER_VISIT`, so the
/// loop returns to the outer check between visits. It cannot cover this one:
/// here the whole flood fits in one visit, so the outer check is never reached
/// again, and only a check inside the per-group loop can stop it.
///
/// The cost per peeked transaction is what makes the difference. These carry a
/// `UniversalStateInit` payload at the transaction size limit, which
/// `is_state_init_bootstrap` hashes and decodes and `tx_cost` decodes twice more,
/// all before the transaction is rejected. A rejected transaction burns no gas
/// and never advances `total_size`, so the gas and size budgets stay put.
#[test]
fn test_prepare_transactions_respects_time_limit_within_one_group_visit() {
    /// Mainnet `max_transaction_size` (`core/parameters/res/runtime_configs/69.yaml:3`).
    const MAX_TX_SIZE: usize = 1_572_864;
    /// Default `transaction_pool_size_limit` (`core/chain-configs/src/client_config.rs`).
    const POOL_LIMIT: usize = 100_000_000;
    const BOOTSTRAP_NONCE: Nonce = 1_000;

    let (mut env, chain, _) = get_test_env_with_chain_and_pool();
    let block_hash = env.head.prev_block_hash;

    let committed =
        InMemorySigner::from_seed("unused.near".parse().unwrap(), KeyType::ED25519, "committed");
    // One entry holding one large value, rather than many small entries: this
    // shape stays within every per-entry and per-key limit, so what the test
    // measures cannot be taken away by a cap on the payload's structure.
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::from([(b"k".to_vec(), vec![0u8; MAX_TX_SIZE - 1_000])]),
        access_keys: BTreeSet::from([PublicKeyHandle::from(committed.public_key())]),
    });
    let raw_state_init = state_init.to_raw();
    let payload_len = raw_state_init.0.len();
    assert!(payload_len < MAX_TX_SIZE, "payload {payload_len} exceeds the transaction size limit");
    let account_id = derive_universal_account_id(&raw_state_init);

    // The account exists but is uninitialized and far too poor to pay for a state
    // init this large, so every transaction below is rejected on balance.
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&block_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let shard_uid =
        shard_id_to_uid(env.epoch_manager.as_ref(), shard_id, &env.head.epoch_id).unwrap();
    {
        let trie = env.runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[0]);
        let mut state_update = TrieUpdate::new(trie);
        set_account(
            &mut state_update,
            account_id.clone(),
            &Account::new_uninitialized(Balance::from_yoctonear(1), 100, BOOTSTRAP_NONCE),
        );
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = env.runtime.tries.store_update();
        env.state_roots[0] =
            env.runtime.tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }

    // All at the one admissible bootstrap nonce, so the nonce gap check lets every
    // one of them through to the expensive work. They differ only in the action
    // deposit, which is enough for distinct hashes and so distinct pool entries.
    let build_pool = |size: usize| -> TransactionPool {
        let mut pool = TransactionPool::new(TEST_SEED, None, "");
        for i in 0..size {
            let tx = Transaction::V0(TransactionV0 {
                signer_id: account_id.clone(),
                public_key: committed.public_key(),
                nonce: BOOTSTRAP_NONCE + 1,
                receiver_id: account_id.clone(),
                block_hash,
                actions: vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                    state_init: raw_state_init.clone(),
                    deposit: Balance::from_yoctonear(i as u128),
                }))],
            });
            let signed = SignedTransaction::new(Signature::empty(KeyType::ED25519), tx);
            assert_eq!(
                pool.insert_transaction(ValidatedTransaction::new_for_test(signed)),
                InsertTransactionResult::Success,
            );
        }
        pool
    };

    let run = |pool: &mut TransactionPool, time_limit| -> (PreparedTransactions, Duration) {
        let start = std::time::Instant::now();
        let mut iter = pool.pool_iterator();
        let prepared = prepare_transactions_extra_with_time_limit(
            &env,
            &chain,
            &mut iter,
            HashSet::new(),
            &|_| true,
            &mut PendingTxCheckResult::always_admit(),
            None,
            time_limit,
        )
        .unwrap()
        .0;
        (prepared, Duration::nanoseconds(start.elapsed().as_nanos() as i64))
    };

    // Calibrate the budget against this machine rather than a fixed duration:
    // time an unbudgeted run over a small pool, and give the real run twice
    // that. The doubling leaves room above the per-call setup the warmup also
    // paid for, so the outer check cannot fire before the group is entered,
    // while staying far below what draining the whole flood would take.
    const WARMUP: usize = 8;
    let (warmup, warmup_elapsed) = run(&mut build_pool(WARMUP), None);
    assert!(warmup.transactions.is_empty(), "the flood must not be includable");
    let time_limit = warmup_elapsed * 2;

    // A shard pool's worth of them, all in the one (account, key) group, and
    // fewer than the per-visit cap so the loop never returns to the outer check.
    let flood_size = POOL_LIMIT / payload_len;
    assert!(
        flood_size < MAX_TXS_PER_GROUP_PER_VISIT,
        "the pool byte limit, not the per-visit count cap, has to be what bounds this group",
    );
    let mut pool = build_pool(flood_size);

    let (result, _) = run(&mut pool, Some(time_limit));
    assert!(result.transactions.is_empty(), "the flood must not be includable");

    // Nothing below measures wall-clock time, so a slow or heavily loaded worker
    // cannot fail this. The counts alone say the inner check is what stopped it:
    // there is a single group, so had the loop only checked the clock between
    // groups it would have drained the group and reported `NoMoreTxsInPool`.
    assert!(
        matches!(result.limited_by, PrepareTransactionsLimit::Time),
        "expected to stop on the time limit, stopped on {:?}",
        result.limited_by,
    );
    let examined = flood_size - pool.len();
    assert!(
        examined > 0,
        "expected the group to be entered, so that the stop came from the inner check \
         rather than from the outer one before any transaction was peeked",
    );
    assert!(
        examined < flood_size,
        "expected to stop before draining the group, examined all {flood_size}",
    );
}

/// Check that transactions validation fails if provided empty storage proof.
#[test]
fn test_prepare_transactions_empty_storage_proof() {
    // First prepare transactions using proper db and check all transactions are
    // included in the result.
    let db_storage_source = StorageDataSource::Db;
    let (transaction_count, prepared_transactions) =
        test_prepare_transactions_helper(db_storage_source)
            .expect("prepare transactions should succeed with proper db");
    assert_ne!(transaction_count, 0);
    assert_eq!(prepared_transactions.transactions.len(), transaction_count);

    // Second prepare transactions using empty storage proof and check that
    // prepare_transactions fails.
    let empty_storage_source =
        StorageDataSource::Recorded(PartialStorage { nodes: PartialState::default() });
    test_prepare_transactions_helper(empty_storage_source)
        .expect_err("prepare transactions should fail with empty storage proof");
}

// Helper function to test prepare_transactions with different storage sources.
fn test_prepare_transactions_helper(
    storage_source: StorageDataSource,
) -> Result<(usize, PreparedTransactions), Error> {
    let (env, chain, mut transaction_pool) = get_test_env_with_chain_and_pool();
    let transactions_count = transaction_pool.len();

    let storage_config = RuntimeStorageConfig {
        state_root: env.state_roots[0],
        use_flat_storage: true,
        source: storage_source,
        state_patch: Default::default(),
    };

    let mut transaction_groups = PoolIteratorWrapper::new(&mut transaction_pool);
    let prepared_transactions =
        match prepare_transactions(&env, &chain, &mut transaction_groups, storage_config) {
            Ok(prepared_transactions) => prepared_transactions,
            Err(err) => {
                return Err(err);
            }
        };
    Ok((transactions_count, prepared_transactions))
}

#[test]
fn test_prepare_transactions_extra() {
    let (env, chain, mut transaction_pool) = get_test_env_with_chain_and_pool();
    let validate_tx_ttl = &|_: &SignedTransaction| true;

    // First run the preparation without any extra arguments
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut transaction_pool),
        HashSet::new(),
        validate_tx_ttl,
        &mut PendingTxCheckResult::always_admit(),
        None,
    )
    .unwrap();
    assert_eq!(prepared.transactions.len(), 4 * 3); // 4 validators, a transaction between each pair.
    assert!(skipped.0.is_empty());

    // Keep a list of all transactions, will be useful for checking things
    let all_txs = prepared.transactions;

    // Reintroduce transactions
    for tx in &all_txs {
        transaction_pool.insert_transaction(tx.clone());
    }

    // Run with `cancel` set to Some(false)
    // Should have no effect.
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut transaction_pool),
        HashSet::new(),
        validate_tx_ttl,
        &mut PendingTxCheckResult::always_admit(),
        Some(Arc::new(AtomicBool::new(false))),
    )
    .unwrap();
    assert_eq!(prepared.transactions.len(), all_txs.len());
    assert!(skipped.0.is_empty());

    for tx in prepared.transactions {
        transaction_pool.insert_transaction(tx);
    }

    // Run with `cancel` set to Some(true)
    // Should stop immediately.
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut transaction_pool),
        HashSet::new(),
        validate_tx_ttl,
        &mut PendingTxCheckResult::always_admit(),
        Some(Arc::new(AtomicBool::new(true))),
    )
    .unwrap();
    assert!(prepared.transactions.is_empty());
    assert!(skipped.0.is_empty());

    // Skip the first 4 transactions
    let skip_num = 4;
    let skip_tx_hashes: HashSet<CryptoHash> =
        all_txs.iter().map(|tx| tx.get_hash()).take(skip_num).collect();
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut transaction_pool),
        skip_tx_hashes.clone(),
        validate_tx_ttl,
        &mut PendingTxCheckResult::always_admit(),
        None,
    )
    .unwrap();
    assert_eq!(prepared.transactions.len(), all_txs.len() - skip_num);
    assert_eq!(skipped.0.len(), skip_num);

    for tx in prepared.transactions {
        assert!(!skip_tx_hashes.contains(&tx.get_hash()));
    }
    let skipped_hashes: HashSet<CryptoHash> = skipped.0.iter().map(|tx| tx.get_hash()).collect();
    assert_eq!(skipped_hashes, skip_tx_hashes);

    assert_eq!(transaction_pool.len(), 0);
}

/// When check_pending returns Skip for a transaction, it should appear in
/// skipped_transactions (for reintroduction to pool), not in prepared.
#[test]
fn test_prepare_transactions_pending_skip() {
    let num_rounds = NUM_TEST_SIGNERS - 1;
    let total_txs = NUM_TEST_SIGNERS * num_rounds;
    let (env, chain, mut transaction_pool) = get_test_env_with_chain_and_pool();

    // Skip every other transaction.
    let mut call_count = 0usize;
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut transaction_pool),
        HashSet::new(),
        &|_| true,
        &mut |_| {
            call_count += 1;
            if call_count.is_multiple_of(2) {
                PendingTxCheckResult::Skip
            } else {
                PendingTxCheckResult::Admit(PendingConstraints::default())
            }
        },
        None,
    )
    .unwrap();

    // No transactions should be lost: all are either prepared or skipped.
    assert_eq!(prepared.transactions.len() + skipped.0.len(), total_txs);
    assert_eq!(prepared.transactions.len(), total_txs / 2);
    assert_eq!(skipped.0.len(), total_txs / 2);
}

/// When check_pending returns Admit with paid_from_balance, the available
/// balance for validation is reduced. With a large enough paid_from_balance,
/// subsequent transactions should fail balance validation.
#[test]
fn test_prepare_transactions_pending_balance_constraint() {
    let (env, chain, _) = get_test_env_with_chain_and_pool();

    // Create a pool with a small transfer from test1.
    let signer = InMemorySigner::test_signer(&"test1".parse::<AccountId>().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        signer.get_account_id(),
        "test2".parse().unwrap(),
        &signer,
        Balance::from_yoctonear(1),
        env.head.prev_block_hash,
    );
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    pool.insert_transaction(ValidatedTransaction::new_for_test(tx));

    // Without pending constraints, the transfer should succeed.
    let (prepared, _) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut pool),
        HashSet::new(),
        &|_| true,
        &mut PendingTxCheckResult::always_admit(),
        None,
    )
    .unwrap();
    assert_eq!(prepared.transactions.len(), 1);

    // Reinsert and try again with paid_from_balance consuming all balance.
    pool.insert_transaction(prepared.transactions[0].clone());
    let (prepared, _) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut pool),
        HashSet::new(),
        &|_| true,
        &mut |_| {
            PendingTxCheckResult::Admit(PendingConstraints {
                paid_from_balance: TESTING_INIT_BALANCE,
                ..PendingConstraints::default()
            })
        },
        None,
    )
    .unwrap();
    // The transaction should fail balance validation (balance reduced to 0).
    assert_eq!(prepared.transactions.len(), 0);
}

/// When check_pending returns Admit with max_nonce, the effective nonce
/// floor is raised. Transactions with nonces <= max_nonce should be rejected.
#[test]
fn test_prepare_transactions_pending_nonce_constraint() {
    let (env, chain, _) = get_test_env_with_chain_and_pool();

    let signer = InMemorySigner::test_signer(&"test1".parse::<AccountId>().unwrap());
    // Create two transactions with nonces 1 and 2.
    let tx1 = SignedTransaction::send_money(
        1,
        signer.get_account_id(),
        "test2".parse().unwrap(),
        &signer,
        Balance::from_yoctonear(1),
        env.head.prev_block_hash,
    );
    let tx2 = SignedTransaction::send_money(
        2,
        signer.get_account_id(),
        "test2".parse().unwrap(),
        &signer,
        Balance::from_yoctonear(1),
        env.head.prev_block_hash,
    );
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    pool.insert_transaction(ValidatedTransaction::new_for_test(tx1));
    pool.insert_transaction(ValidatedTransaction::new_for_test(tx2));

    // With max_nonce=1, nonce 1 should be rejected (<=max_nonce), nonce 2 accepted.
    let (prepared, _) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut pool),
        HashSet::new(),
        &|_| true,
        &mut |_| {
            PendingTxCheckResult::Admit(PendingConstraints {
                max_nonce: 1,
                ..PendingConstraints::default()
            })
        },
        None,
    )
    .unwrap();
    assert_eq!(prepared.transactions.len(), 1);
    assert_eq!(signed_txs(&prepared.transactions).next().unwrap().nonce().nonce(), 2);
}

#[test]
fn test_strict_nonce_u64_max_not_included() {
    let (env, chain, _transaction_pool) = get_test_env_with_chain_and_pool();
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();
    let block = chain.get_block(&prev_hash).unwrap();
    let congestion_info = block.block_congestion_info();
    let next_epoch_id = env.epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();

    // Set the access key nonce to u64::MAX in the actual trie state so that
    // no strict-nonce tx can satisfy ak_nonce + 1 without overflow.
    let signer = InMemorySigner::test_signer(&"test1".parse::<AccountId>().unwrap());
    let ak_key = TrieKey::access_key("test1".parse().unwrap(), &signer.public_key());
    let ak_value =
        borsh::to_vec(&AccessKey { nonce: u64::MAX, permission: AccessKeyPermission::FullAccess })
            .unwrap();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    let state_root = test_populate_trie(
        &env.runtime.tries,
        &env.state_roots[shard_index],
        shard_uid,
        vec![(ak_key.to_vec(), Some(ak_value))],
    );
    let trie = env.runtime.tries.get_trie_for_shard(shard_uid, state_root);
    let state_update = TrieUpdate::new(trie);

    // Create a strict-nonce tx with nonce u64::MAX (the only value that could
    // plausibly match saturating_add(1) == u64::MAX).
    let strict_tx = SignedTransaction::from_actions_v1_strict(
        TransactionNonce::from_nonce(u64::MAX),
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        &signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
        env.head.prev_block_hash,
    );
    const TEST_SEED: RngSeed = [3; 32];
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    pool.insert_transaction(ValidatedTransaction::new_for_test(strict_tx));

    let (prepared, skipped) = env
        .runtime
        .prepare_transactions_extra(
            state_update,
            shard_id,
            block_context(&env, congestion_info, next_epoch_id),
            &mut PoolIteratorWrapper::new(&mut pool),
            &mut |tx: &SignedTransaction| -> bool {
                chain
                    .chain_store()
                    .check_transaction_validity_period(&block.header(), tx.transaction.block_hash())
                    .is_ok()
            },
            &|_| true,
            HashSet::new(),
            &mut PendingTxCheckResult::always_admit(),
            time_limits(default_produce_chunk_add_transactions_time_limit()),
            None,
            None,
        )
        .unwrap();

    assert!(prepared.transactions.is_empty(), "strict-nonce tx at u64::MAX should not be included");
    assert!(skipped.0.is_empty());
    // The tx was popped from the pool and discarded by the verifier (InvalidNonce).
    assert_eq!(pool.len(), 0);
}

/// Gapped strict-nonce transactions should not inflate the recorded witness
/// size. Sets the storage proof soft limit to 1 byte so that any recorded
/// trie read would exceed it, then verifies that the gap-check reads do NOT
/// trigger the limit (because they use a throwaway trie).
#[test]
fn test_strict_nonce_gap_does_not_count_towards_state_size_soft_limit() {
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.new_transactions_validation_state_size_soft_limit = 1;
    let validators: Vec<AccountId> = (1..=4).map(|i| format!("test{i}").parse().unwrap()).collect();
    let env = TestEnv::new_with_config(
        vec![validators],
        TestEnvConfig {
            epoch_length: 5,
            has_reward: false,
            minimum_stake_divisor: None,
            zero_fees: false,
            create_flat_storage: false,
            runtime_config_store: Some(RuntimeConfigStore::with_one_config(runtime_config)),
        },
    );
    let runtime = &env.runtime;

    let epoch_id = EpochId::default();
    let shard_layout = env.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();

    // Only gapped strict-nonce txs: nonce=100, but current nonce=0.
    let num_gapped = 3;
    const TEST_SEED: RngSeed = [3; 32];
    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    for i in 2..=4 {
        let account_id: AccountId = format!("test{i}").parse().unwrap();
        let signer = InMemorySigner::test_signer(&account_id);
        let tx = SignedTransaction::from_actions_v1_strict(
            TransactionNonce::from_nonce(100),
            account_id,
            "test1".parse().unwrap(),
            &signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            CryptoHash::default(),
        );
        pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
    }

    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    let trie = runtime.tries.get_trie_for_shard(shard_uid, env.state_roots[shard_index]);
    let trie = trie.recording_reads_new_recorder();
    let state_update = TrieUpdate::new(trie);

    let (prepared, skipped) = runtime
        .prepare_transactions_extra(
            state_update,
            shard_id,
            PrepareTransactionsBlockContext {
                next_gas_price: runtime.genesis_config.min_gas_price,
                height: 0,
                timestamp: 0,
                epoch_id,
                epoch_height: 0,
                next_epoch_id: epoch_id,
                congestion_info: Default::default(),
                gas_limit: runtime.genesis_config.gas_limit,
            },
            &mut PoolIteratorWrapper::new(&mut pool),
            &|_| true,
            &|_| true,
            HashSet::new(),
            &mut PendingTxCheckResult::always_admit(),
            time_limits(None),
            None,
            None,
        )
        .unwrap();

    // No txs included (all gapped).
    assert_eq!(prepared.transactions.len(), 0);
    assert!(skipped.0.is_empty());
    // All gapped txs stay in the pool.
    assert_eq!(pool.len(), num_gapped);
    // The 1-byte soft limit was NOT hit because the gap check used a
    // throwaway trie. Without proper accounting this would be StorageProofSize.
    assert_eq!(prepared.limited_by, PrepareTransactionsLimit::NoMoreTxsInPool);
}

/// Gapped strict-nonce transactions are evicted when their block_hash is older
/// than the TTL, but kept when still within range. Multiple expired txs from
/// the same signer group and across different signers are all evicted.
#[test]
fn test_strict_nonce_gap_ttl_eviction() {
    let (env, chain, _) = get_test_env_with_chain_and_pool();
    let prev_hash = env.head.prev_block_hash;
    // env.head.height == 1, prev_hash is genesis (height 0).

    const TEST_SEED: RngSeed = [3; 32];
    let mut pool = TransactionPool::new(TEST_SEED, None, "");

    // Insert 3 gapped txs from test1 (nonces 100, 101, 102) and 2 from test2 (nonces 200, 201).
    // All have ak_nonce=0 so all are gapped.
    let signer1 = InMemorySigner::test_signer(&"test1".parse::<AccountId>().unwrap());
    let signer2 = InMemorySigner::test_signer(&"test2".parse::<AccountId>().unwrap());
    for nonce in [100, 101, 102] {
        let tx = SignedTransaction::from_actions_v1_strict(
            TransactionNonce::from_nonce(nonce),
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            &signer1,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            prev_hash,
        );
        pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
    }
    for nonce in [200, 201] {
        let tx = SignedTransaction::from_actions_v1_strict(
            TransactionNonce::from_nonce(nonce),
            "test2".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer2,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            prev_hash,
        );
        pool.insert_transaction(ValidatedTransaction::new_for_test(tx));
    }
    assert_eq!(pool.len(), 5);

    // TTL=1: 1 <= 0 + 1 holds, all gapped txs stay in the pool.
    let ttl_valid = chain.chain_store().strict_nonce_ttl_check(env.head.height, 1);
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut pool),
        HashSet::new(),
        &ttl_valid,
        &mut PendingTxCheckResult::always_admit(),
        None,
    )
    .unwrap();
    assert!(prepared.transactions.is_empty());
    assert!(skipped.0.is_empty());
    assert_eq!(pool.len(), 5, "all gapped txs should be kept when TTL is sufficient");

    // TTL=0: 1 <= 0 + 0 does not hold, all gapped txs from both signers are evicted.
    let ttl_expired = chain.chain_store().strict_nonce_ttl_check(env.head.height, 0);
    let (prepared, skipped) = prepare_transactions_extra(
        &env,
        &chain,
        &mut PoolIteratorWrapper::new(&mut pool),
        HashSet::new(),
        &ttl_expired,
        &mut PendingTxCheckResult::always_admit(),
        None,
    )
    .unwrap();
    assert!(prepared.transactions.is_empty());
    assert!(skipped.0.is_empty());
    assert_eq!(pool.len(), 0, "all gapped txs should be evicted when TTL expired");
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_storage_proof_garbage() {
    let shard_id = ShardId::new(0);
    let signer = create_test_signer("test1");
    let env = TestEnv::new(vec![vec![signer.validator_id().clone()]], 100, false);
    let garbage_size_mb = 50usize;
    let receipt = Receipt::V0(ReceiptV0 {
        predecessor_id: signer.validator_id().clone(),
        receiver_id: signer.validator_id().clone(),
        receipt_id: CryptoHash::hash_bytes(&[42]),
        receipt: near_primitives::receipt::ReceiptEnum::Action(ActionReceipt {
            signer_id: signer.validator_id().clone(),
            signer_public_key: signer.public_key(),
            gas_price: env.runtime.genesis_config.min_gas_price,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![Action::function_call_with_deposit(
                format!("internal_record_storage_garbage_{garbage_size_mb}"),
                vec![],
                Gas::from_teragas(300),
                Balance::from_yoctonear(300000000000000),
            )],
        }),
    });
    let apply_result = env.apply_new_chunk(shard_id, Vec::<SignedTransaction>::new(), &[receipt]);
    let PartialState::TrieValues(storage_proof) = apply_result.proof.unwrap().nodes;
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    assert_eq!(total_size / 1000_000, garbage_size_mb);
}

/// Tests that precompiling a set of contracts updates the compiled contract cache.
#[test]
fn test_precompile_contracts_updates_cache() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let store = near_store::test_utils::create_test_store();
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    let contract_cache = FilesystemContractRuntimeCache::new(
        tempdir.path(),
        None::<&str>,
        "contract.cache",
        FilesystemContractRuntimeCache::MAX_DISK_CACHE_BYTES,
    )
    .expect("filesystem contract cache");
    let runtime = NightshadeRuntime::test_with_runtime_config_store(
        tempdir.path(),
        store,
        contract_cache.handle(),
        &genesis.config,
        epoch_manager,
        RuntimeConfigStore::new(),
    );

    let contracts = vec![
        ContractCode::new(near_test_contracts::sized_contract(100).to_vec(), None),
        ContractCode::new(near_test_contracts::rs_contract().to_vec(), None),
        ContractCode::new(near_test_contracts::trivial_contract().to_vec(), None),
    ];
    let code_hashes: Vec<CryptoHash> = contracts.iter().map(|c| c.hash()).cloned().collect();

    // First check that the cache does not have the contracts.
    let config = Arc::clone(&runtime.get_runtime_config(PROTOCOL_VERSION).wasm_config);
    for code_hash in &code_hashes {
        let config = Arc::clone(&config);
        assert!(!near_vm_runner::contract_cached(config, &contract_cache, *code_hash).unwrap());
    }
    runtime.precompile_contracts(&EpochId::default(), contracts).unwrap();
    // Check that the persistent cache contains the compiled contract after precompilation.
    for code_hash in code_hashes {
        let config = Arc::clone(&config);
        assert!(near_vm_runner::contract_cached(config, &contract_cache, code_hash).unwrap());
    }
}

fn stake(
    nonce: Nonce,
    signer: &Signer,
    sender: &ValidatorSigner,
    stake: Balance,
) -> SignedTransaction {
    SignedTransaction::from_actions(
        nonce,
        sender.validator_id().clone(),
        sender.validator_id().clone(),
        &signer,
        vec![Action::Stake(Box::new(StakeAction { stake, public_key: sender.public_key() }))],
        // runtime does not validate block history
        CryptoHash::default(),
    )
}

mod check_dynamic_resharding {
    use crate::runtime::check_dynamic_resharding;
    use near_primitives::epoch_manager::DynamicReshardingConfig;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::ShardId;
    use near_store::test_utils::{TestTriesBuilder, test_populate_trie};
    use near_store::{ShardUId, Trie};

    fn get_config(
        memory_usage_threshold: u64,
        min_child_memory_usage: u64,
        max_number_of_shards: u64,
        force_split_shards: Vec<ShardId>,
        block_split_shards: Vec<ShardId>,
    ) -> DynamicReshardingConfig {
        DynamicReshardingConfig {
            memory_usage_threshold,
            min_child_memory_usage,
            max_number_of_shards,
            min_epochs_between_resharding: 1.try_into().unwrap(),
            force_split_shards,
            block_split_shards,
        }
    }

    fn make_trie_with_accounts(accounts: &[(&str, usize)]) -> Trie {
        let tries = TestTriesBuilder::new().with_flat_storage(true).build();
        let shard_uid = ShardUId::single_shard();

        let changes = accounts
            .iter()
            .map(|(acc, size)| {
                let key = TrieKey::Account { account_id: acc.parse().unwrap() };
                (key.to_vec(), Some(vec![0u8; *size]))
            })
            .collect();

        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes);
        tries.get_trie_for_shard(shard_uid, root)
    }

    #[test]
    fn max_shards_reached() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(0, 0, 1, vec![], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn block_split_shards() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(0, 0, 100, vec![], vec![shard_id]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn force_split_shards() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        // shard_id is in force_split_shards, should split regardless of memory
        let config = get_config(u64::MAX, u64::MAX, 100, vec![shard_id], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn below_memory_threshold() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(u64::MAX, 0, 100, vec![], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn left_child_too_small() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100_000)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(0, 1000, 100, vec![], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn right_child_too_small() {
        let trie = make_trie_with_accounts(&[("bb", 100_000), ("cc", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(0, 1000, 100, vec![], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn split_approved() {
        let trie = make_trie_with_accounts(&[("aa", 100), ("bb", 100)]);
        let shard_layout = ShardLayout::single_shard();
        let shard_id = ShardId::new(0);

        let config = get_config(0, 0, 100, vec![], vec![]);
        let result = check_dynamic_resharding(&trie, shard_id, shard_layout, &config);
        assert!(result.unwrap().is_some());
    }
}
