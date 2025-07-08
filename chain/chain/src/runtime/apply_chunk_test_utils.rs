use std::collections::BTreeSet;

use crate::stateless_validation::processing_tracker::ProcessingDoneTracker;
use crate::types::BlockType;

use super::*;
use itertools::Itertools;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::time::Clock;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{DEFAULT_GC_NUM_EPOCHS_TO_KEEP, Genesis, NEAR_BASE};
use near_crypto::{InMemorySigner, Signer};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::bandwidth_scheduler::{BandwidthRequests, BlockBandwidthRequests};
use near_primitives::congestion_info::{BlockCongestionInfo, CongestionInfo};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use near_primitives::types::{AccountId, BlockHeightDelta, Nonce, ValidatorId};
use near_primitives::utils::from_timestamp;
use near_primitives::views::AccountView;
use near_store::flat::{FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata};
use near_store::genesis::initialize_genesis_state;
use near_store::{NodeStorage, PartialStorage, get_account, get_genesis_state_roots};
use near_vm_runner::FilesystemContractRuntimeCache;
use num_rational::Ratio;
use primitive_types::U256;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

pub struct TestEnvConfig {
    pub epoch_length: BlockHeightDelta,
    pub has_reward: bool,
    pub minimum_stake_divisor: Option<u64>,
    pub zero_fees: bool,
    pub create_flat_storage: bool,
}

#[derive(Clone, Debug)]
pub struct UserAccount {
    account_id: AccountId,
    signer: Signer,
    nonce: Nonce,
}

impl UserAccount {
    fn new(account_id: AccountId) -> Self {
        let signer = InMemorySigner::test_signer(&account_id);
        Self { account_id, signer, nonce: 0 }
    }

    fn next_nonce(&mut self) -> Nonce {
        let nonce = self.nonce;
        self.nonce += 1;
        nonce + 1
    }
}

#[derive(Default, Clone)]
pub struct TestEnvConfigExtended {
    user_accounts: Vec<UserAccount>,
    shard_layout: Option<ShardLayout>,
    load_memtries: bool,
}

impl TestEnvConfigExtended {
    pub fn add_user_accounts_simple(mut self, accounts: &[AccountId]) -> Self {
        self.user_accounts
            .extend(accounts.iter().map(|account_id| UserAccount::new(account_id.clone())));
        self
    }

    pub fn get_account_id(&self, i: usize) -> &AccountId {
        &self.user_accounts[i].account_id
    }
}
/// Environment to test runtime behavior separate from Chain.
/// Runtime operates in a mock chain where i-th block is attached to (i-1)-th one, has height `i` and hash
/// `hash([i])`.
pub struct TestEnv {
    pub epoch_manager: Arc<EpochManagerHandle>,
    pub runtime: Arc<NightshadeRuntime>,
    pub head: Tip,
    pub state_roots: Vec<StateRoot>,
    pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
    pub last_shard_proposals: HashMap<ShardId, Vec<ValidatorStake>>,
    pub last_proposals: Vec<ValidatorStake>,
    pub last_proofs: HashMap<ShardId, PartialStorage>,
    pub keep_proofs: bool,
    pub time: u64,
    _dir: TempDir, // hold the temp dir to prevent it from being deleted
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
            },
        )
    }

    pub fn new_with_config(validators: Vec<Vec<AccountId>>, config: TestEnvConfig) -> Self {
        Self::new_with_extended_config(validators, config, Default::default())
    }

    fn new_with_extended_config(
        validators: Vec<Vec<AccountId>>,
        config: TestEnvConfig,
        config_ext: TestEnvConfigExtended,
    ) -> Self {
        let (dir, opener) = NodeStorage::test_opener();
        let store = opener.open().unwrap().get_hot_store();
        let all_validators = validators.iter().fold(BTreeSet::new(), |acc, x| {
            acc.union(&x.iter().cloned().collect()).cloned().collect()
        });
        let validators_len = all_validators.len() as ValidatorId;

        let mut genesis = if let Some(shard_layout) = config_ext.shard_layout {
            let clock = Clock::real();
            let genesis_time = from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64);
            let validators_strings: Vec<_> =
                all_validators.iter().map(|account_id| account_id.to_string()).collect();
            let validators_spec = ValidatorsSpec::desired_roles(
                &validators_strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                &[],
            );
            let user_accounts = config_ext
                .user_accounts
                .iter()
                .map(|account| account.account_id.clone())
                .collect::<Vec<_>>();
            let liquid_balance = 100_000_000 * NEAR_BASE;
            TestGenesisBuilder::new()
                .validators_spec(validators_spec.clone())
                .genesis_time(genesis_time)
                .shard_layout(shard_layout)
                .add_user_accounts_simple(&user_accounts, liquid_balance)
                .build()
        } else {
            Genesis::test_sharded_new_version(
                all_validators.into_iter().collect(),
                validators_len,
                validators.iter().map(|x| x.len() as ValidatorId).collect(),
            )
        };

        // No fees mode.
        genesis.config.epoch_length = config.epoch_length;
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

        let runtime_config_store =
            if config.zero_fees { RuntimeConfigStore::free() } else { RuntimeConfigStore::test() };

        let compiled_contract_cache =
            FilesystemContractRuntimeCache::new(&dir.as_ref(), None::<&str>).unwrap();

        initialize_genesis_state(store.clone(), &genesis, Some(dir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
        let mut trie_config = TrieConfig::default();
        trie_config.load_memtries_for_tracked_shards = config_ext.load_memtries;
        let runtime = NightshadeRuntime::new(
            store.clone(),
            compiled_contract_cache.handle(),
            &genesis.config,
            epoch_manager.clone(),
            None,
            None,
            Some(runtime_config_store),
            DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            trie_config,
            StateSnapshotConfig::enabled(dir.path(), "data", "state_snapshot"),
        );
        let state_roots = get_genesis_state_roots(&store).unwrap().unwrap();
        assert_eq!(
            state_roots.len(),
            epoch_manager.get_shard_layout(&EpochId::default()).unwrap().num_shards() as usize
        );
        let genesis_hash = hash(&[0]);

        if config.create_flat_storage {
            // Create flat storage. Naturally it happens on Chain creation, but here we test only Runtime behavior
            // and use a mock chain, so we need to initialize flat storage manually.
            let flat_storage_manager = runtime.get_flat_storage_manager();
            for shard_uid in
                epoch_manager.get_shard_layout(&EpochId::default()).unwrap().shard_uids()
            {
                let mut store_update = store.store_update();
                flat_storage_manager.set_flat_storage_for_genesis(
                    &mut store_update.flat_store_update(),
                    shard_uid,
                    &genesis_hash,
                    0,
                );
                store_update.commit().unwrap();
                assert!(matches!(
                    flat_storage_manager.get_flat_storage_status(shard_uid),
                    near_store::flat::FlatStorageStatus::Ready(_)
                ));
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
        }
        if config_ext.load_memtries {
            assert!(config.create_flat_storage);
            let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
            let all_shards = shard_layout.shard_uids().collect_vec();
            // ChunkExtra is needed for in-memory trie loading code to query state roots.
            let congestion_info = Some(CongestionInfo::default());
            let mut update_for_chunk_extra = store.store_update();
            for shard_uid in &all_shards {
                let shard_id = shard_uid.shard_id();
                let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
                let chunk_extra = ChunkExtra::new(
                    &state_roots[shard_index],
                    CryptoHash::default(),
                    Vec::new(),
                    0,
                    0,
                    0,
                    congestion_info,
                    BandwidthRequests::empty(),
                );
                update_for_chunk_extra
                    .set_ser(
                        DBCol::ChunkExtra,
                        &get_block_shard_uid(&genesis_hash, shard_uid),
                        &chunk_extra,
                    )
                    .unwrap();
            }
            update_for_chunk_extra.commit().unwrap();
            runtime
                .get_tries()
                .load_memtries_for_enabled_shards(&all_shards, &[].into(), false)
                .expect("Failed to load memtries for enabled shards");
        }

        epoch_manager
            .add_validator_proposals(
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
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .commit()
            .unwrap();
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
            last_proofs: HashMap::default(),
            keep_proofs: false,
            time: 0,
            _dir: dir,
        }
    }

    pub fn apply_new_chunk(
        &self,
        shard_id: ShardId,
        transactions: Vec<SignedTransaction>,
        receipts: &[Receipt],
    ) -> ApplyChunkResult {
        let context = self.apply_chunk_context(shard_id);
        let args = self.apply_new_chunk_args_with_storage(
            shard_id,
            transactions,
            receipts,
            &context,
            None,
        );
        self.apply_new_chunk_with_storage(args)
    }

    pub fn apply_chunk_context(&self, shard_id: ShardId) -> TestApplyChunkContext {
        let prev_block_hash = self.head.last_block_hash;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).unwrap();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        TestApplyChunkContext {
            prev_block_hash: self.head.last_block_hash,
            state_root: self.state_roots[shard_index],
            height: self.head.height + 1,
        }
    }

    fn apply_new_chunk_args_with_storage(
        &self,
        shard_id: ShardId,
        transactions: Vec<SignedTransaction>,
        receipts: &[Receipt],
        context: &TestApplyChunkContext,
        storage: Option<PartialStorage>,
    ) -> ApplyChunkArgs {
        // TODO(congestion_control): pass down prev block info and read congestion info from there
        // For now, just use default.
        // TODO(bandwidth_scheduler) - pass bandwidth requests from prev_block
        let prev_block_hash = context.prev_block_hash;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).unwrap();
        let height = context.height;
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
        let reason = if storage.is_some() {
            ApplyChunkReason::ValidateChunkStateWitness
        } else {
            ApplyChunkReason::UpdateTrackedShard
        };
        let mut storage_config = RuntimeStorageConfig::new(context.state_root, true);
        if let Some(storage) = storage {
            storage_config.source = StorageDataSource::Recorded(storage);
        }

        ApplyChunkArgs {
            storage_config,
            apply_reason: reason,
            shard_id,
            // TODO: move to context
            validator_proposals: self
                .last_shard_proposals
                .get(&shard_id)
                .cloned()
                .unwrap_or_default(),
            block: ApplyChunkBlockContext {
                block_type: BlockType::Normal,
                height,
                prev_block_hash,
                block_timestamp,
                gas_price,
                random_seed: CryptoHash::default(),
                congestion_info,
                bandwidth_requests: BlockBandwidthRequests::empty(),
            },
            receipts: receipts.to_vec(),
            transactions,
        }
    }

    pub fn apply_new_chunk_with_storage(&self, args: ApplyChunkArgs) -> ApplyChunkResult {
        apply_chunk_using_args(&self.runtime, args)
    }

    fn update_runtime(
        &self,
        shard_id: ShardId,
        new_block_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        receipts: &[Receipt],
    ) -> ApplyChunkResult {
        let mut apply_result = self.apply_new_chunk(shard_id, transactions, receipts);
        let mut store_update = self.runtime.store().store_update();
        let flat_state_changes =
            FlatStateChanges::from_state_changes(&apply_result.trie_changes.state_changes());
        apply_result.trie_changes.insertions_into(&mut store_update.trie_store_update());
        apply_result
            .trie_changes
            .state_changes_into(&new_block_hash, &mut store_update.trie_store_update());
        apply_result.trie_changes.apply_mem_changes();

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
        store_update.commit().unwrap();

        apply_result
    }

    pub fn step(&mut self, transactions: Vec<Vec<SignedTransaction>>, chunk_mask: Vec<bool>) {
        let new_hash = hash(&[(self.head.height + 1) as u8]);
        let shard_ids = self.epoch_manager.shard_ids(&self.head.epoch_id).unwrap();
        let shard_layout = self.epoch_manager.get_shard_layout(&self.head.epoch_id).unwrap();
        assert_eq!(transactions.len(), shard_ids.len());
        assert_eq!(chunk_mask.len(), shard_ids.len());
        let mut all_proposals = vec![];
        let mut all_receipts = vec![];

        if self.keep_proofs {
            self.last_proofs.clear();
        }
        for shard_id in shard_ids {
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let apply_result = self.update_runtime(
                shard_id,
                new_hash,
                transactions[shard_index].clone(),
                self.last_receipts.get(&shard_id).map_or(&[], |v| v.as_slice()),
            );
            self.state_roots[shard_index] = apply_result.new_root;
            all_receipts.extend(apply_result.outgoing_receipts);
            all_proposals.append(&mut apply_result.validator_proposals.clone());
            self.last_shard_proposals.insert(shard_id, apply_result.validator_proposals);
            if self.keep_proofs {
                if let Some(storage) = apply_result.proof {
                    self.last_proofs.insert(shard_id, storage);
                }
            }
        }
        self.epoch_manager
            .add_validator_proposals(
                BlockInfo::new(
                    new_hash,
                    self.head.height + 1,
                    self.head.height.saturating_sub(1),
                    self.head.last_block_hash,
                    self.head.last_block_hash,
                    self.last_proposals.clone(),
                    chunk_mask,
                    self.runtime.genesis_config.total_supply,
                    self.runtime.genesis_config.protocol_version,
                    self.time + 10u64.pow(9),
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .commit()
            .unwrap();
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
        let per_epoch_total_reward =
            (U256::from(*self.runtime.genesis_config.max_inflation_rate.numer() as u64)
                * U256::from(self.runtime.genesis_config.total_supply)
                * U256::from(epoch_duration)
                / (U256::from(num_seconds_per_year)
                    * U256::from(*self.runtime.genesis_config.max_inflation_rate.denom() as u128)
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

#[derive(Clone, Debug)]
pub struct TestApplyChunkContext {
    pub prev_block_hash: CryptoHash,
    pub state_root: StateRoot,
    pub height: u64,
}

const TERAGAS: u64 = 1_000_000_000_000;

fn gen_transactions_by_shard(
    rng: &mut StdRng,
    block_hash: CryptoHash,
    accounts: &mut [UserAccount],
    shard_layout: &ShardLayout,
    num_transactions: usize,
) -> Vec<Vec<SignedTransaction>> {
    let mut transactions_by_shard_index = vec![Vec::new(); shard_layout.num_shards() as usize];

    for _ in 0..num_transactions {
        // Choose sender and receiver randomly from the accounts.
        let sender = rng.gen_range(0..accounts.len());
        let receiver = rng.gen_range(0..accounts.len());

        let shard_id = shard_layout.account_id_to_shard_id(&accounts[sender].account_id);
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        transactions_by_shard_index[shard_index].push(SignedTransaction::send_money(
            accounts[sender].next_nonce(),
            accounts[sender].account_id.clone(),
            accounts[receiver].account_id.clone(),
            &accounts[sender].signer,
            1,
            block_hash,
        ));
    }

    transactions_by_shard_index
}

pub struct TestApplyChunkParams {
    pub num_shards: usize,
    pub num_txs_per_chunk: usize,
    pub num_accounts: usize,
}

pub fn test_apply_new_chunk_setup(params: TestApplyChunkParams) -> TestApplyChunkSetup {
    let accounts = (0..params.num_accounts)
        .map(|i| format!("account{:06}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let num_shards = params.num_shards;
    // choose boundary_accounts based on num_shards to equally partition accounts
    let boundary_accounts = accounts
        .chunks(accounts.len() / num_shards)
        .map(|chunk| chunk.last().unwrap().to_owned())
        .take(num_shards - 1)
        .collect::<Vec<_>>();
    eprintln!("Boundary accounts: {:?}", boundary_accounts);
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 3);

    let num_nodes = num_shards;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let config = TestEnvConfig {
        epoch_length: 10,
        has_reward: false,
        minimum_stake_divisor: None,
        zero_fees: false,
        create_flat_storage: true,
    };
    let mut config_ext = TestEnvConfigExtended::default();
    config_ext.shard_layout = Some(shard_layout.clone());
    config_ext.load_memtries = true;
    config_ext = config_ext.add_user_accounts_simple(&accounts);
    let mut env =
        TestEnv::new_with_extended_config(vec![validators.clone()], config, config_ext.clone());

    let account_id = config_ext.get_account_id(0).clone();
    let shard_id = shard_layout.account_id_to_shard_id(&account_id);

    let state_roots = get_genesis_state_roots(env.runtime.store())
        .expect("genesis state roots should be initialized")
        .expect("genesis state roots should not be empty");

    let block_hash = env.head.last_block_hash;
    let trie = env.runtime.get_trie_for_shard(shard_id, &block_hash, state_roots[0], true).unwrap();
    let account = get_account(&trie, &account_id)
        .expect("account should exist in the trie")
        .expect("account should be present in the trie");
    assert!(account.amount() > 0);
    assert!(trie.has_memtries());

    // Seed rng for consistent transaction generation.
    let mut rng = StdRng::from_seed([0; 32]);
    let transactions_by_shard_index = gen_transactions_by_shard(
        &mut rng,
        env.head.last_block_hash,
        &mut config_ext.user_accounts,
        &shard_layout,
        params.num_txs_per_chunk,
    );
    // These chunks will not have any incoming receipts, so we are going to make another
    // step right after this one to generate some receipts.
    env.step(transactions_by_shard_index, vec![true; num_shards]);

    let transactions_by_shard_index = gen_transactions_by_shard(
        &mut rng,
        env.head.last_block_hash,
        &mut config_ext.user_accounts,
        &shard_layout,
        params.num_txs_per_chunk,
    );

    // Remember receipts so we can apply them with the recorded storage.
    let last_receipts = env.last_receipts.clone();

    env.keep_proofs = true;
    let apply_contexts = (0..num_shards)
        .map(|shard_index| {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            eprintln!("Remembering context for shard {}", shard_id);
            env.apply_chunk_context(shard_id)
        })
        .collect_vec();
    env.step(transactions_by_shard_index.clone(), vec![true; num_shards]);
    env.keep_proofs = false;

    // Generate new transactions for the next step.
    let new_transactions_by_shard_index = gen_transactions_by_shard(
        &mut rng,
        env.head.last_block_hash,
        &mut config_ext.user_accounts,
        &shard_layout,
        params.num_txs_per_chunk,
    );

    TestApplyChunkSetup {
        env,
        last_receipts,
        transactions_by_shard_index,
        new_transactions_by_shard_index,
        shard_layout,
        apply_contexts,
    }
}

pub fn test_apply_new_chunk_impl(setup: &TestApplyChunkSetup, verbose: bool) {
    for (shard_index, transactions) in setup.transactions_by_shard_index.iter().enumerate() {
        // See if it works with recorded storage.
        let shard_id = setup.shard_layout.get_shard_id(shard_index).unwrap();
        if verbose {
            eprintln!(
                "Applying {} transactions with recorded storage for shard {} (index = {})",
                transactions.len(),
                shard_id,
                shard_index
            );
        }
        let recorded = setup.env.last_proofs.get(&shard_id).cloned().unwrap(); // TODO: clone as part of setup
        let receipts = setup.last_receipts.get(&shard_id).map(Vec::as_slice).unwrap_or(&[]);
        let apply_args = setup.env.apply_new_chunk_args_with_storage(
            shard_id,
            transactions.clone(), // TODO: clone as part of setup
            receipts,
            &setup.apply_contexts[shard_index],
            Some(recorded),
        );
        let apply_result = setup.env.apply_new_chunk_with_storage(apply_args);
        if verbose {
            eprintln!("Incoming receipts: {}", receipts.len());
            eprintln!("Outgoing receipts: {:?}", apply_result.outgoing_receipts.len());
            eprintln!("Gas used: {}T", apply_result.total_gas_burnt / TERAGAS);
            eprintln!("Transaction outcomes: {}", apply_result.outcomes.len());
        }
    }

    // TODO: use thread pool to apply chunks in parallel
    // TODO: measurement
}

pub struct TestApplyChunkSetup {
    pub env: TestEnv,
    pub transactions_by_shard_index: Vec<Vec<SignedTransaction>>,
    pub new_transactions_by_shard_index: Vec<Vec<SignedTransaction>>,
    pub last_receipts: HashMap<ShardId, Vec<Receipt>>,
    pub shard_layout: ShardLayout,
    pub apply_contexts: Vec<TestApplyChunkContext>,
}

// TODO: Use NewChunkData instead
struct ApplyChunkArgs {
    storage_config: RuntimeStorageConfig,
    apply_reason: ApplyChunkReason,
    shard_id: ShardId,
    validator_proposals: Vec<ValidatorStake>,
    block: ApplyChunkBlockContext,
    receipts: Vec<Receipt>,
    transactions: SignedValidPeriodTransactions,
}

fn apply_chunk_using_args(runtime: &NightshadeRuntime, args: ApplyChunkArgs) -> ApplyChunkResult {
    runtime
        .apply_chunk(
            args.storage_config,
            args.apply_reason,
            ApplyChunkShardContext {
                shard_id: args.shard_id,
                last_validator_proposals: ValidatorStakeIter::new(&args.validator_proposals),
                gas_limit: u64::MAX, // TODO: use real gas limit
                is_new_chunk: true,
            },
            args.block,
            &args.receipts,
            args.transactions,
        )
        .unwrap()
}

pub struct TestApplyChunkInputs {
    pub args: ApplyChunkArgs,
    pub done: ProcessingDoneTracker,
    spawn_delay: std::time::Duration,
}

pub fn run(
    runtime: Arc<NightshadeRuntime>,
    inputs: Vec<TestApplyChunkInputs>,
    spawner: Arc<dyn AsyncComputationSpawner>,
) {
    // Make waiters for all inputs.
    let waiters = inputs.iter().map(|input| input.done.make_waiter()).collect_vec();

    // Spawn tasks for each input.
    let start_time = std::time::Instant::now();
    for input in inputs {
        let since_start = std::time::Instant::now().duration_since(start_time);
        if since_start < input.spawn_delay {
            std::thread::sleep(input.spawn_delay - since_start);
        }

        let runtime = runtime.clone();
        spawner.spawn("apply_chunk", move || {
            apply_chunk_using_args(&runtime, input.args);
            drop(input.done);
        });
    }

    // Wait for all tasks to finish.
    for waiter in waiters {
        waiter.wait();
    }
}

impl TestApplyChunkSetup {
    pub fn new_case(
        &self,
        apply_from_recorded: Vec<(usize, std::time::Duration)>,
        apply_new_chunk: Vec<(usize, std::time::Duration)>,
    ) -> Vec<TestApplyChunkInputs> {
        let mut inputs = Vec::new();
        for (shard_index, delay) in apply_from_recorded {
            let shard_id = self.shard_layout.get_shard_id(shard_index).unwrap();
            let receipts = self.last_receipts.get(&shard_id).map(Vec::as_slice).unwrap_or(&[]);
            let recorded = self.env.last_proofs.get(&shard_id).cloned().unwrap();

            inputs.push(TestApplyChunkInputs {
                args: self.env.apply_new_chunk_args_with_storage(
                    shard_id,
                    self.transactions_by_shard_index[shard_index].clone(),
                    receipts,
                    &self.apply_contexts[shard_index],
                    Some(recorded),
                ),
                done: ProcessingDoneTracker::new(),
                spawn_delay: delay,
            });
        }

        for (shard_index, delay) in apply_new_chunk {
            let shard_id = self.shard_layout.get_shard_id(shard_index).unwrap();
            let receipts = self.env.last_receipts.get(&shard_id).map(Vec::as_slice).unwrap_or(&[]);

            inputs.push(TestApplyChunkInputs {
                args: self.env.apply_new_chunk_args_with_storage(
                    shard_id,
                    self.new_transactions_by_shard_index[shard_index].clone(),
                    receipts,
                    &self.env.apply_chunk_context(shard_id),
                    None,
                ),
                done: ProcessingDoneTracker::new(),
                spawn_delay: delay,
            });
        }

        // Sort inputs by spawn delay to ensure that we apply chunks in the order of their spawn delay.
        inputs.sort_by_key(|input| input.spawn_delay);

        inputs
    }
}
