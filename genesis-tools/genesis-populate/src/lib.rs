//! Tools for creating a genesis block.

pub mod state_dump;

use crate::state_dump::StateDump;
use indicatif::{ProgressBar, ProgressStyle};
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, Chain, ChainStore};
use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, KeyType};
use near_epoch_manager::types::BlockHeaderInfo;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::{genesis_chunks, Tip};
use near_primitives::contract::ContractCode;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::shard_layout::{account_id_to_shard_id, ShardUId};
use near_primitives::state_record::StateRecord;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, Balance, EpochId, ShardId, StateChangeCause, StateRoot};
use near_store::genesis::{compute_storage_usage, initialize_genesis_state};
use near_store::{
    get_account, get_genesis_state_roots, set_access_key, set_account, set_code, Store, TrieUpdate,
};
use nearcore::{NearConfig, NightshadeRuntime};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Deterministically construct an account ID by index.
///
/// This is used by the estimator to fill a DB with many accounts for which the
/// name can be constructed again during estimations.
///
/// The ids are constructed to form a somewhat interesting shape in the trie. It
/// starts with a hash that will be different for each account, followed by a
/// string that is sufficiently long. The hash is supposed to produces a bunch
/// of branches, whereas the string after that will produce an extension.
///
/// If anyone has a reason to change the format, there is no strong reason to
/// keep it exactly as it is. But keeping the length of the accounts the same
/// would be desired to avoid breaking tests and estimations.
///
/// Note that existing estimator DBs need to be reconstructed when the format
/// changes. Daily estimations are not affected by this.
pub fn get_account_id(account_index: u64) -> AccountId {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    account_index.hash(&mut hasher);
    let hash = hasher.finish();
    // Some estimations rely on the account ID length being constant.
    // Pad booth numbers to length 20, the longest decimal representation of an u64.
    AccountId::try_from(format!("{hash:020}_near_{account_index:020}")).unwrap()
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct GenesisBuilder {
    home_dir: PathBuf,
    // We hold this temporary directory to avoid deletion through deallocation.
    #[allow(dead_code)]
    tmpdir: tempfile::TempDir,
    genesis: Arc<Genesis>,
    store: Store,
    epoch_manager: Arc<EpochManagerHandle>,
    runtime: Arc<NightshadeRuntime>,
    unflushed_records: BTreeMap<ShardId, Vec<StateRecord>>,
    roots: BTreeMap<ShardId, StateRoot>,
    state_updates: BTreeMap<ShardId, TrieUpdate>,

    // Things that can be set.
    additional_accounts_num: u64,
    additional_accounts_code: Option<Vec<u8>>,
    additional_accounts_code_hash: CryptoHash,

    print_progress: bool,
}

impl GenesisBuilder {
    pub fn from_config_and_store(home_dir: &Path, config: NearConfig, store: Store) -> Self {
        let tmpdir = tempfile::Builder::new().prefix("storage").tempdir().unwrap();
        initialize_genesis_state(store.clone(), &config.genesis, Some(tmpdir.path()));
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
        let runtime = NightshadeRuntime::from_config(
            tmpdir.path(),
            store.clone(),
            &config,
            epoch_manager.clone(),
        );
        Self {
            home_dir: home_dir.to_path_buf(),
            tmpdir,
            genesis: Arc::new(config.genesis),
            store,
            epoch_manager,
            runtime,
            unflushed_records: Default::default(),
            roots: Default::default(),
            state_updates: Default::default(),
            additional_accounts_num: 0,
            additional_accounts_code: None,
            additional_accounts_code_hash: CryptoHash::default(),
            print_progress: false,
        }
    }

    pub fn print_progress(mut self) -> Self {
        self.print_progress = true;
        self
    }

    pub fn add_additional_accounts(mut self, num: u64) -> Self {
        self.additional_accounts_num = num;
        self
    }

    pub fn add_additional_accounts_contract(mut self, contract_code: Vec<u8>) -> Self {
        self.additional_accounts_code_hash = hash(&contract_code);
        self.additional_accounts_code = Some(contract_code);
        self
    }

    pub fn build(mut self) -> Result<Self> {
        // First, apply whatever is defined by the genesis config.
        let roots = get_genesis_state_roots(self.runtime.store())?
            .expect("genesis state roots not initialized.");
        let genesis_shard_version = self.genesis.config.shard_layout.version();
        self.roots = roots.into_iter().enumerate().map(|(k, v)| (k as u64, v)).collect();
        self.state_updates = self
            .roots
            .iter()
            .map(|(shard_idx, root)| {
                (
                    *shard_idx,
                    self.runtime.get_tries().new_trie_update(
                        ShardUId { version: genesis_shard_version, shard_id: *shard_idx as u32 },
                        *root,
                    ),
                )
            })
            .collect();
        self.unflushed_records =
            self.roots.keys().cloned().map(|shard_idx| (shard_idx, vec![])).collect();

        let num_shards = self.genesis.config.shard_layout.num_shards();
        let total_accounts_num = self.additional_accounts_num * num_shards;
        let bar = ProgressBar::new(total_accounts_num as _);
        bar.set_style(ProgressStyle::default_bar().template(
            "[elapsed {elapsed_precise} remaining {eta_precise}] Writing into storage {bar} {pos:>7}/{len:7}",
        ));
        // Add records in chunks of 3000 per shard for memory efficiency reasons.
        for i in 0..total_accounts_num {
            let account_id = get_account_id(i);
            self.add_additional_account(account_id)?;
            bar.inc(1);
        }

        for shard_id in 0..num_shards {
            self.flush_shard_records(shard_id)?;
        }
        bar.finish();
        self.write_genesis_block()?;
        Ok(self)
    }

    pub fn dump_state(self) -> Result<Self> {
        let state_dump =
            StateDump { store: self.store.clone(), roots: self.roots.values().cloned().collect() };
        state_dump.save_to_dir(self.home_dir.clone())?;
        Ok(self)
    }

    fn flush_shard_records(&mut self, shard_idx: ShardId) -> Result<()> {
        let records = self.unflushed_records.insert(shard_idx, vec![]).unwrap_or_default();
        if records.is_empty() {
            return Ok(());
        }
        let mut state_update =
            self.state_updates.remove(&shard_idx).expect("State updates are always available");
        let protocol_config = self.runtime.get_protocol_config(&EpochId::default())?;
        let storage_usage_config = protocol_config.runtime_config.fees.storage_usage_config;

        // Compute storage usage and update accounts.
        for (account_id, storage_usage) in compute_storage_usage(&records, &storage_usage_config) {
            let mut account =
                get_account(&state_update, &account_id)?.expect("We should've created account");
            account.set_storage_usage(storage_usage);
            set_account(&mut state_update, account_id, &account);
        }
        let tries = self.runtime.get_tries();
        state_update.commit(StateChangeCause::InitialState);
        let (_, trie_changes, state_changes) = state_update.finalize()?;
        let genesis_shard_version = self.genesis.config.shard_layout.version();
        let shard_uid = ShardUId { version: genesis_shard_version, shard_id: shard_idx as u32 };
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        near_store::flat::FlatStateChanges::from_state_changes(&state_changes)
            .apply_to_flat_state(&mut store_update, shard_uid);
        store_update.commit()?;

        self.roots.insert(shard_idx, root);
        self.state_updates.insert(shard_idx, tries.new_trie_update(shard_uid, root));
        Ok(())
    }

    fn write_genesis_block(&mut self) -> Result<()> {
        let genesis_chunks = genesis_chunks(
            self.roots.values().cloned().collect(),
            self.genesis.config.shard_layout.num_shards(),
            self.genesis.config.gas_limit,
            self.genesis.config.genesis_height,
            self.genesis.config.protocol_version,
        );
        let genesis = Block::genesis(
            self.genesis.config.protocol_version,
            genesis_chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
            self.genesis.config.genesis_time,
            self.genesis.config.genesis_height,
            self.genesis.config.min_gas_price,
            self.genesis.config.total_supply,
            Chain::compute_bp_hash(
                self.epoch_manager.as_ref(),
                EpochId::default(),
                EpochId::default(),
                &CryptoHash::default(),
            )?,
        );

        let mut store =
            ChainStore::new(self.store.clone(), self.genesis.config.genesis_height, true);
        let mut store_update = store.store_update();

        store_update.merge(
            self.epoch_manager
                .add_validator_proposals(BlockHeaderInfo::new(genesis.header(), 0))
                .unwrap(),
        );
        store_update
            .save_block_header(genesis.header().clone())
            .expect("save genesis block header shouldn't fail");
        store_update.save_block(genesis.clone());

        for (chunk_header, state_root) in genesis.chunks().iter().zip(self.roots.values()) {
            store_update.save_chunk_extra(
                genesis.hash(),
                &ShardUId::from_shard_id_and_layout(
                    chunk_header.shard_id(),
                    &self.genesis.config.shard_layout,
                ),
                ChunkExtra::new(
                    state_root,
                    CryptoHash::default(),
                    vec![],
                    0,
                    self.genesis.config.gas_limit,
                    0,
                ),
            );
        }

        let head = Tip::from_header(genesis.header());
        store_update.save_head(&head).unwrap();
        store_update.save_final_head(&head).unwrap();
        store_update.commit().unwrap();

        Ok(())
    }

    fn add_additional_account(&mut self, account_id: AccountId) -> Result<()> {
        let testing_init_balance: Balance = 10u128.pow(30);
        let testing_init_stake: Balance = 0;
        let shard_id = account_id_to_shard_id(&account_id, &self.genesis.config.shard_layout);
        let mut records = self.unflushed_records.remove(&shard_id).unwrap_or_default();
        let mut state_update =
            self.state_updates.remove(&shard_id).expect("State update should have been added");

        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let account = Account::new(
            testing_init_balance,
            testing_init_stake,
            self.additional_accounts_code_hash,
            0,
        );
        set_account(&mut state_update, account_id.clone(), &account);
        let account_record = StateRecord::Account { account_id: account_id.clone(), account };
        records.push(account_record);
        let access_key_record = StateRecord::AccessKey {
            account_id: account_id.clone(),
            public_key: signer.public_key.clone(),
            access_key: AccessKey::full_access(),
        };
        set_access_key(
            &mut state_update,
            account_id.clone(),
            signer.public_key,
            &AccessKey::full_access(),
        );
        records.push(access_key_record);
        if let Some(wasm_binary) = self.additional_accounts_code.as_ref() {
            let code = ContractCode::new(wasm_binary.clone(), None);
            set_code(&mut state_update, account_id.clone(), &code);
            let contract_record = StateRecord::Contract { account_id, code: wasm_binary.clone() };
            records.push(contract_record);
        }

        // Add records in chunks of 3000 per shard for memory efficiency reasons.
        const CHUNK_SIZE: usize = 3000;
        let num_records_to_flush = records.len();
        let needs_flush = num_records_to_flush >= CHUNK_SIZE;
        self.unflushed_records.insert(shard_id, records);
        self.state_updates.insert(shard_id, state_update);

        if needs_flush {
            self.flush_shard_records(shard_id)?;
        }
        Ok(())
    }
}
