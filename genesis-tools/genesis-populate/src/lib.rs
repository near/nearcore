//! Tools for creating a genesis block.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use borsh::BorshSerialize;
use indicatif::{ProgressBar, ProgressStyle};
use tempdir::TempDir;

use near::{get_store_path, NightshadeRuntime};
use near_chain::{Block, Chain, ChainStore, RuntimeAdapter, Tip};
use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::AccessKey;
use near_primitives::block::genesis_chunks;
use near_primitives::contract::ContractCode;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, Balance, ChunkExtra, EpochId, ShardId, StateRoot};
use near_primitives::views::AccountView;
use near_store::{
    create_store, get_account, set_access_key, set_account, set_code, ColState, Store, TrieUpdate,
};

fn get_account_id(account_index: u64) -> String {
    format!("near_{}_{}", account_index, account_index)
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct GenesisBuilder {
    home_dir: PathBuf,
    // We hold this temporary directory to avoid deletion through deallocation.
    #[allow(dead_code)]
    tmpdir: TempDir,
    genesis: Arc<Genesis>,
    store: Arc<Store>,
    runtime: NightshadeRuntime,
    unflushed_records: BTreeMap<ShardId, Vec<StateRecord>>,
    roots: BTreeMap<ShardId, StateRoot>,
    state_updates: BTreeMap<ShardId, TrieUpdate>,

    // Things that can be set.
    additional_accounts_num: u64,
    additional_accounts_code: Option<Vec<u8>>,
    additional_accounts_code_base64: Option<String>,
    additional_accounts_code_hash: CryptoHash,

    print_progress: bool,
}

impl GenesisBuilder {
    pub fn from_config_and_store(
        home_dir: &Path,
        genesis: Arc<Genesis>,
        store: Arc<Store>,
    ) -> Self {
        let tmpdir = TempDir::new("storage").unwrap();
        let runtime = NightshadeRuntime::new(
            tmpdir.path(),
            store.clone(),
            Arc::clone(&genesis),
            // Since we are not using runtime as an actor
            // there is no reason to track accounts or shards.
            vec![],
            vec![],
        );
        Self {
            home_dir: home_dir.to_path_buf(),
            tmpdir,
            genesis,
            store,
            runtime,
            unflushed_records: Default::default(),
            roots: Default::default(),
            state_updates: Default::default(),
            additional_accounts_num: 0,
            additional_accounts_code: None,
            additional_accounts_code_base64: None,
            additional_accounts_code_hash: CryptoHash::default(),
            print_progress: false,
        }
    }

    pub fn from_config(home_dir: &Path, genesis: Arc<Genesis>) -> Self {
        let store = create_store(&get_store_path(home_dir));
        Self::from_config_and_store(home_dir, genesis, store)
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
        self.additional_accounts_code_base64 = Some(to_base64(&contract_code));
        self.additional_accounts_code_hash = hash(&contract_code);
        self.additional_accounts_code = Some(contract_code);
        self
    }

    pub fn build(mut self) -> Result<Self> {
        // First, apply whatever is defined by the genesis config.
        let (_store, store_update, roots) = self.runtime.genesis_state();
        store_update.commit()?;
        self.roots = roots.into_iter().enumerate().map(|(k, v)| (k as u64, v)).collect();
        self.state_updates = self
            .roots
            .iter()
            .map(|(shard_idx, root)| {
                (*shard_idx, TrieUpdate::new(self.runtime.trie.clone(), *root))
            })
            .collect();
        self.unflushed_records =
            self.roots.keys().cloned().map(|shard_idx| (shard_idx, vec![])).collect();

        let total_accounts_num = self.additional_accounts_num * self.runtime.num_shards();
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

        for shard_id in 0..self.runtime.num_shards() {
            self.flush_shard_records(shard_id)?;
        }
        bar.finish();
        self.write_genesis_block()?;
        Ok(self)
    }

    pub fn dump_state(self) -> Result<Self> {
        let mut dump_path = self.home_dir.clone();
        dump_path.push("state_dump");
        self.store.save_to_file(ColState, dump_path.as_path())?;
        {
            let mut roots_files = self.home_dir.clone();
            roots_files.push("genesis_roots");
            let mut file = File::create(roots_files)?;
            let roots: Vec<_> = self.roots.values().cloned().collect();
            let data = roots.try_to_vec()?;
            file.write_all(&data)?;
        }
        Ok(self)
    }

    fn flush_shard_records(&mut self, shard_idx: ShardId) -> Result<()> {
        let records = self.unflushed_records.insert(shard_idx, vec![]).unwrap_or_default();
        if records.is_empty() {
            return Ok(());
        }
        let mut state_update =
            self.state_updates.remove(&shard_idx).expect("State updates are always available");

        // Compute storage usage and update accounts.
        for (account_id, storage_usage) in self.runtime.runtime.compute_storage_usage(&records) {
            let mut account =
                get_account(&state_update, &account_id)?.expect("We should've created account");
            account.storage_usage = storage_usage;
            set_account(&mut state_update, account_id, &account);
        }
        let trie = state_update.trie.clone();
        let (store_update, root) = state_update.finalize()?.0.into(trie)?;
        store_update.commit()?;

        self.roots.insert(shard_idx, root.clone());
        self.state_updates.insert(shard_idx, TrieUpdate::new(self.runtime.trie.clone(), root));
        Ok(())
    }

    fn write_genesis_block(&mut self) -> Result<()> {
        let genesis_chunks = genesis_chunks(
            self.roots.values().cloned().collect(),
            self.runtime.num_shards(),
            self.genesis.config.gas_limit,
            self.genesis.config.genesis_height,
        );
        let genesis = Block::genesis(
            genesis_chunks.into_iter().map(|chunk| chunk.header).collect(),
            self.genesis.config.genesis_time,
            self.genesis.config.genesis_height,
            self.genesis.config.min_gas_price,
            self.genesis.config.total_supply,
            Chain::compute_bp_hash(&self.runtime, EpochId::default(), &CryptoHash::default())?,
        );

        let mut store = ChainStore::new(self.store.clone(), self.genesis.config.genesis_height);
        let mut store_update = store.store_update();

        self.runtime
            .add_validator_proposals(
                CryptoHash::default(),
                genesis.hash(),
                genesis.header.inner_rest.random_value,
                genesis.header.inner_lite.height,
                0,
                vec![],
                vec![],
                vec![],
                0,
                self.genesis.config.total_supply.clone(),
            )
            .unwrap();
        store_update.save_block_header(genesis.header.clone());
        store_update.save_block(genesis.clone());

        for (chunk_header, state_root) in genesis.chunks.iter().zip(self.roots.values()) {
            store_update.save_chunk_extra(
                &genesis.hash(),
                chunk_header.inner.shard_id,
                ChunkExtra::new(
                    state_root,
                    CryptoHash::default(),
                    vec![],
                    0,
                    self.genesis.config.gas_limit.clone(),
                    0,
                    0,
                ),
            );
        }

        let head = Tip::from_header(&genesis.header);
        store_update.save_head(&head).unwrap();
        store_update.save_sync_head(&head);
        store_update.commit().unwrap();

        Ok(())
    }

    fn add_additional_account(&mut self, account_id: AccountId) -> Result<()> {
        let testing_init_balance: Balance = 10u128.pow(30);
        let testing_init_stake: Balance = 0;
        let shard_id = self.runtime.account_id_to_shard_id(&account_id);
        let mut records = self.unflushed_records.remove(&shard_id).unwrap_or_default();
        let mut state_update =
            self.state_updates.remove(&shard_id).expect("State update should have been added");

        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let account = AccountView {
            amount: testing_init_balance,
            locked: testing_init_stake,
            code_hash: self.additional_accounts_code_hash.clone(),
            storage_usage: 0,
            storage_paid_at: 0,
        };
        set_account(&mut state_update, account_id.clone(), &account.clone().into());
        let account_record = StateRecord::Account { account_id: account_id.clone(), account };
        records.push(account_record);
        let access_key_record = StateRecord::AccessKey {
            account_id: account_id.clone(),
            public_key: signer.public_key.clone(),
            access_key: AccessKey::full_access().into(),
        };
        set_access_key(
            &mut state_update,
            account_id.clone(),
            signer.public_key.clone(),
            &AccessKey::full_access(),
        );
        records.push(access_key_record);
        if let (Some(wasm_binary), Some(wasm_binary_base64)) =
            (self.additional_accounts_code.as_ref(), self.additional_accounts_code_base64.as_ref())
        {
            let code = ContractCode::new(wasm_binary.to_vec());
            set_code(&mut state_update, account_id.clone(), &code);
            let contract_record =
                StateRecord::Contract { account_id, code: wasm_binary_base64.clone() };
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
