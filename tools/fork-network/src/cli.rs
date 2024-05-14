use crate::single_shard_storage_mutator::SingleShardStorageMutator;
use crate::storage_mutator::StorageMutator;
use crate::trim_database;
use anyhow::Context;
use chrono::{DateTime, Utc};
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::{Genesis, GenesisConfig, GenesisValidationMode, NEAR_BASE};
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_mirror::key_mapping::{map_account, map_key};
use near_o11y::default_subscriber_with_opentelemetry;
use near_o11y::env_filter::make_env_filter;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::account::id::AccountType;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::dec_format;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_account_key;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, EpochId, NumBlocks, ShardId, StateRoot,
};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_store::db::RocksDB;
use near_store::flat::{store_helper, BlockInfo, FlatStorageManager, FlatStorageStatus};
use near_store::{
    checkpoint_hot_storage_and_cleanup_columns, DBCol, Store, TrieDBStorage, TrieStorage,
    FINAL_HEAD_KEY,
};
use nearcore::{load_config, open_storage, NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(clap::Parser)]
/// Use the following sub-commands:
/// * init
/// * amend-access-keys
/// * set-validators
/// * finalize
///
/// If something goes wrong, use the sub-command reset and start over.
pub struct ForkNetworkCommand {
    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(clap::Subcommand)]
enum SubCommand {
    /// Prepares the DB for doing the modifications:
    /// * Makes a snapshot.
    /// * Finds and persists state roots.
    Init(InitCmd),

    /// Creates a DB snapshot, then
    /// Updates the state to ensure every account has a full access key that is known to us.
    AmendAccessKeys(AmendAccessKeysCmd),

    /// Creates a DB snapshot, then
    /// Reads a list of validator accounts from a file
    /// Adds validator accounts to the state
    /// Creates a genesis file with the new validators.
    SetValidators(SetValidatorsCmd),

    /// Drops unneeded columns.
    Finalize(FinalizeCmd),

    /// Recovers from a snapshot.
    /// Deletes the snapshot.
    Reset(ResetCmd),
}

#[derive(clap::Parser)]
struct InitCmd;

#[derive(clap::Parser)]
struct FinalizeCmd;

#[derive(clap::Parser)]
struct AmendAccessKeysCmd {
    #[arg(short, long, default_value = "2000000")]
    batch_size: u64,
}

#[derive(clap::Parser)]
struct SetValidatorsCmd {
    /// Path to the JSON list of [`Validator`] structs containing account id and public keys.
    /// The path can be relative to `home_dir` or an absolute path.
    /// Example of a valid file that sets one validator with 50k tokens:
    /// [{
    ///   "account_id": "validator0",
    ///   "public_key": "ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX",
    ///   "amount": 50000000000000000000000000000
    /// }]
    #[arg(short, long)]
    pub validators: PathBuf,
    #[arg(short, long, default_value = "1000")]
    pub epoch_length: NumBlocks,
    #[arg(long, default_value = "-fork", allow_hyphen_values = true)]
    pub chain_id_suffix: String,
    /// Timestamp that should be set in the genesis block. This is required if you want
    /// to create a consistent forked network across many machines
    #[arg(long)]
    pub genesis_time: Option<DateTime<Utc>>,
    /// Genesis protocol version. If not present, the protocol version of the current epoch
    /// will be used.
    #[arg(long)]
    pub protocol_version: Option<ProtocolVersion>,
}

#[derive(clap::Parser)]
struct ResetCmd;

#[derive(Deserialize)]
struct Validator {
    account_id: AccountId,
    public_key: PublicKey,
    #[serde(with = "dec_format")]
    amount: Option<Balance>,
}

type MakeSingleShardStorageMutatorFn =
    Arc<dyn Fn(StateRoot) -> anyhow::Result<SingleShardStorageMutator> + Send + Sync>;

impl ForkNetworkCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
        verbose_target: Option<&str>,
        o11y_opts: &near_o11y::Options,
    ) -> anyhow::Result<()> {
        // Load config and check flat storage param
        let mut near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {e:#}"));

        let sys = actix::System::new();
        sys.block_on(async move {
            self.run_impl(&mut near_config, verbose_target, o11y_opts, home_dir).await.unwrap();
            actix::System::current().stop();
        });
        sys.run().unwrap();
        tracing::info!("Waiting for RocksDB to gracefully shutdown");
        RocksDB::block_until_all_instances_are_dropped();
        tracing::info!("exit");
        Ok(())
    }

    async fn run_impl(
        self,
        near_config: &mut NearConfig,
        verbose_target: Option<&str>,
        o11y_opts: &near_o11y::Options,
        home_dir: &Path,
    ) -> anyhow::Result<()> {
        // As we're running multiple threads, we need to initialize the logging
        // system to handle logging from all threads.
        let _subscriber_guard = default_subscriber_with_opentelemetry(
            make_env_filter(verbose_target).unwrap(),
            o11y_opts,
            near_config.client_config.chain_id.clone(),
            near_config.network_config.node_key.public_key().clone(),
            None,
        )
        .await
        .global();

        near_config.config.store.state_snapshot_enabled = false;

        match &self.command {
            SubCommand::Init(InitCmd) => {
                self.init(near_config, home_dir)?;
            }
            SubCommand::AmendAccessKeys(AmendAccessKeysCmd { batch_size }) => {
                self.amend_access_keys(*batch_size, near_config, home_dir)?;
            }
            SubCommand::SetValidators(SetValidatorsCmd {
                genesis_time,
                protocol_version,
                validators,
                epoch_length,
                chain_id_suffix,
            }) => {
                self.set_validators(
                    genesis_time.unwrap_or_else(chrono::Utc::now),
                    *protocol_version,
                    validators,
                    *epoch_length,
                    chain_id_suffix,
                    near_config,
                    home_dir,
                )?;
            }
            SubCommand::Finalize(FinalizeCmd) => {
                self.finalize(near_config, home_dir)?;
            }
            SubCommand::Reset(ResetCmd) => {
                self.reset(near_config, home_dir)?;
            }
        };
        Ok(())
    }

    /// Checks if a DB snapshot exists.
    /// If a snapshot doesn't exist, then creates it at `~/.near/data/fork-snapshot`.
    fn snapshot_db(
        &self,
        store: Store,
        near_config: &NearConfig,
        home_dir: &Path,
    ) -> anyhow::Result<bool> {
        let store_path = home_dir
            .join(near_config.config.store.path.clone().unwrap_or_else(|| PathBuf::from("data")));
        let fork_snapshot_path = store_path.join("fork-snapshot");

        if fork_snapshot_path.exists() && fork_snapshot_path.is_dir() {
            tracing::info!(?fork_snapshot_path, "Found a DB snapshot");
            Ok(false)
        } else {
            tracing::info!(destination = ?fork_snapshot_path, "Creating snapshot of original DB");
            // checkpointing only hot storage, because cold storage will not be changed
            checkpoint_hot_storage_and_cleanup_columns(&store, &fork_snapshot_path, None)?;
            Ok(true)
        }
    }

    // Snapshots the DB.
    // Determines parameters that will be used to initialize the new chain.
    // After this completes, almost every DB column can be removed, however this command doesn't delete anything itself.
    fn init(&self, near_config: &mut NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        // Open storage with migration
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();
        assert!(self.snapshot_db(store.clone(), near_config, home_dir)?);

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        let all_shard_uids: Vec<_> = shard_layout.shard_uids().collect();
        let num_shards = all_shard_uids.len();
        // Flat state can be at different heights for different shards.
        // That is fine, we'll simply lookup state root for each .
        let fork_heads = get_fork_heads(&all_shard_uids, store.clone())?;
        tracing::info!(?fork_heads);

        let chain =
            ChainStore::new(store.clone(), near_config.genesis.config.genesis_height, false);

        // Move flat storage to the max height for consistency across shards.
        let (block_height, desired_block_hash) =
            fork_heads.iter().map(|head| (head.height, head.hash)).max().unwrap();

        let desired_block_header = chain.get_block_header(&desired_block_hash)?;
        let epoch_id = desired_block_header.epoch_id();
        let flat_storage_manager = FlatStorageManager::new(store.clone());

        // Advance flat heads to the same (max) block height to ensure
        // consistency of state across the shards.
        let state_roots: Vec<StateRoot> = (0..num_shards)
            .map(|shard_id| {
                let shard_uid =
                    epoch_manager.shard_id_to_uid(shard_id as ShardId, epoch_id).unwrap();
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
                let flat_storage =
                    flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
                flat_storage.update_flat_head(&desired_block_hash, true).unwrap();
                let chunk_extra = chain.get_chunk_extra(&desired_block_hash, &shard_uid).unwrap();
                let state_root = chunk_extra.state_root();
                tracing::info!(?shard_id, ?epoch_id, ?state_root);
                *state_root
            })
            .collect();

        // Increment height to represent that some changes were made to the original state.
        tracing::info!(
            block_height,
            ?desired_block_hash,
            ?state_roots,
            ?epoch_id,
            "Moved flat heads to a common block"
        );
        let block_height = block_height + 1;

        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::Misc, b"FORK_TOOL_EPOCH_ID", epoch_id)?;
        store_update.set_ser(DBCol::Misc, b"FORK_TOOL_BLOCK_HASH", &desired_block_hash)?;
        store_update.set(DBCol::Misc, b"FORK_TOOL_BLOCK_HEIGHT", &block_height.to_le_bytes());
        for (shard_id, state_root) in state_roots.iter().enumerate() {
            store_update.set_ser(
                DBCol::Misc,
                format!("FORK_TOOL_SHARD_ID:{shard_id}").as_bytes(),
                state_root,
            )?;
        }
        store_update.commit()?;
        Ok(())
    }

    /// Creates a DB snapshot, then
    /// Updates the state to ensure every account has a full access key that is known to us.
    fn amend_access_keys(
        &self,
        batch_size: u64,
        near_config: &mut NearConfig,
        home_dir: &Path,
    ) -> anyhow::Result<Vec<StateRoot>> {
        // Open storage with migration
        near_config.config.store.load_mem_tries_for_tracked_shards = true;
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();

        let (prev_state_roots, prev_hash, epoch_id, block_height) =
            self.get_state_roots_and_hash(store.clone())?;
        tracing::info!(?prev_state_roots, ?epoch_id, ?prev_hash);

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let num_shards = prev_state_roots.len();
        let all_shard_uids: Vec<ShardUId> = (0..num_shards)
            .map(|shard_id| epoch_manager.shard_id_to_uid(shard_id as ShardId, &epoch_id).unwrap())
            .collect();
        let runtime =
            NightshadeRuntime::from_config(home_dir, store.clone(), &near_config, epoch_manager)
                .context("could not create the transaction runtime")?;
        runtime.get_tries().load_mem_tries_for_enabled_shards(&all_shard_uids).unwrap();

        let make_storage_mutator: MakeSingleShardStorageMutatorFn =
            Arc::new(move |prev_state_root| {
                SingleShardStorageMutator::new(&runtime.clone(), prev_state_root)
            });

        let new_state_roots = self.prepare_state(
            batch_size,
            &all_shard_uids,
            store,
            &prev_state_roots,
            block_height,
            make_storage_mutator.clone(),
        )?;
        Ok(new_state_roots)
    }

    /// Creates a DB snapshot, then
    /// Reads a list of validator accounts from a file
    /// Adds validator accounts to the state
    /// Creates a genesis file with the new validators.
    fn set_validators(
        &self,
        genesis_time: DateTime<Utc>,
        protocol_version: Option<ProtocolVersion>,
        validators: &Path,
        epoch_length: u64,
        chain_id_suffix: &str,
        near_config: &mut NearConfig,
        home_dir: &Path,
    ) -> anyhow::Result<(Vec<StateRoot>, Vec<AccountInfo>)> {
        // Open storage with migration
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();

        let (prev_state_roots, _prev_hash, epoch_id, block_height) =
            self.get_state_roots_and_hash(store.clone())?;

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);

        let runtime =
            NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager.clone())
                .context("could not create the transaction runtime")?;

        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);

        let storage_mutator = StorageMutator::new(
            epoch_manager.clone(),
            &runtime,
            epoch_id.clone(),
            prev_state_roots,
        )?;
        let (new_state_roots, new_validator_accounts) =
            self.add_validator_accounts(validators, runtime_config, home_dir, storage_mutator)?;

        tracing::info!("Creating a new genesis");
        backup_genesis_file(home_dir, &near_config)?;
        self.make_and_write_genesis(
            genesis_time,
            protocol_version,
            epoch_length,
            block_height,
            chain_id_suffix,
            &epoch_id,
            new_state_roots.clone(),
            new_validator_accounts.clone(),
            epoch_manager,
            home_dir,
            &near_config,
        )?;

        tracing::info!("All Done! Run the node normally to start the forked network.");
        Ok((new_state_roots, new_validator_accounts))
    }

    /// Deletes DB columns that are not needed in the new chain.
    fn finalize(&self, near_config: &mut NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        // Open storage with migration
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();

        let store_path = home_dir
            .join(near_config.config.store.path.clone().unwrap_or_else(|| PathBuf::from("data")));
        let temp_store_home = store_path.join("temp-trimmed-db");
        if std::fs::metadata(&temp_store_home).is_ok() {
            panic!("Temp trimmed DB already exists; please delete temp-trimmed-db in the data directory first");
        }
        let temp_storage = open_storage(&temp_store_home, near_config).unwrap();
        let temp_store = temp_storage.get_hot_store();
        let temp_store_path = temp_store_home.join("data");

        trim_database::trim_database(store, &near_config.genesis.config, temp_store)?;

        tracing::info!("Removing all current data");
        for entry in std::fs::read_dir(&store_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(&entry.path())?;
            }
        }
        tracing::info!("Moving in the new database");
        for entry in std::fs::read_dir(&temp_store_path)? {
            let entry = entry?;
            std::fs::rename(&entry.path(), &store_path.join(entry.file_name()))?;
        }
        std::fs::remove_dir(&temp_store_path)?;
        std::fs::remove_dir(&temp_store_home)?;

        Ok(())
    }

    fn get_state_roots_and_hash(
        &self,
        store: Store,
    ) -> anyhow::Result<(Vec<StateRoot>, CryptoHash, EpochId, BlockHeight)> {
        let epoch_id = EpochId(store.get_ser(DBCol::Misc, b"FORK_TOOL_EPOCH_ID")?.unwrap());
        let block_hash = store.get_ser(DBCol::Misc, b"FORK_TOOL_BLOCK_HASH")?.unwrap();
        let block_height = store.get(DBCol::Misc, b"FORK_TOOL_BLOCK_HEIGHT")?.unwrap();
        let block_height = u64::from_le_bytes(block_height.as_slice().try_into().unwrap());
        let mut state_roots = vec![];
        for (shard_id, item) in
            store.iter_prefix(DBCol::Misc, "FORK_TOOL_SHARD_ID:".as_bytes()).enumerate()
        {
            let (key, value) = item?;
            let key = String::from_utf8(key.to_vec())?;
            let state_root = borsh::from_slice(&value)?;
            assert_eq!(key, format!("FORK_TOOL_SHARD_ID:{shard_id}"));
            state_roots.push(state_root);
        }
        tracing::info!(?state_roots, ?block_hash, ?epoch_id, block_height);
        Ok((state_roots, block_hash, epoch_id, block_height))
    }

    /// Checks that `~/.near/data/fork-snapshot/data` exists.
    /// Deletes files (not directories) in `~/.near/data`
    /// Moves everything from `~/.near/data/fork-snapshot/data/` to `~/.near/data`.
    /// Deletes `~/.near/data/fork-snapshot/data`.
    /// Moves `~/.near/genesis.json.backup` to `~/.near/genesis.json`.
    fn reset(self, near_config: &NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        let store_path = home_dir
            .join(near_config.config.store.path.clone().unwrap_or_else(|| PathBuf::from("data")));
        // '/data' prefix comes from the use of `checkpoint_hot_storage_and_cleanup_columns` fn
        let fork_snapshot_path = store_path.join("fork-snapshot/data");
        if !Path::new(&fork_snapshot_path).exists() {
            panic!("Fork snapshot does not exist");
        }
        tracing::info!("Removing all current data");
        for entry in std::fs::read_dir(&store_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(&entry.path())?;
            }
        }
        tracing::info!("Restoring fork snapshot");
        for entry in std::fs::read_dir(&fork_snapshot_path)? {
            let entry = entry?;
            std::fs::rename(&entry.path(), &store_path.join(entry.file_name()))?;
        }
        std::fs::remove_dir(&fork_snapshot_path)?;

        tracing::info!("Restoring genesis file");
        restore_backup_genesis_file(home_dir, &near_config)?;
        tracing::info!("Reset complete");
        return Ok(());
    }

    fn prepare_shard_state(
        &self,
        batch_size: u64,
        shard_uid: ShardUId,
        store: Store,
        prev_state_root: StateRoot,
        block_height: BlockHeight,
        make_storage_mutator: MakeSingleShardStorageMutatorFn,
    ) -> anyhow::Result<StateRoot> {
        // Doesn't support secrets.
        tracing::info!(?shard_uid);
        let mut storage_mutator: SingleShardStorageMutator = make_storage_mutator(prev_state_root)?;

        // TODO: allow mutating the state with a secret, so this can be used to prepare a public test network
        let default_key = near_mirror::key_mapping::default_extra_key(None).public_key();
        // Keeps track of accounts that have a full access key.
        let mut has_full_key = HashSet::new();
        // Lets us lookup large values in the `State` columns.
        let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);

        // Iterate over the whole flat storage and do the necessary changes to have access to all accounts.
        let mut index_delayed_receipt = 0;
        let mut ref_keys_retrieved = 0;
        let mut records_not_parsed = 0;
        let mut records_parsed = 0;
        let mut access_keys_updated = 0;
        let mut accounts_implicit_updated = 0;
        let mut contract_data_updated = 0;
        let mut contract_code_updated = 0;
        let mut postponed_receipts_updated = 0;
        let mut received_data_updated = 0;
        let mut fake_block_height = block_height + 1;
        for item in store_helper::iter_flat_state_entries(shard_uid, &store, None, None) {
            let (key, value) = match item {
                Ok((key, FlatStateValue::Ref(ref_value))) => {
                    ref_keys_retrieved += 1;
                    (key, trie_storage.retrieve_raw_bytes(&ref_value.hash)?.to_vec())
                }
                Ok((key, FlatStateValue::Inlined(value))) => (key, value),
                otherwise => panic!("Unexpected flat state value: {otherwise:?}"),
            };
            if let Some(sr) = StateRecord::from_raw_key_value(key.clone(), value.clone()) {
                match sr {
                    StateRecord::AccessKey { account_id, public_key, access_key } => {
                        // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                        if account_id.get_account_type() != AccountType::NearImplicitAccount
                            && access_key.permission == AccessKeyPermission::FullAccess
                        {
                            has_full_key.insert(account_id.clone());
                        }
                        let new_account_id = map_account(&account_id, None);
                        let replacement = map_key(&public_key, None);
                        storage_mutator.delete_access_key(account_id, public_key)?;
                        storage_mutator.set_access_key(
                            new_account_id,
                            replacement.public_key(),
                            access_key.clone(),
                        )?;
                        access_keys_updated += 1;
                    }

                    StateRecord::Account { account_id, account } => {
                        // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                        if account_id.get_account_type() == AccountType::NearImplicitAccount {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_account(account_id)?;
                            storage_mutator.set_account(new_account_id, account)?;
                            accounts_implicit_updated += 1;
                        }
                    }
                    StateRecord::Data { account_id, data_key, value } => {
                        // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                        if account_id.get_account_type() == AccountType::NearImplicitAccount {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_data(account_id, &data_key)?;
                            storage_mutator.set_data(new_account_id, &data_key, value)?;
                            contract_data_updated += 1;
                        }
                    }
                    StateRecord::Contract { account_id, code } => {
                        // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                        if account_id.get_account_type() == AccountType::NearImplicitAccount {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_code(account_id)?;
                            storage_mutator.set_code(new_account_id, code)?;
                            contract_code_updated += 1;
                        }
                    }
                    StateRecord::PostponedReceipt(mut receipt) => {
                        storage_mutator.delete_postponed_receipt(&receipt)?;
                        near_mirror::genesis::map_receipt(&mut receipt, None, &default_key);
                        storage_mutator.set_postponed_receipt(&receipt)?;
                        postponed_receipts_updated += 1;
                    }
                    StateRecord::ReceivedData { account_id, data_id, data } => {
                        // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                        if account_id.get_account_type() == AccountType::NearImplicitAccount {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_received_data(account_id, data_id)?;
                            storage_mutator.set_received_data(new_account_id, data_id, &data)?;
                            received_data_updated += 1;
                        }
                    }
                    StateRecord::DelayedReceipt(mut receipt) => {
                        storage_mutator.delete_delayed_receipt(index_delayed_receipt)?;
                        near_mirror::genesis::map_receipt(&mut receipt, None, &default_key);
                        storage_mutator.set_delayed_receipt(index_delayed_receipt, &receipt)?;
                        index_delayed_receipt += 1;
                    }
                }
                records_parsed += 1;
            } else {
                records_not_parsed += 1;
            }
            if storage_mutator.should_commit(batch_size) {
                tracing::info!(
                    ?shard_uid,
                    ref_keys_retrieved,
                    records_parsed,
                    updated = access_keys_updated
                        + accounts_implicit_updated
                        + contract_data_updated
                        + contract_code_updated
                        + postponed_receipts_updated
                        + index_delayed_receipt
                        + received_data_updated,
                );
                let state_root = storage_mutator.commit(&shard_uid, fake_block_height)?;
                fake_block_height += 1;
                storage_mutator = make_storage_mutator(state_root)?;
            }
        }

        tracing::info!(
            ?shard_uid,
            ref_keys_retrieved,
            records_parsed,
            records_not_parsed,
            accounts_implicit_updated,
            access_keys_updated,
            contract_code_updated,
            contract_data_updated,
            postponed_receipts_updated,
            delayed_receipts_updated = index_delayed_receipt,
            received_data_updated,
            num_has_full_key = has_full_key.len(),
            "Pass 1 done"
        );

        // Now do another pass to ensure all accounts have full access keys.
        // Remember that we kept track of accounts with full access keys in `has_full_key`.
        // Iterating over the whole flat state is very fast compared to writing all the updates.
        let mut num_added = 0;
        let mut num_accounts = 0;
        for item in store_helper::iter_flat_state_entries(shard_uid, &store, None, None) {
            if let Ok((key, _)) = item {
                if key[0] == col::ACCOUNT {
                    num_accounts += 1;
                    let account_id = match parse_account_id_from_account_key(&key) {
                        Ok(account_id) => account_id,
                        Err(err) => {
                            tracing::error!(
                                ?err,
                                "Failed to parse account id {}",
                                hex::encode(&key)
                            );
                            continue;
                        }
                    };
                    if has_full_key.contains(&account_id) {
                        continue;
                    }
                    storage_mutator.set_access_key(
                        account_id,
                        default_key.clone(),
                        AccessKey::full_access(),
                    )?;
                    num_added += 1;
                    if storage_mutator.should_commit(batch_size) {
                        let state_root = storage_mutator.commit(&shard_uid, fake_block_height)?;
                        fake_block_height += 1;
                        storage_mutator = make_storage_mutator(state_root)?;
                    }
                }
            }
        }
        tracing::info!(?shard_uid, num_accounts, num_added, "Pass 2 done");

        let state_root = storage_mutator.commit(&shard_uid, fake_block_height)?;

        tracing::info!(?shard_uid, "Commit done");
        Ok(state_root)
    }

    fn prepare_state(
        &self,
        batch_size: u64,
        all_shard_uids: &[ShardUId],
        store: Store,
        prev_state_roots: &[StateRoot],
        block_height: BlockHeight,
        make_storage_mutator: MakeSingleShardStorageMutatorFn,
    ) -> anyhow::Result<Vec<StateRoot>> {
        let state_roots = all_shard_uids
            .into_par_iter()
            .map(|shard_uid| {
                let state_root = self
                    .prepare_shard_state(
                        batch_size,
                        *shard_uid,
                        store.clone(),
                        prev_state_roots[shard_uid.shard_id as usize],
                        block_height,
                        make_storage_mutator.clone(),
                    )
                    .unwrap();
                state_root
            })
            .collect();
        tracing::info!(?state_roots, "All done");
        Ok(state_roots)
    }

    /// Reads the validators file (which is a path relative to the home dir),
    /// and adds new accounts and new keys for the specified accounts.
    fn add_validator_accounts(
        &self,
        validators: &Path,
        runtime_config: &Arc<RuntimeConfig>,
        home_dir: &Path,
        mut storage_mutator: StorageMutator,
    ) -> anyhow::Result<(Vec<StateRoot>, Vec<AccountInfo>)> {
        let mut new_validator_accounts = vec![];

        let liquid_balance = 100_000_000 * NEAR_BASE;
        let storage_bytes = runtime_config.fees.storage_usage_config.num_bytes_account;
        let validators_path = if validators.is_absolute() {
            PathBuf::from(validators)
        } else {
            home_dir.join(&validators)
        };
        let file = File::open(&validators_path)
            .expect("Failed to open the validators JSON {validators_path:?}");
        let new_validators: Vec<Validator> = serde_json::from_reader(BufReader::new(file))
            .expect("Failed to read validators JSON {validators_path:?}");
        for validator in new_validators.into_iter() {
            let validator_account = AccountInfo {
                account_id: validator.account_id,
                amount: validator.amount.unwrap_or(50_000 * NEAR_BASE),
                public_key: validator.public_key,
            };
            new_validator_accounts.push(validator_account.clone());
            storage_mutator.set_account(
                &validator_account.account_id,
                Account::new(
                    liquid_balance,
                    validator_account.amount,
                    0,
                    CryptoHash::default(),
                    storage_bytes,
                    PROTOCOL_VERSION,
                ),
            )?;
            storage_mutator.set_access_key(
                &validator_account.account_id,
                validator_account.public_key,
                AccessKey::full_access(),
            )?;
        }
        let new_state_roots = storage_mutator.commit()?;
        Ok((new_state_roots, new_validator_accounts))
    }

    /// Makes a new genesis and writes it to `~/.near/genesis.json`.
    fn make_and_write_genesis(
        &self,
        genesis_time: DateTime<Utc>,
        protocol_version: Option<ProtocolVersion>,
        epoch_length: u64,
        height: BlockHeight,
        chain_id_suffix: &str,
        epoch_id: &EpochId,
        new_state_roots: Vec<StateRoot>,
        new_validator_accounts: Vec<AccountInfo>,
        epoch_manager: Arc<EpochManagerHandle>,
        home_dir: &Path,
        near_config: &NearConfig,
    ) -> anyhow::Result<()> {
        let epoch_config = epoch_manager.get_epoch_config(epoch_id)?;
        let protocol_version = match protocol_version {
            Some(v) => v,
            None => {
                let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
                epoch_info.protocol_version()
            }
        };
        let original_config = near_config.genesis.config.clone();

        let new_config = GenesisConfig {
            chain_id: original_config.chain_id.clone() + chain_id_suffix,
            genesis_height: height,
            genesis_time,
            epoch_length,
            num_block_producer_seats: epoch_config.num_block_producer_seats,
            num_block_producer_seats_per_shard: epoch_config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: epoch_config.avg_hidden_validator_seats_per_shard,
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            max_kickout_stake_perc: 0,
            online_min_threshold: epoch_config.online_min_threshold,
            online_max_threshold: epoch_config.online_max_threshold,
            fishermen_threshold: epoch_config.fishermen_threshold,
            minimum_stake_divisor: epoch_config.minimum_stake_divisor,
            protocol_upgrade_stake_threshold: epoch_config.protocol_upgrade_stake_threshold,
            shard_layout: epoch_config.shard_layout.clone(),
            num_chunk_only_producer_seats: epoch_config
                .validator_selection_config
                .num_chunk_only_producer_seats,
            minimum_validators_per_shard: epoch_config
                .validator_selection_config
                .minimum_validators_per_shard,
            minimum_stake_ratio: epoch_config.validator_selection_config.minimum_stake_ratio,
            shuffle_shard_assignment_for_chunk_producers: epoch_config
                .validator_selection_config
                .shuffle_shard_assignment_for_chunk_producers,
            dynamic_resharding: false,
            protocol_version,
            validators: new_validator_accounts,
            gas_price_adjustment_rate: original_config.gas_price_adjustment_rate,
            gas_limit: original_config.gas_limit,
            max_gas_price: original_config.max_gas_price,
            max_inflation_rate: original_config.max_inflation_rate,
            min_gas_price: original_config.min_gas_price,
            num_blocks_per_year: original_config.num_blocks_per_year,
            protocol_reward_rate: original_config.protocol_reward_rate,
            protocol_treasury_account: original_config.protocol_treasury_account.clone(),
            total_supply: original_config.total_supply,
            transaction_validity_period: original_config.transaction_validity_period,
            use_production_config: original_config.use_production_config,
        };

        let genesis = Genesis::new_from_state_roots(new_config, new_state_roots);
        let genesis_file = &near_config.config.genesis_file;
        let original_genesis_file = home_dir.join(&genesis_file);

        tracing::info!(?original_genesis_file, "Writing new genesis");
        genesis.to_file(&original_genesis_file);

        Ok(())
    }
}

fn backup_genesis_file_path(home_dir: &Path, genesis_file: &str) -> PathBuf {
    home_dir.join(format!("{}.backup", &genesis_file))
}

/// Returns hash of flat head.
/// Checks that all shards have flat storage.
/// Checks that flat heads of all shards match.
fn get_fork_heads(all_shard_uids: &[ShardUId], store: Store) -> anyhow::Result<Vec<BlockInfo>> {
    // Iterate over each shard to check that flat storage is Ready.
    let flat_heads :Vec<BlockInfo> = all_shard_uids.iter().map(|shard_uid|{
        let flat_storage_status = store
            .get_ser::<FlatStorageStatus>(DBCol::FlatStorageStatus, &shard_uid.to_bytes()).unwrap()
            .unwrap();
        if let FlatStorageStatus::Ready(ready) = &flat_storage_status {
            ready.flat_head
        } else {
            panic!("Flat storage is not ready for shard {shard_uid}: {flat_storage_status:?}. Please reset the fork, and run the node for longer");
        }
    }).collect();
    Ok(flat_heads)
}

fn backup_genesis_file(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<()> {
    let genesis_file = &near_config.config.genesis_file;
    let original_genesis_file = home_dir.join(&genesis_file);
    let backup_genesis_file = backup_genesis_file_path(home_dir, &genesis_file);
    tracing::info!(?original_genesis_file, ?backup_genesis_file, "Backing up old genesis.");
    std::fs::rename(&original_genesis_file, &backup_genesis_file)?;
    Ok(())
}

fn restore_backup_genesis_file(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<()> {
    let genesis_file = &near_config.config.genesis_file;
    let backup_genesis_file = backup_genesis_file_path(home_dir, &genesis_file);
    let original_genesis_file = home_dir.join(&genesis_file);
    tracing::info!(?backup_genesis_file, ?original_genesis_file, "Restoring genesis from a backup");
    std::fs::rename(&backup_genesis_file, &original_genesis_file)?;
    Ok(())
}
