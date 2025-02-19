use crate::delayed_receipts::DelayedReceiptTracker;
use crate::storage_mutator::{ShardUpdateState, StorageMutator};
use anyhow::Context;
use chrono::{DateTime, Utc};
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::{Genesis, GenesisConfig, GenesisValidationMode, NEAR_BASE};
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_mirror::key_mapping::{map_account, map_key};
use near_o11y::default_subscriber_with_opentelemetry;
use near_o11y::env_filter::make_env_filter;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::account::id::AccountType;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account, AccountContract};
use near_primitives::borsh;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::serialize::dec_format;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::FlatStateValue;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_account_key;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, EpochId, NumBlocks, NumSeats, StateRoot,
};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_store::adapter::StoreAdapter;
use near_store::db::RocksDB;
use near_store::flat::{BlockInfo, FlatStorageManager, FlatStorageStatus};
use near_store::{
    checkpoint_hot_storage_and_cleanup_columns, DBCol, Store, TrieDBStorage, TrieStorage,
    FINAL_HEAD_KEY,
};
use nearcore::{load_config, open_storage, NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use strum::IntoEnumIterator;

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
struct InitCmd {
    /// If given, the shard layout in this file will be used to generate the forked genesis state
    #[arg(long)]
    pub shard_layout_file: Option<PathBuf>,
}

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
    /// Sets the chain id to this value. By default, if this value is not set, the initial chain_id
    /// will be reused by appending the suffix.
    #[arg(long)]
    pub chain_id: Option<String>,
    /// Timestamp that should be set in the genesis block. This is required if you want
    /// to create a consistent forked network across many machines
    #[arg(long)]
    pub genesis_time: Option<DateTime<Utc>>,
    /// Genesis protocol version. If not present, the protocol version of the current epoch
    /// will be used.
    #[arg(long)]
    pub protocol_version: Option<ProtocolVersion>,
    /// Number of validator seats.
    #[clap(long)]
    pub num_seats: Option<NumSeats>,
}

const FORKED_ROOTS_KEY_PREFIX: &[u8; 20] = b"FORK_TOOL_SHARD_UID:";

fn parse_state_roots_key(key: &[u8]) -> anyhow::Result<ShardUId> {
    // Sanity check assertion since we should be iterating based on this prefix
    assert!(key.starts_with(FORKED_ROOTS_KEY_PREFIX));
    let shard_uid_part = &key[FORKED_ROOTS_KEY_PREFIX.len()..];
    borsh::from_slice(shard_uid_part)
        .with_context(|| format!("Failed parsing ShardUId from fork tool key {:?}", key))
}

pub(crate) fn make_state_roots_key(shard_uid: ShardUId) -> Vec<u8> {
    let mut key = FORKED_ROOTS_KEY_PREFIX.to_vec();
    key.append(&mut borsh::to_vec(&shard_uid).unwrap());
    key
}

/// The minimum set of columns that will be needed to start a node after the `finalize` command runs
const COLUMNS_TO_KEEP: &[DBCol] = &[DBCol::DbVersion, DBCol::Misc, DBCol::State, DBCol::FlatState];

/// Extra columns needed in the setup before the `finalize` command
const SETUP_COLUMNS_TO_KEEP: &[DBCol] =
    &[DBCol::EpochInfo, DBCol::FlatStorageStatus, DBCol::ChunkExtra];

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
    Arc<dyn Fn(Vec<ShardUpdateState>) -> anyhow::Result<StorageMutator> + Send + Sync>;

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
            SubCommand::Init(InitCmd { shard_layout_file }) => {
                self.init(near_config, home_dir, shard_layout_file.as_deref())?;
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
                chain_id,
                num_seats,
            }) => {
                self.set_validators(
                    genesis_time.unwrap_or_else(chrono::Utc::now),
                    *protocol_version,
                    validators,
                    *epoch_length,
                    num_seats,
                    chain_id_suffix,
                    chain_id,
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
    fn write_fork_info(
        &self,
        near_config: &mut NearConfig,
        home_dir: &Path,
        shard_layout_file: Option<&Path>,
    ) -> anyhow::Result<()> {
        // Open storage with migration
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();
        assert!(self.snapshot_db(store.clone(), near_config, home_dir)?);

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        let all_shard_uids: Vec<_> = shard_layout.shard_uids().collect();

        let target_shard_layout = match shard_layout_file {
            Some(shard_layout_file) => {
                let layout = std::fs::read_to_string(shard_layout_file).with_context(|| {
                    format!("failed reading shard layout file at {}", shard_layout_file.display())
                })?;
                serde_json::from_str(&layout).with_context(|| {
                    format!("failed parsing shard layout file at {}", shard_layout_file.display())
                })?
            }
            None => shard_layout,
        };

        // Flat state can be at different heights for different shards.
        // That is fine, we'll simply lookup state root for each .
        let fork_heads = get_fork_heads(&all_shard_uids, store.clone())?;
        tracing::info!(?fork_heads);

        let chain = ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        // Move flat storage to the max height for consistency across shards.
        let desired_flat_head = fork_heads.iter().max_by_key(|b| b.height).unwrap();

        let desired_block_header = chain.get_block_header(&desired_flat_head.hash)?;
        let epoch_id = desired_block_header.epoch_id();
        let flat_storage_manager = FlatStorageManager::new(store.flat_store());

        // Advance flat heads to the same (max) block height to ensure
        // consistency of state across the shards.
        let state_roots: Vec<(ShardUId, StateRoot)> = all_shard_uids
            .into_iter()
            .map(|shard_uid| {
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
                let flat_storage =
                    flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
                flat_storage.update_flat_head(&desired_flat_head.hash).unwrap();
                let chunk_extra =
                    chain.get_chunk_extra(&desired_flat_head.hash, &shard_uid).unwrap();
                let state_root = chunk_extra.state_root();
                tracing::info!(?shard_uid, ?epoch_id, ?state_root);
                (shard_uid, *state_root)
            })
            .collect();

        // Increment height to represent that some changes were made to the original state.
        tracing::info!(
            ?desired_flat_head,
            ?state_roots,
            ?epoch_id,
            "Moved flat heads to a common block"
        );

        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::Misc, b"FORK_TOOL_EPOCH_ID", epoch_id)?;
        store_update.set_ser(DBCol::Misc, b"FORK_TOOL_FLAT_HEAD", &desired_flat_head)?;
        store_update.set_ser(DBCol::Misc, b"FORK_TOOL_SHARD_LAYOUT", &target_shard_layout)?;
        for (shard_uid, state_root) in state_roots.iter() {
            store_update.set_ser(DBCol::Misc, &make_state_roots_key(*shard_uid), state_root)?;
        }
        store_update.commit()?;
        Ok(())
    }

    fn init(
        &self,
        near_config: &mut NearConfig,
        home_dir: &Path,
        shard_layout_file: Option<&Path>,
    ) -> anyhow::Result<()> {
        self.write_fork_info(near_config, home_dir, shard_layout_file)?;
        let mut unwanted_cols = Vec::new();
        for col in DBCol::iter() {
            if !COLUMNS_TO_KEEP.contains(&col) && !SETUP_COLUMNS_TO_KEEP.contains(&col) {
                unwanted_cols.push(col);
            }
        }
        near_store::clear_columns(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
            &unwanted_cols,
        )
        .context("failed deleting unwanted columns")?;
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
        near_config.config.store.load_memtries_for_tracked_shards = true;
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let (prev_state_roots, flat_head, epoch_id, target_shard_layout) =
            self.get_state_roots_and_hash(store.clone())?;
        tracing::info!(?prev_state_roots, ?epoch_id, ?flat_head);

        let source_shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let all_shard_uids = source_shard_layout.shard_uids().collect::<Vec<_>>();
        assert_eq!(all_shard_uids.len(), prev_state_roots.len());

        let runtime =
            NightshadeRuntime::from_config(home_dir, store.clone(), &near_config, epoch_manager)
                .context("could not create the transaction runtime")?;
        // TODO: add an option to not load them all at once. As is, this takes an insane amount of memory for mainnet state.
        runtime
            .get_tries()
            .load_memtries_for_enabled_shards(&all_shard_uids, &[].into(), true)
            .unwrap();

        let shard_tries = runtime.get_tries();
        let target_shard_layout2 = target_shard_layout.clone();
        let make_storage_mutator: MakeSingleShardStorageMutatorFn = Arc::new(move |update_state| {
            StorageMutator::new(shard_tries.clone(), update_state, target_shard_layout2.clone())
        });

        let new_state_roots = self.prepare_state(
            batch_size,
            store,
            source_shard_layout,
            target_shard_layout,
            flat_head,
            prev_state_roots,
            make_storage_mutator.clone(),
            runtime,
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
        num_seats: &Option<NumSeats>,
        chain_id_suffix: &str,
        chain_id: &Option<String>,
        near_config: &mut NearConfig,
        home_dir: &Path,
    ) -> anyhow::Result<(Vec<StateRoot>, Vec<AccountInfo>)> {
        // Open storage with migration
        let storage = open_storage(&home_dir, near_config).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        let (prev_state_roots, flat_head, _epoch_id, target_shard_layout) =
            self.get_state_roots_and_hash(store.clone())?;

        let runtime =
            NightshadeRuntime::from_config(home_dir, store.clone(), &near_config, epoch_manager)
                .context("could not create the transaction runtime")?;

        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);

        let shard_uids = target_shard_layout.shard_uids().collect::<Vec<_>>();
        assert_eq!(
            shard_uids.iter().collect::<HashSet<_>>(),
            prev_state_roots.iter().map(|(k, _v)| k).collect::<HashSet<_>>()
        );

        let flat_store = store.flat_store();
        // Here we use the same shard layout for source and target, because we assume that amend-access-keys has already
        // written the new shard layout to the FORK_TOOL_SHARD_LAYOUT key, and in any case we're not mapping things from
        // source shards to target shards in this function
        let update_state = ShardUpdateState::new_update_state(
            &flat_store,
            &target_shard_layout,
            &target_shard_layout,
            &prev_state_roots,
        )?;
        let storage_mutator = StorageMutator::new(
            runtime.get_tries(),
            update_state.clone(),
            target_shard_layout.clone(),
        )?;
        let new_validator_accounts = self.add_validator_accounts(
            validators,
            runtime_config,
            home_dir,
            &target_shard_layout,
            storage_mutator,
        )?;
        let new_state_roots = update_state.into_iter().map(|u| u.state_root()).collect::<Vec<_>>();
        tracing::info!("Creating a new genesis");
        backup_genesis_file(home_dir, &near_config)?;
        self.make_and_write_genesis(
            genesis_time,
            protocol_version,
            target_shard_layout,
            epoch_length,
            num_seats,
            flat_head.height + 1,
            chain_id_suffix,
            chain_id,
            new_state_roots.clone(),
            new_validator_accounts.clone(),
            home_dir,
            near_config,
        )?;

        tracing::info!("All Done! Run the node normally to start the forked network.");
        Ok((new_state_roots, new_validator_accounts))
    }

    /// Deletes DB columns that are not needed in the new chain.
    fn finalize(&self, near_config: &mut NearConfig, home_dir: &Path) -> anyhow::Result<()> {
        tracing::info!("Delete unneeded columns in the original DB");
        let mut unwanted_cols = Vec::new();
        for col in DBCol::iter() {
            if !COLUMNS_TO_KEEP.contains(&col) {
                unwanted_cols.push(col);
            }
        }
        near_store::clear_columns(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
            &unwanted_cols,
        )
        .context("failed deleting unwanted columns")?;
        Ok(())
    }

    // The Vec<StateRoot> returned is in ShardIndex order
    fn get_state_roots_and_hash(
        &self,
        store: Store,
    ) -> anyhow::Result<(HashMap<ShardUId, StateRoot>, BlockInfo, EpochId, ShardLayout)> {
        let epoch_id = EpochId(store.get_ser(DBCol::Misc, b"FORK_TOOL_EPOCH_ID")?.unwrap());
        let flat_head = store.get_ser(DBCol::Misc, b"FORK_TOOL_FLAT_HEAD")?.unwrap();
        let shard_layout = store.get_ser(DBCol::Misc, b"FORK_TOOL_SHARD_LAYOUT")?.unwrap();
        let mut state_roots = HashMap::new();
        for item in store.iter_prefix(DBCol::Misc, FORKED_ROOTS_KEY_PREFIX) {
            let (key, value) = item?;
            let shard_uid = parse_state_roots_key(&key)?;
            let state_root: StateRoot = borsh::from_slice(&value)?;

            state_roots.insert(shard_uid, state_root);
        }
        tracing::info!(?state_roots, ?flat_head, ?epoch_id);
        Ok((state_roots, flat_head, epoch_id, shard_layout))
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

    /// Creates epoch config overrides since `first_version` and places them
    /// in `home_dir`.
    fn override_epoch_configs(
        &self,
        first_version: ProtocolVersion,
        num_seats: &Option<NumSeats>,
        home_dir: &Path,
    ) -> anyhow::Result<EpochConfig> {
        let epoch_config_dir = home_dir.join("epoch_configs");
        if epoch_config_dir.exists() {
            std::fs::remove_dir_all(epoch_config_dir.clone())?;
        }
        std::fs::create_dir_all(epoch_config_dir.clone()).with_context(|| {
            anyhow::anyhow!("Failed to create directory {:?}", epoch_config_dir)
        })?;

        let base_epoch_config_store =
            EpochConfigStore::for_chain_id(near_primitives::chains::MAINNET, None)
                .expect("Could not load the EpochConfigStore for mainnet.");
        let mut new_epoch_configs = BTreeMap::new();
        for version in first_version..=PROTOCOL_VERSION {
            let mut config = base_epoch_config_store.get_config(version).as_ref().clone();
            if let Some(num_seats) = num_seats {
                config.num_block_producer_seats = *num_seats;
                config.num_chunk_producer_seats = *num_seats;
                config.num_chunk_validator_seats = *num_seats;
            }
            new_epoch_configs.insert(version, Arc::new(config));
        }
        let first_config = new_epoch_configs.get(&first_version).unwrap().as_ref().clone();
        let epoch_config_store = EpochConfigStore::test(new_epoch_configs);

        epoch_config_store.dump_epoch_configs_between(
            &first_version,
            &PROTOCOL_VERSION,
            epoch_config_dir.to_str().unwrap(),
        );
        tracing::info!(target: "near", "Generated epoch configs files in {}", epoch_config_dir.display());
        Ok(first_config)
    }

    /// Returns info on delayed receipts mapped from this shard, and this shard's state root after
    /// all updates are applied.
    fn prepare_shard_state(
        &self,
        batch_size: u64,
        source_shard_layout: ShardLayout,
        target_shard_layout: ShardLayout,
        shard_uid: ShardUId,
        store: Store,
        make_storage_mutator: MakeSingleShardStorageMutatorFn,
        update_state: Vec<ShardUpdateState>,
    ) -> anyhow::Result<DelayedReceiptTracker> {
        let mut storage_mutator: StorageMutator = make_storage_mutator(update_state.clone())?;

        // TODO: allow mutating the state with a secret, so this can be used to prepare a public test network
        let default_key = near_mirror::key_mapping::default_extra_key(None).public_key();
        // Keeps track of accounts that have a full access key.
        let mut has_full_key = HashSet::new();
        // Lets us lookup large values in the `State` columns.
        let trie_storage = TrieDBStorage::new(store.trie_store(), shard_uid);

        let mut receipts_tracker =
            DelayedReceiptTracker::new(shard_uid, target_shard_layout.shard_ids().count());

        // Iterate over the whole flat storage and do the necessary changes to have access to all accounts.
        let mut ref_keys_retrieved = 0;
        let mut records_not_parsed = 0;
        let mut records_parsed = 0;

        for item in store.flat_store().iter(shard_uid) {
            let (key, value) = match item {
                Ok((key, FlatStateValue::Ref(ref_value))) => {
                    ref_keys_retrieved += 1;
                    (key, trie_storage.retrieve_raw_bytes(&ref_value.hash)?.to_vec())
                }
                Ok((key, FlatStateValue::Inlined(value))) => (key, value),
                otherwise => panic!("Unexpected flat state value: {otherwise:?}"),
            };
            if let Some(sr) = StateRecord::from_raw_key_value(&key, value.clone()) {
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
                        let new_shard_id =
                            target_shard_layout.account_id_to_shard_id(&new_account_id);
                        let new_shard_idx =
                            target_shard_layout.get_shard_index(new_shard_id).unwrap();

                        storage_mutator.remove_access_key(shard_uid, account_id, public_key)?;
                        storage_mutator.set_access_key(
                            new_shard_idx,
                            new_account_id,
                            replacement.public_key(),
                            access_key.clone(),
                        )?;
                    }
                    StateRecord::Account { account_id, account } => {
                        storage_mutator.map_account(shard_uid, account_id, account)?;
                    }
                    StateRecord::Data { account_id, data_key, value } => {
                        storage_mutator.map_data(shard_uid, account_id, &data_key, value)?;
                    }
                    StateRecord::Contract { account_id, code } => {
                        storage_mutator.map_code(shard_uid, account_id, code)?;
                    }
                    StateRecord::PostponedReceipt(mut receipt) => {
                        storage_mutator.remove_postponed_receipt(
                            shard_uid,
                            receipt.receiver_id().clone(),
                            *receipt.receipt_id(),
                        )?;
                        near_mirror::genesis::map_receipt(&mut receipt, None, &default_key);

                        let new_shard_id =
                            target_shard_layout.account_id_to_shard_id(receipt.receiver_id());
                        let new_shard_idx =
                            target_shard_layout.get_shard_index(new_shard_id).unwrap();

                        storage_mutator.set_postponed_receipt(new_shard_idx, &receipt)?;
                    }
                    StateRecord::ReceivedData { account_id, data_id, data } => {
                        storage_mutator.map_received_data(shard_uid, account_id, data_id, &data)?;
                    }
                    StateRecord::DelayedReceipt(receipt) => {
                        let new_account_id = map_account(receipt.receipt.receiver_id(), None);
                        let new_shard_id =
                            target_shard_layout.account_id_to_shard_id(&new_account_id);
                        let new_shard_idx =
                            target_shard_layout.get_shard_index(new_shard_id).unwrap();

                        // The index is guaranteed to be set when iterating over the trie rather than reading
                        // serialized StateRecords
                        let index = receipt.index.unwrap();
                        receipts_tracker.push(new_shard_idx, index);
                    }
                }
                records_parsed += 1;
            } else {
                records_not_parsed += 1;
            }
            if storage_mutator.should_commit(batch_size) {
                tracing::info!(?shard_uid, ref_keys_retrieved, records_parsed,);
                storage_mutator.commit()?;
                storage_mutator = make_storage_mutator(update_state.clone())?;
            }
        }

        // Commit the remaining updates.
        if storage_mutator.should_commit(1) {
            tracing::info!(?shard_uid, ref_keys_retrieved, records_parsed,);
            storage_mutator.commit()?;
            storage_mutator = make_storage_mutator(update_state.clone())?;
        }

        tracing::info!(
            ?shard_uid,
            ref_keys_retrieved,
            records_parsed,
            records_not_parsed,
            num_has_full_key = has_full_key.len(),
            "Pass 1 done"
        );

        // Now do another pass to ensure all accounts have full access keys.
        // Remember that we kept track of accounts with full access keys in `has_full_key`.
        // Iterating over the whole flat state is very fast compared to writing all the updates.
        // TODO: Just remember what accounts we saw in the above iteration
        let mut num_added = 0;
        let mut num_accounts = 0;
        for item in store.flat_store().iter(shard_uid) {
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
                    if account_id.get_account_type() == AccountType::NearImplicitAccount
                        || has_full_key.contains(&account_id)
                    {
                        continue;
                    }
                    let shard_id = source_shard_layout.account_id_to_shard_id(&account_id);
                    if shard_id != shard_uid.shard_id() {
                        tracing::warn!(
                            "Account {} belongs to shard {} but was found in flat storage for shard {}",
                            &account_id, shard_id, shard_uid.shard_id(),
                        );
                    }
                    let shard_idx = source_shard_layout.get_shard_index(shard_id).unwrap();
                    storage_mutator.set_access_key(
                        shard_idx,
                        account_id,
                        default_key.clone(),
                        AccessKey::full_access(),
                    )?;
                    num_added += 1;
                    if storage_mutator.should_commit(batch_size) {
                        storage_mutator.commit()?;
                        storage_mutator = make_storage_mutator(update_state.clone())?;
                    }
                }
            }
        }
        tracing::info!(?shard_uid, num_accounts, num_added, "Pass 2 done");
        storage_mutator.commit()?;
        Ok(receipts_tracker)
    }

    // TODO: instead of calling this every time, this could be integrated into StorageMutator or something
    fn update_source_state_roots(
        source_state_roots: &mut HashMap<ShardUId, StateRoot>,
        target_shard_layout: &ShardLayout,
        update_state: &[ShardUpdateState],
    ) {
        for (shard_uid, state_root) in source_state_roots.iter_mut() {
            if target_shard_layout.shard_uids().any(|s| s == *shard_uid) {
                let shard_idx = target_shard_layout.get_shard_index(shard_uid.shard_id()).unwrap();
                *state_root = update_state[shard_idx].state_root();
            }
        }
    }

    fn prepare_state(
        &self,
        batch_size: u64,
        store: Store,
        source_shard_layout: ShardLayout,
        target_shard_layout: ShardLayout,
        flat_head: BlockInfo,
        mut source_state_roots: HashMap<ShardUId, StateRoot>,
        make_storage_mutator: MakeSingleShardStorageMutatorFn,
        runtime: Arc<NightshadeRuntime>,
    ) -> anyhow::Result<Vec<StateRoot>> {
        let shard_uids = source_shard_layout.shard_uids().collect::<Vec<_>>();
        assert_eq!(
            shard_uids.iter().collect::<HashSet<_>>(),
            source_state_roots.iter().map(|(k, _v)| k).collect::<HashSet<_>>()
        );

        let flat_store = store.flat_store();
        let update_state = ShardUpdateState::new_update_state(
            &flat_store,
            &source_shard_layout,
            &target_shard_layout,
            &source_state_roots,
        )?;

        // the try_fold().try_reduce() will give a Vec<> of the return values and return early if one fails
        let receipt_trackers = shard_uids
            .into_par_iter()
            .try_fold(
                || Vec::new(),
                |mut trackers, shard_uid| {
                    let t = self.prepare_shard_state(
                        batch_size,
                        source_shard_layout.clone(),
                        target_shard_layout.clone(),
                        shard_uid,
                        store.clone(),
                        make_storage_mutator.clone(),
                        update_state.clone(),
                    )?;
                    trackers.push(t);
                    anyhow::Ok(trackers)
                },
            )
            .try_reduce(
                || Vec::new(),
                |mut l, mut r| {
                    l.append(&mut r);
                    Ok(l)
                },
            )?;

        Self::update_source_state_roots(
            &mut source_state_roots,
            &target_shard_layout,
            &update_state,
        );
        let shard_tries = runtime.get_tries();
        crate::storage_mutator::write_bandwidth_scheduler_state(
            &shard_tries,
            &source_shard_layout,
            &target_shard_layout,
            &source_state_roots,
            &update_state,
        )?;

        let default_key = near_mirror::key_mapping::default_extra_key(None).public_key();
        Self::update_source_state_roots(
            &mut source_state_roots,
            &target_shard_layout,
            &update_state,
        );
        crate::delayed_receipts::write_delayed_receipts(
            &shard_tries,
            &update_state,
            receipt_trackers,
            &source_state_roots,
            &target_shard_layout,
            &default_key,
        )?;
        crate::storage_mutator::finalize_state(
            &shard_tries,
            &source_shard_layout,
            &target_shard_layout,
            flat_head,
        )?;

        let state_roots = update_state.into_iter().map(|u| u.state_root()).collect();
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
        shard_layout: &ShardLayout,
        mut storage_mutator: StorageMutator,
    ) -> anyhow::Result<Vec<AccountInfo>> {
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
            let shard_id = shard_layout.account_id_to_shard_id(&validator.account_id);
            let shard_idx = shard_layout.get_shard_index(shard_id).unwrap();
            let validator_account = AccountInfo {
                account_id: validator.account_id,
                amount: validator.amount.unwrap_or(50_000 * NEAR_BASE),
                public_key: validator.public_key,
            };
            new_validator_accounts.push(validator_account.clone());
            storage_mutator.set_account(
                shard_idx,
                validator_account.account_id.clone(),
                Account::new(
                    liquid_balance,
                    validator_account.amount,
                    AccountContract::None,
                    storage_bytes,
                ),
            )?;
            storage_mutator.set_access_key(
                shard_idx,
                validator_account.account_id,
                validator_account.public_key,
                AccessKey::full_access(),
            )?;
        }
        storage_mutator.commit()?;
        Ok(new_validator_accounts)
    }

    /// Makes a new genesis and writes it to `~/.near/genesis.json`.
    fn make_and_write_genesis(
        &self,
        genesis_time: DateTime<Utc>,
        protocol_version: Option<ProtocolVersion>,
        shard_layout: ShardLayout,
        epoch_length: u64,
        num_seats: &Option<NumSeats>,
        height: BlockHeight,
        chain_id_suffix: &str,
        chain_id: &Option<String>,
        new_state_roots: Vec<StateRoot>,
        new_validator_accounts: Vec<AccountInfo>,
        home_dir: &Path,
        near_config: &mut NearConfig,
    ) -> anyhow::Result<()> {
        // At this point, the genesis will contain the chain id of the original chain.
        // We need to load the epoch config for the new chain id and new genesis protocol version.
        let new_chain_id = chain_id
            .clone()
            .unwrap_or_else(|| near_config.genesis.config.chain_id.clone() + chain_id_suffix);
        near_config.genesis.config.chain_id = new_chain_id.clone();

        let genesis_protocol_version = match protocol_version {
            Some(v) => v,
            None => near_config.genesis.config.protocol_version,
        };
        near_config.genesis.config.protocol_version = genesis_protocol_version;

        // This is based on the assumption that epoch length is part of genesis config and not epoch config.
        near_config.genesis.config.epoch_length = epoch_length;

        let epoch_config =
            self.override_epoch_configs(genesis_protocol_version, num_seats, home_dir)?;

        let original_config = near_config.genesis.config.clone();

        // TODO: consider doing something smarter with these two
        let num_block_producer_seats_per_shard = vec![
            original_config
                .num_block_producer_seats_per_shard[0];
            shard_layout.num_shards() as usize
        ];
        let avg_hidden_validator_seats_per_shard =
            if original_config.avg_hidden_validator_seats_per_shard.is_empty() {
                Vec::new()
            } else {
                vec![
                    original_config.avg_hidden_validator_seats_per_shard[0];
                    shard_layout.num_shards() as usize
                ]
            };
        let new_config = GenesisConfig {
            chain_id: new_chain_id,
            genesis_height: height,
            genesis_time,
            epoch_length,
            num_block_producer_seats: epoch_config.num_block_producer_seats,
            num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard,
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            chunk_validator_only_kickout_threshold: 0,
            target_validator_mandates_per_shard: epoch_config.target_validator_mandates_per_shard,
            max_kickout_stake_perc: 0,
            online_min_threshold: epoch_config.online_min_threshold,
            online_max_threshold: epoch_config.online_max_threshold,
            fishermen_threshold: epoch_config.fishermen_threshold,
            minimum_stake_divisor: epoch_config.minimum_stake_divisor,
            protocol_upgrade_stake_threshold: epoch_config.protocol_upgrade_stake_threshold,
            shard_layout,
            num_chunk_only_producer_seats: epoch_config.num_chunk_only_producer_seats,
            minimum_validators_per_shard: epoch_config.minimum_validators_per_shard,
            minimum_stake_ratio: epoch_config.minimum_stake_ratio,
            shuffle_shard_assignment_for_chunk_producers: epoch_config
                .shuffle_shard_assignment_for_chunk_producers,
            dynamic_resharding: false,
            protocol_version: genesis_protocol_version,
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
            num_chunk_producer_seats: epoch_config.num_chunk_producer_seats,
            num_chunk_validator_seats: epoch_config.num_chunk_validator_seats,
            chunk_producer_assignment_changes_limit: epoch_config
                .chunk_producer_assignment_changes_limit,
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
