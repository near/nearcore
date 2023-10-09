use crate::storage_mutator::StorageMutator;
use near_chain::types::Tip;
use near_chain_configs::{Genesis, GenesisConfig, GenesisValidationMode};
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_mirror::key_mapping::{map_account, map_key};
use near_primitives::account::Account;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::state_record::StateRecord;
use near_primitives::types::AccountInfo;
use near_primitives::types::{AccountId, ShardId, StateRoot};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::flat::store_helper;
use near_store::flat::FlatStorageStatus;
use near_store::{
    checkpoint_hot_storage_and_cleanup_columns, DBCol, Store, TrieDBStorage, TrieStorage, HEAD_KEY,
};
use nearcore::{load_config, open_storage, NearConfig, NightshadeRuntime, NEAR_BASE};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use strum::IntoEnumIterator;

#[derive(clap::Parser)]
/// TODO: Write docs.
/// If --reset, then does [`reset`] and nothing else.
/// Snapshots `~/.near/data` into `~/.near/data/fork-snapshot/data`.
/// Checks that hash of flat storage head is the same for every shard.
/// Reads a block corresponding to the flat head.
/// Reads a list of validators from `--validators`.
/// Adds validator accounts to the genesis.
/// Deletes all columns in `~/.near/data` other than FlatState, State, DbVersion.
/// Makes a backup of the genesis file in `~/.near/genesis.json.backup`.
/// Creates a new genesis:
/// * for chain `$original_chain_id + --chain_id_suffix`
/// * with epoch_length set to `--epoch_length`
/// * with contents set to the state roots determined by the block corresponding to the flat head.
pub struct ForkNetworkCommand {
    #[arg(short, long)]
    pub reset: bool,
    #[arg(short, long, default_value = "1000")]
    pub epoch_length: u64,
    /// Path to the JSON list of the first epoch validators and their public keys relative to `home_dir`.
    #[arg(short, long)]
    pub validators: PathBuf,
    #[arg(long, default_value = "-fork")]
    pub chain_id_suffix: String,
}

#[derive(Deserialize)]
struct Validator {
    account_id: AccountId,
    public_key: PublicKey,
}

impl ForkNetworkCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        if self.reset {
            // Reset and do nothing else.
            return self.reset(home_dir);
        }

        // Load config and check flat storage param
        let mut near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {e:#}"));

        if !near_config.config.store.flat_storage_creation_enabled {
            panic!("Flat storage must be enabled");
        }

        // Open storage with migration
        let storage = open_storage(&home_dir, &mut near_config).unwrap();
        let store = storage.get_hot_store();

        let store_path =
            home_dir.join(near_config.config.store.path.clone().unwrap_or(PathBuf::from("data")));
        let fork_snapshot_path = store_path.join("fork-snapshot");

        println!("Creating snapshot of original db into {}", fork_snapshot_path.display());
        // checkpointing only hot storage, because cold storage will not be changed
        checkpoint_hot_storage_and_cleanup_columns(&store, &fork_snapshot_path, None).unwrap();

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        let all_shard_uids = shard_layout.get_shard_uids();

        let fork_head = get_fork_head(&all_shard_uids, store.clone())?;

        println!("Forking from the flat storage final head: {fork_head}");

        let fork_head_block =
            store.get_ser::<Block>(DBCol::Block, &fork_head.try_to_vec().unwrap())?.unwrap();

        let (new_state_roots, new_validator_accounts) = self.mutate_state(
            &fork_head_block,
            &all_shard_uids,
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )?;

        println!("Creating a new genesis");
        backup_genesis_file(home_dir, &near_config)?;
        self.make_and_write_genesis(
            fork_head_block.header(),
            new_state_roots,
            new_validator_accounts,
            epoch_manager,
            home_dir,
            &near_config,
        )?;

        println!("Delete all columns in the original DB other than DbVersion, State and FlatState");
        let mut update = store.store_update();
        for col in DBCol::iter() {
            match col {
                DBCol::DbVersion | DBCol::State | DBCol::FlatState => {}
                _ => {
                    update.delete_all(col);
                }
            }
        }
        update.commit()?;

        println!("All Done! Run the node normally to start the forked network.");
        Ok(())
    }

    /// Checks that `~/.near/data/fork-snapshot/data` exists.
    /// Deletes files (not directories) in `~/.near/data`
    /// Moves everything from `~/.near/data/fork-snapshot/data/` to `~/.near/data`.
    /// Deletes `~/.near/data/fork-snapshot/data`.
    /// Moves `~/.near/genesis.json.backup` to `~/.near/genesis.json`.
    fn reset(self, home_dir: &Path) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, GenesisValidationMode::UnsafeFast)
            .unwrap_or_else(|e| panic!("Error loading config: {e:#}"));

        let store_path =
            home_dir.join(near_config.config.store.path.clone().unwrap_or(PathBuf::from("data")));
        // '/data' prefix comes from the use of `checkpoint_hot_storage_and_cleanup_columns` fn
        let fork_snapshot_path = store_path.join("fork-snapshot/data");
        if !Path::new(&fork_snapshot_path).exists() {
            panic!("Fork snapshot does not exist");
        }
        println!("Removing all current data");
        for entry in std::fs::read_dir(&store_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                std::fs::remove_file(&entry.path())?;
            }
        }
        println!("Restoring fork snapshot");
        for entry in std::fs::read_dir(&fork_snapshot_path)? {
            let entry = entry?;
            std::fs::rename(&entry.path(), &store_path.join(entry.file_name()))?;
        }
        std::fs::remove_dir(&fork_snapshot_path)?;

        println!("Restoring genesis file");
        restore_backup_genesis_file(home_dir, &near_config)?;
        return Ok(());
    }

    /// Reads a list of validators from a file `self.validators`.
    fn mutate_state(
        &self,
        fork_head_block: &Block,
        all_shard_uids: &[ShardUId],
        home_dir: &Path,
        store: Store,
        near_config: &NearConfig,
        epoch_manager: Arc<EpochManagerHandle>,
    ) -> anyhow::Result<(Vec<StateRoot>, Vec<AccountInfo>)> {
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        );
        let prev_state_roots = fork_head_block
            .chunks()
            .iter()
            .map(|chunk| chunk.prev_state_root())
            .collect::<Vec<_>>();
        let mut storage_mutator = StorageMutator::new(
            epoch_manager,
            &*runtime,
            fork_head_block.header().epoch_id(),
            *fork_head_block.header().prev_hash(),
            &prev_state_roots,
        )?;

        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);

        self.prepare_state(&all_shard_uids, store, &mut storage_mutator)?;
        let new_validator_accounts =
            self.add_validator_accounts(&mut storage_mutator, runtime_config, home_dir)?;

        let new_state_roots = storage_mutator.commit()?;
        Ok((new_state_roots, new_validator_accounts))
    }

    fn prepare_state(
        &self,
        all_shard_uids: &[ShardUId],
        store: Store,
        storage_mutator: &mut StorageMutator,
    ) -> anyhow::Result<()> {
        // Doesn't support secrets.
        let mut has_full_key = HashSet::new();
        let mut accounts = HashSet::new();

        for shard_uid in all_shard_uids {
            let trie_storage = TrieDBStorage::new(store.clone(), *shard_uid);
            let mut index_delayed_receipt = 0;
            for item in store_helper::iter_flat_state_entries(*shard_uid, &store, None, None) {
                let (key, value) = match item {
                    Ok((key, FlatStateValue::Ref(ref_value))) => {
                        (key, trie_storage.retrieve_raw_bytes(&ref_value.hash)?.to_vec())
                    }
                    Ok((key, FlatStateValue::Inlined(value))) => (key, value),
                    otherwise => panic!("Unexpected flat state value: {otherwise:?}"),
                };
                match StateRecord::from_raw_key_value(key, value).unwrap() {
                    StateRecord::AccessKey { account_id, public_key, access_key } => {
                        if !account_id.is_implicit()
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
                    }

                    StateRecord::Account { account_id, account } => {
                        if account_id.is_implicit() {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_account(account_id)?;
                            storage_mutator.set_account(new_account_id, account)?;
                        } else {
                            accounts.insert(account_id.clone());
                        }
                    }
                    StateRecord::Data { account_id, data_key, value } => {
                        if account_id.is_implicit() {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_data(account_id, &data_key)?;
                            storage_mutator.set_data(new_account_id, &data_key, value)?;
                        }
                    }
                    StateRecord::Contract { account_id, code } => {
                        if account_id.is_implicit() {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_code(account_id)?;
                            storage_mutator.set_code(new_account_id, code)?;
                        }
                    }
                    StateRecord::PostponedReceipt(receipt) => {
                        if receipt.predecessor_id.is_implicit() || receipt.receiver_id.is_implicit()
                        {
                            let new_receipt = Receipt {
                                predecessor_id: map_account(&receipt.predecessor_id, None),
                                receiver_id: map_account(&receipt.receiver_id, None),
                                receipt_id: receipt.receipt_id,
                                receipt: receipt.receipt.clone(),
                            };
                            storage_mutator.delete_postponed_receipt(receipt)?;
                            storage_mutator.set_postponed_receipt(&new_receipt)?;
                        }
                    }
                    StateRecord::ReceivedData { account_id, data_id, data } => {
                        if account_id.is_implicit() {
                            let new_account_id = map_account(&account_id, None);
                            storage_mutator.delete_received_data(account_id, data_id)?;
                            storage_mutator.set_received_data(new_account_id, data_id, &data)?;
                        }
                    }
                    StateRecord::DelayedReceipt(receipt) => {
                        if receipt.predecessor_id.is_implicit() || receipt.receiver_id.is_implicit()
                        {
                            let new_receipt = Receipt {
                                predecessor_id: map_account(&receipt.predecessor_id, None),
                                receiver_id: map_account(&receipt.receiver_id, None),
                                receipt_id: receipt.receipt_id,
                                receipt: receipt.receipt,
                            };
                            storage_mutator.delete_delayed_receipt(
                                shard_uid.shard_id as ShardId,
                                index_delayed_receipt,
                            )?;
                            storage_mutator.set_delayed_receipt(
                                shard_uid.shard_id as ShardId,
                                index_delayed_receipt,
                                &new_receipt,
                            )?;
                        }
                        index_delayed_receipt += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn add_validator_accounts(
        &self,
        storage_mutator: &mut StorageMutator,
        runtime_config: &Arc<RuntimeConfig>,
        home_dir: &Path,
    ) -> anyhow::Result<Vec<AccountInfo>> {
        let mut new_validator_accounts = vec![];

        let liquid_balance = 100_000_000 * NEAR_BASE;
        let storage_bytes = runtime_config.fees.storage_usage_config.num_bytes_account;
        let new_validators: Vec<Validator> = serde_json::from_reader(BufReader::new(
            File::open(home_dir.join(&self.validators)).expect("Failed to open validators JSON"),
        ))
        .expect("Failed to read validators JSON");
        for validator in new_validators.into_iter() {
            let validator_account = AccountInfo {
                account_id: validator.account_id,
                amount: 50_000 * NEAR_BASE,
                public_key: validator.public_key,
            };
            new_validator_accounts.push(validator_account.clone());
            storage_mutator.set_account(
                validator_account.account_id.clone(),
                Account::new(
                    liquid_balance,
                    validator_account.amount,
                    CryptoHash::default(),
                    storage_bytes,
                ),
            )?;
            storage_mutator.set_access_key(
                validator_account.account_id,
                validator_account.public_key,
                AccessKey::full_access(),
            )?;
        }
        Ok(new_validator_accounts)
    }

    /// Makes a new genesis and writes it to `~/.near/genesis.json`.
    fn make_and_write_genesis(
        &self,
        fork_head_header: &BlockHeader,
        new_state_roots: Vec<StateRoot>,
        new_validator_accounts: Vec<AccountInfo>,
        epoch_manager: Arc<EpochManagerHandle>,
        home_dir: &Path,
        near_config: &NearConfig,
    ) -> anyhow::Result<()> {
        let epoch_config = epoch_manager.get_epoch_config(&fork_head_header.epoch_id())?;
        let epoch_info = epoch_manager.get_epoch_info(&fork_head_header.epoch_id())?;
        let original_config = near_config.genesis.config.clone();

        let new_config = GenesisConfig {
            chain_id: original_config.chain_id.clone() + self.chain_id_suffix.as_str(),
            genesis_height: fork_head_header.height(),
            genesis_time: fork_head_header.timestamp(),
            epoch_length: self.epoch_length,
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
            dynamic_resharding: false,
            protocol_version: epoch_info.protocol_version(),
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

        println!("Writing new genesis to {}", original_genesis_file.display());
        genesis.to_file(&original_genesis_file);

        println!("All Done! Run the node normally to start the forked network.");
        Ok(())
    }
}

fn backup_genesis_file_path(home_dir: &Path, genesis_file: &str) -> PathBuf {
    home_dir.join(format!("{}.backup", &genesis_file))
}

/// Returns hash of flat head.
/// Checks that all shards have flat storage.
/// Checks that flat heads of all shards match.
fn get_fork_head(all_shard_uids: &[ShardUId], store: Store) -> anyhow::Result<CryptoHash> {
    let mut flat_head: Option<CryptoHash> = None;
    for shard_uid in all_shard_uids {
        let flat_storage_status = store
            .get_ser::<FlatStorageStatus>(DBCol::FlatStorageStatus, &shard_uid.to_bytes())?
            .unwrap();
        if let FlatStorageStatus::Ready(ready) = &flat_storage_status {
            let flat_head_for_this_shard = ready.flat_head.hash;
            if let Some(hash) = flat_head {
                if hash != flat_head_for_this_shard {
                    panic!("Not all shards have the same flat head. Please reset the fork, and run the node for a little longer");
                }
            }
            flat_head = Some(flat_head_for_this_shard);
        } else {
            panic!("Flat storage is not ready for shard {shard_uid}: {flat_storage_status:?}. Please reset the fork, and run the node for longer");
        }
    }
    Ok(flat_head.unwrap())
}

fn backup_genesis_file(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<()> {
    let genesis_file = &near_config.config.genesis_file;
    let original_genesis_file = home_dir.join(&genesis_file);
    let backup_genesis_file = backup_genesis_file_path(home_dir, &genesis_file);
    println!("Backing up old genesis. {original_genesis_file:?} -> {backup_genesis_file:?}");
    std::fs::rename(&original_genesis_file, &backup_genesis_file)?;
    Ok(())
}

fn restore_backup_genesis_file(home_dir: &Path, near_config: &NearConfig) -> anyhow::Result<()> {
    let genesis_file = &near_config.config.genesis_file;
    let backup_genesis_file = backup_genesis_file_path(home_dir, &genesis_file);
    let original_genesis_file = home_dir.join(&genesis_file);
    println!(
        "Restoring genesis from a backup. {backup_genesis_file:?} -> {original_genesis_file:?}"
    );
    std::fs::rename(&backup_genesis_file, &original_genesis_file)?;
    Ok(())
}
