use crate::storage_mutator::StorageMutator;
use near_chain::types::Tip;
use near_chain_configs::{Genesis, GenesisConfig, GenesisValidationMode};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::account::AccessKey;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::{
    account::Account, borsh::BorshSerialize, hash::CryptoHash, types::AccountInfo,
};
use near_store::{flat::FlatStorageStatus, DBCol, Mode, NodeStorage, HEAD_KEY};
use nearcore::{load_config, NightshadeRuntime, NEAR_BASE};
use std::path::{Path, PathBuf};
use strum::IntoEnumIterator;

#[derive(clap::Parser)]
pub struct ForkNetworkCommand {
    #[arg(short, long)]
    pub reset: bool,
    #[arg(short, long, default_value = "1000")]
    pub epoch_length: u64,
}

impl ForkNetworkCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        if self.reset {
            let near_config = load_config(home_dir, GenesisValidationMode::UnsafeFast)
                .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

            let store_path = home_dir
                .join(near_config.config.store.path.clone().unwrap_or(PathBuf::from("data")));
            let fork_snapshot_path = store_path.join("fork-snapshot");
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
            let genesis_path = home_dir.join(near_config.config.genesis_file.clone() + ".backup");
            let original_genesis_path = home_dir.join(&near_config.config.genesis_file);
            std::fs::rename(&genesis_path, &original_genesis_path)?;
            return Ok(());
        }
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        if !near_config.config.store.flat_storage_creation_enabled {
            panic!("Flat storage must be enabled");
        }

        if near_config.validator_signer.is_none() {
            panic!("Validator ID is required; please set up validator_keys.json");
        }

        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            None,
        );

        let store_path =
            home_dir.join(near_config.config.store.path.clone().unwrap_or(PathBuf::from("data")));
        let fork_snapshot_path = store_path.join("fork-snapshot");

        println!("Creating snapshot of original db into {}", fork_snapshot_path.display());
        let migration_snapshot_path = store_opener
            .create_snapshots(Mode::ReadWrite)?
            .0
             .0
            .clone()
            .expect("Migration snapshot must be enabled");
        std::fs::rename(&migration_snapshot_path, &fork_snapshot_path)?;

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        );
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        let all_shard_uids = shard_layout.get_shard_uids();

        let fork_head = {
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
                    panic!(
                        "Flat storage is not ready for shard {}: {:?}. Please reset the fork, and run the node for longer",
                        shard_uid, flat_storage_status
                    );
                }
            }
            println!("Forking from the flat storage final head: {}", flat_head.unwrap());
            flat_head.unwrap()
        };

        let fork_head_block = store
            .get_ser::<near_primitives::block::Block>(
                DBCol::Block,
                &fork_head.try_to_vec().unwrap(),
            )?
            .unwrap();

        let prev_state_roots = fork_head_block
            .chunks()
            .iter()
            .map(|chunk| chunk.prev_state_root())
            .collect::<Vec<_>>();

        // TODO(robin-near): Make this part customizable via some kind of config file.
        let self_validator = near_config.validator_signer.as_ref().unwrap();
        let self_account = AccountInfo {
            account_id: self_validator.validator_id().clone(),
            amount: 50_000 * NEAR_BASE,
            public_key: self_validator.public_key(),
        };

        let mut storage_mutator = StorageMutator::new(
            epoch_manager.clone(),
            &*runtime,
            fork_head_block.header().epoch_id(),
            *fork_head_block.header().prev_hash(),
            &prev_state_roots,
        )?;

        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);
        let storage_bytes = runtime_config.fees.storage_usage_config.num_bytes_account;
        let liquid_balance = 100_000_000 * NEAR_BASE;
        storage_mutator.set_account(
            self_validator.validator_id().clone(),
            Account::new(liquid_balance, self_account.amount, CryptoHash::default(), storage_bytes),
        )?;
        storage_mutator.set_access_key(
            self_account.account_id.clone(),
            self_account.public_key.clone(),
            AccessKey::full_access(),
        )?;

        let new_state_roots = storage_mutator.commit()?;

        println!("Creating a new genesis");
        let epoch_config = epoch_manager.get_epoch_config(&fork_head_block.header().epoch_id())?;
        let epoch_info = epoch_manager.get_epoch_info(&fork_head_block.header().epoch_id())?;
        let original_config = near_config.genesis.config.clone();

        let new_config = GenesisConfig {
            chain_id: original_config.chain_id.clone() + "-fork",
            genesis_height: fork_head_block.header().height(),
            genesis_time: fork_head_block.header().timestamp(),
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
            validators: vec![self_account],
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
        drop(epoch_manager);

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

        let genesis = Genesis::new_from_state_roots(new_config, new_state_roots);
        let genesis_file = near_config.config.genesis_file;
        let original_genesis_file = home_dir.join(&genesis_file);
        let backup_genesis_file = home_dir.join(format!("{}.backup", &genesis_file));
        println!("Backing up old genesis to {}", backup_genesis_file.display());
        std::fs::rename(&original_genesis_file, &backup_genesis_file)?;
        println!("Writing new genesis to {}", original_genesis_file.display());
        genesis.to_file(&original_genesis_file);

        println!("All Done! Run the node normally to start the forked network.");
        Ok(())
    }
}
