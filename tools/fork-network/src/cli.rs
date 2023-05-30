use std::path::{Path, PathBuf};

use near_chain::types::Tip;
use near_chain_configs::{Genesis, GenesisConfig, GenesisRecords, GenesisValidationMode};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::{borsh::BorshSerialize, hash::CryptoHash, types::AccountInfo};
use near_store::{flat::FlatStorageStatus, DBCol, Mode, NodeStorage, HEAD_KEY};
use nearcore::load_config;

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
            panic!("Flat storage is required");
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

        println!("Creating snapshot");
        store_opener.create_snapshots(Mode::ReadWrite)?;

        let store_path =
            home_dir.join(near_config.config.store.path.clone().unwrap_or(PathBuf::from("data")));
        let migration_snapshot_path = store_path.join(
            near_config
                .config
                .store
                .migration_snapshot
                .get_path(&store_path)
                .expect("Migration snapshot must be enabled"),
        );
        let fork_snapshot_path = store_path.join("fork-snapshot");
        std::fs::rename(&migration_snapshot_path, &fork_snapshot_path)?;

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        let all_shard_uids = shard_layout.get_shard_uids();

        let mut flat_head: Option<CryptoHash> = None;
        for shard_uid in all_shard_uids {
            let flat_storage_status = store
                .get_ser::<FlatStorageStatus>(DBCol::FlatStorageStatus, &shard_uid.to_bytes())?
                .unwrap();
            if let FlatStorageStatus::Ready(ready) = &flat_storage_status {
                let flat_head_for_this_shard = ready.flat_head.hash;
                if let Some(hash) = flat_head {
                    if hash != flat_head_for_this_shard {
                        panic!("Not all shards have the same flat head");
                    }
                }
                flat_head = Some(flat_head_for_this_shard);
            } else {
                panic!(
                    "Flat storage is not ready for shard {}: {:?}",
                    shard_uid, flat_storage_status
                );
            }
        }

        let flat_head_block = store
            .get_ser::<near_primitives::block::Block>(
                DBCol::Block,
                &flat_head.unwrap().try_to_vec().unwrap(),
            )?
            .unwrap();

        let state_roots = flat_head_block
            .chunks()
            .iter()
            .map(|chunk| chunk.prev_state_root())
            .collect::<Vec<_>>();

        println!("Creating a new genesis");
        let epoch_config = epoch_manager.get_epoch_config(&flat_head_block.header().epoch_id())?;
        let epoch_info = epoch_manager.get_epoch_info(&flat_head_block.header().epoch_id())?;
        let original_config = near_config.genesis.config.clone();

        let self_validator = near_config.validator_signer.as_ref().unwrap();
        let self_account = AccountInfo {
            account_id: self_validator.validator_id().clone(),
            amount: 50000000000000000000000000000_u128,
            public_key: self_validator.public_key().clone(),
        };

        let new_config = GenesisConfig {
            chain_id: original_config.chain_id.clone() + "-fork",
            genesis_height: flat_head_block.header().height(),
            genesis_time: flat_head_block.header().timestamp(),
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
        // update.delete_all(DBCol::DbVersion);
        update.delete_all(DBCol::BlockMisc);
        update.delete_all(DBCol::Block);
        update.delete_all(DBCol::BlockHeader);
        update.delete_all(DBCol::BlockHeight);
        // update.delete_all(DBCol::State);
        update.delete_all(DBCol::ChunkExtra);
        update.delete_all(DBCol::_TransactionResult);
        update.delete_all(DBCol::OutgoingReceipts);
        update.delete_all(DBCol::IncomingReceipts);
        update.delete_all(DBCol::_Peers);
        update.delete_all(DBCol::RecentOutboundConnections);
        update.delete_all(DBCol::EpochInfo);
        update.delete_all(DBCol::BlockInfo);
        update.delete_all(DBCol::Chunks);
        update.delete_all(DBCol::PartialChunks);
        update.delete_all(DBCol::BlocksToCatchup);
        update.delete_all(DBCol::StateDlInfos);
        update.delete_all(DBCol::ChallengedBlocks);
        update.delete_all(DBCol::StateHeaders);
        update.delete_all(DBCol::InvalidChunks);
        update.delete_all(DBCol::BlockExtra);
        update.delete_all(DBCol::BlockPerHeight);
        update.delete_all(DBCol::StateParts);
        update.delete_all(DBCol::EpochStart);
        update.delete_all(DBCol::AccountAnnouncements);
        update.delete_all(DBCol::NextBlockHashes);
        update.delete_all(DBCol::EpochLightClientBlocks);
        update.delete_all(DBCol::ReceiptIdToShardId);
        update.delete_all(DBCol::_NextBlockWithNewChunk);
        update.delete_all(DBCol::_LastBlockWithNewChunk);
        update.delete_all(DBCol::PeerComponent);
        update.delete_all(DBCol::ComponentEdges);
        update.delete_all(DBCol::LastComponentNonce);
        update.delete_all(DBCol::Transactions);
        update.delete_all(DBCol::_ChunkPerHeightShard);
        update.delete_all(DBCol::StateChanges);
        update.delete_all(DBCol::BlockRefCount);
        update.delete_all(DBCol::TrieChanges);
        update.delete_all(DBCol::BlockMerkleTree);
        update.delete_all(DBCol::ChunkHashesByHeight);
        update.delete_all(DBCol::BlockOrdinal);
        update.delete_all(DBCol::_GCCount);
        update.delete_all(DBCol::OutcomeIds);
        update.delete_all(DBCol::_TransactionRefCount);
        update.delete_all(DBCol::ProcessedBlockHeights);
        update.delete_all(DBCol::Receipts);
        update.delete_all(DBCol::CachedContractCode);
        update.delete_all(DBCol::EpochValidatorInfo);
        update.delete_all(DBCol::HeaderHashesByHeight);
        update.delete_all(DBCol::StateChangesForSplitStates);
        update.delete_all(DBCol::TransactionResultForBlock);
        // update.delete_all(DBCol::FlatState);
        update.delete_all(DBCol::FlatStateChanges);
        update.delete_all(DBCol::FlatStateDeltaMetadata);
        update.delete_all(DBCol::FlatStorageStatus);
        update.commit()?;

        let genesis = Genesis::new_validated(
            new_config,
            GenesisRecords::default(),
            Some(state_roots),
            GenesisValidationMode::UnsafeFast,
        )?;
        // let epoch_manager = EpochManager::new_arc_handle(store, &genesis_config);
        // let runtime = NightshadeRuntime::from_config(home_dir, store, new_config, epoch_manager)?;

        let genesis_file = near_config.config.genesis_file;
        let original_genesis_file = home_dir.join(&genesis_file);
        let backup_genesis_file = home_dir.join(format!("{}.backup", &genesis_file));
        std::fs::rename(&original_genesis_file, &backup_genesis_file)?;
        genesis.to_file(&original_genesis_file);

        Ok(())
    }
}
