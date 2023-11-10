use anyhow::Context;
use clap;
use near_chain::{ChainStore, ChainStoreAccess, ChainUpdate, DoomslugThresholdMode};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::EpochManager;
use near_primitives::block::BlockHeader;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_store::{checkpoint_hot_storage_and_cleanup_columns, DBCol, NodeStorage};
use nearcore::NightshadeRuntime;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(clap::Parser)]
pub struct EpochSyncCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    ValidateEpochSyncInfo,
}

impl EpochSyncCommand {
    pub fn run(self, home_dir: &Path) -> anyhow::Result<()> {
        let mut near_config = EpochSyncCommand::create_snapshot(home_dir)?;
        let storage = nearcore::open_storage(&home_dir, &mut near_config)?;

        match self.subcmd {
            SubCommand::ValidateEpochSyncInfo => {
                validate_epoch_sync_info(&home_dir, &storage, &near_config)
            }
        }
    }

    fn create_snapshot(home_dir: &Path) -> anyhow::Result<nearcore::config::NearConfig> {
        let mut near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::UnsafeFast,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let store_path_addition = near_config
            .config
            .store
            .path
            .clone()
            .unwrap_or(PathBuf::from("data"))
            .join("epoch-sync-snapshot");
        let snapshot_path = home_dir.join(store_path_addition.clone());

        let storage = nearcore::open_storage(&home_dir, &mut near_config)?;

        if snapshot_path.exists() && snapshot_path.is_dir() {
            tracing::info!(?snapshot_path, "Found a DB snapshot");
        } else {
            tracing::info!(destination = ?snapshot_path, "Creating snapshot of original DB");
            // checkpointing only hot storage, because cold storage will not be changed
            checkpoint_hot_storage_and_cleanup_columns(
                &storage.get_hot_store(),
                &snapshot_path,
                None,
            )?;
        }

        near_config.config.store.path = Some(store_path_addition.join("data"));

        Ok(near_config)
    }
}

fn validate_epoch_sync_info(
    home_dir: &Path,
    storage: &NodeStorage,
    config: &nearcore::config::NearConfig,
) -> anyhow::Result<()> {
    let store = storage.get_hot_store();

    let hash_to_prev_hash = get_hash_to_prev_hash(storage)?;
    let epoch_ids = get_all_epoch_ids(storage)?;

    let mut chain_store =
        ChainStore::new(store.clone(), config.genesis.config.genesis_height, false);
    let header_head_hash = chain_store.header_head()?.last_block_hash;
    let hash_to_next_hash = get_hash_to_next_hash(&hash_to_prev_hash, &header_head_hash)?;

    let epoch_manager =
        EpochManager::new_arc_handle(storage.get_hot_store(), &config.genesis.config);
    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&config.client_config), epoch_manager.clone());
    let runtime = NightshadeRuntime::from_config(
        home_dir,
        storage.get_hot_store(),
        &config,
        epoch_manager.clone(),
    );
    let chain_update = ChainUpdate::new(
        &mut chain_store,
        epoch_manager,
        shard_tracker,
        runtime,
        DoomslugThresholdMode::TwoThirds,
        config.genesis.config.transaction_validity_period,
    );

    let genesis_hash = store
        .get_ser::<CryptoHash>(
            DBCol::BlockHeight,
            &config.genesis.config.genesis_height.to_le_bytes(),
        )?
        .expect("Expect genesis height to be present in BlockHeight column");

    let mut cur_hash = header_head_hash;

    // Edge case if we exactly at the epoch boundary.
    if epoch_ids.contains(&cur_hash) {
        cur_hash = hash_to_prev_hash[&cur_hash];
    }

    let mut num_errors = 0;

    while cur_hash != genesis_hash {
        tracing::debug!("Big loop hash {:?}", cur_hash);

        // epoch ids are the last hashes of some epochs
        if epoch_ids.contains(&cur_hash) {
            let last_header = store
                .get_ser::<BlockHeader>(DBCol::BlockHeader, cur_hash.as_ref())?
                .context("BlockHeader for cur_hash not found")?;
            let last_finalized_height = if *last_header.last_final_block() == CryptoHash::default()
            {
                0
            } else {
                let last_finalized_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        last_header.last_final_block().as_ref(),
                    )?
                    .context("BlockHeader for cur_hash.last_final_block not found")?;
                last_finalized_header.height()
            };

            loop {
                let prev_hash = hash_to_prev_hash[&cur_hash];
                if epoch_ids.contains(&prev_hash) {
                    // prev_hash is the end of previous epoch
                    // cur_hash is the start of current epoch
                    break;
                } else {
                    // prev_hash is still in the current epoch
                    // we descent to it
                    cur_hash = prev_hash;
                }
            }

            let first_block_hash = cur_hash;

            let mut last_block_info = BlockInfo::new(
                *last_header.hash(),
                last_header.height(),
                last_finalized_height,
                *last_header.last_final_block(),
                *last_header.prev_hash(),
                last_header.prev_validator_proposals().collect(),
                last_header.chunk_mask().to_vec(),
                vec![],
                last_header.total_supply(),
                last_header.latest_protocol_version(),
                last_header.raw_timestamp(),
            );

            *last_block_info.epoch_id_mut() = last_header.epoch_id().clone();
            *last_block_info.epoch_first_block_mut() = first_block_hash;

            let next_epoch_first_hash = hash_to_next_hash[last_header.hash()];
            tracing::debug!("Creating EpochSyncInfo from block {:?}", last_header);

            let epoch_sync_info =
                chain_update.create_epoch_sync_info(&last_block_info, &next_epoch_first_hash)?;

            let calculated_epoch_sync_data_hash_result =
                epoch_sync_info.calculate_epoch_sync_data_hash();
            let canonical_epoch_sync_data_hash_result = epoch_sync_info.get_epoch_sync_data_hash();

            if let Ok(calculated_epoch_sync_data_hash) = calculated_epoch_sync_data_hash_result {
                if let Ok(Some(canonical_epoch_sync_data_hash)) =
                    canonical_epoch_sync_data_hash_result
                {
                    if calculated_epoch_sync_data_hash == canonical_epoch_sync_data_hash {
                        tracing::info!(
                            "EpochSyncInfo for height {:?} OK",
                            epoch_sync_info.epoch_info.epoch_height()
                        );
                        continue;
                    }
                }
            }
            tracing::error!(
                "EpochSyncInfo for height {:?} ERROR {:?} {:?}",
                epoch_sync_info.epoch_info.epoch_height(),
                calculated_epoch_sync_data_hash_result,
                canonical_epoch_sync_data_hash_result
            );
            num_errors += 1;
        } else {
            cur_hash = hash_to_prev_hash[&cur_hash];
        }
    }
    assert_eq!(num_errors, 0);
    Ok(())
}

fn get_hash_to_prev_hash(storage: &NodeStorage) -> anyhow::Result<HashMap<CryptoHash, CryptoHash>> {
    let mut hash_to_prev_hash = HashMap::new();
    for result in storage.get_hot_store().iter(DBCol::BlockInfo) {
        let (_, value) = result?;
        let block_info =
            BlockInfo::try_from_slice(value.as_ref()).expect("Failed to deser BlockInfo");
        if block_info.hash() != block_info.prev_hash() {
            hash_to_prev_hash.insert(*block_info.hash(), *block_info.prev_hash());
        }
    }
    Ok(hash_to_prev_hash)
}

fn get_hash_to_next_hash(
    hash_to_prev_hash: &HashMap<CryptoHash, CryptoHash>,
    chain_head: &CryptoHash,
) -> anyhow::Result<HashMap<CryptoHash, CryptoHash>> {
    let mut hash_to_next_hash = HashMap::new();
    let mut cur_head = *chain_head;
    while let Some(prev_hash) = hash_to_prev_hash.get(&cur_head) {
        hash_to_next_hash.insert(*prev_hash, cur_head);
        cur_head = *prev_hash;
    }
    Ok(hash_to_next_hash)
}

fn get_all_epoch_ids(storage: &NodeStorage) -> anyhow::Result<HashSet<CryptoHash>> {
    let mut epoch_ids = HashSet::new();
    for result in storage.get_hot_store().iter(DBCol::EpochInfo) {
        let (key, _) = result?;
        if key.as_ref() == AGGREGATOR_KEY {
            continue;
        }
        epoch_ids
            .insert(CryptoHash::try_from_slice(key.as_ref()).expect("Failed to deser CryptoHash"));
    }
    Ok(epoch_ids)
}
