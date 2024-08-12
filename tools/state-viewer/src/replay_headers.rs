use std::path::Path;

use itertools::Itertools;
use near_chain::BlockHeader;
use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::EpochManagerHandle;
use near_epoch_manager::{types::BlockHeaderInfo, EpochManager};
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::{BlockHeight, ValidatorInfoIdentifier};
use near_primitives::version::ProtocolFeature;
use near_store::db::{MixedDB, ReadOrder, TestDB};
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::NearConfig;

pub(crate) fn replay_headers(
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let start_height: BlockHeight =
        start_height.unwrap_or_else(|| chain_store.get_genesis_height());
    let end_height: BlockHeight = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);

    let epoch_manager = EpochManager::new_arc_handle(store, &near_config.genesis.config);

    let replay_store = create_replay_store(home_dir, &near_config);
    let epoch_manager_replay =
        EpochManager::new_arc_handle(replay_store, &near_config.genesis.config);

    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            tracing::trace!("Height: {}, header: {:#?}", height, header);

            let header_info = get_block_header_info(&header, &chain_store, epoch_manager.as_ref())
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to add chunk endorsements for block height {}: {:#}",
                        header.height(),
                        e
                    )
                });
            epoch_manager_replay.add_validator_proposals(header_info).unwrap().commit().unwrap();

            if epoch_manager
                .is_last_block_in_finished_epoch(&block_hash)
                .expect("Could not determine if block is last block in epoch")
            {
                let identifier = ValidatorInfoIdentifier::BlockHash(block_hash);
                let validator_info = epoch_manager.get_validator_info(identifier.clone()).unwrap();
                let validator_info_replay =
                    epoch_manager_replay.get_validator_info(identifier).unwrap();

                assert_eq!(validator_info.epoch_height, validator_info_replay.epoch_height);
                tracing::info!(
                    "Comparing validator infos for epoch height {}",
                    validator_info.epoch_height
                );

                assert_eq!(validator_info, validator_info_replay);
            }
        }
    }
}

fn get_block_header_info(
    header: &BlockHeader,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<BlockHeaderInfo, Error> {
    let mut header_info = BlockHeaderInfo::new(
        &header,
        chain_store.get_block_height(header.last_final_block()).unwrap(),
    );

    // Note(#11900): If stateless validation is enabled, we generate chunk endorsement bitmap
    // in the block header from the chunk endorsement signatures in the block body.

    let epoch_id = header.epoch_id();
    let protocol_version = epoch_manager.get_epoch_protocol_version(epoch_id)?;
    if !ProtocolFeature::StatelessValidation.enabled(protocol_version) {
        return Ok(header_info);
    }

    let shard_ids = epoch_manager.get_shard_layout(epoch_id)?.shard_ids().collect_vec();
    let mut bitmap = ChunkEndorsementsBitmap::new(shard_ids.len());

    let block = chain_store.get_block(header.hash())?;
    let shards_to_endorsements = block.chunk_endorsements().to_vec();
    assert_eq!(shards_to_endorsements.len(), shard_ids.len());

    let height = header.height();
    for shard_id in shard_ids.into_iter() {
        let assignments = epoch_manager
            .get_chunk_validator_assignments(epoch_id, shard_id, height)?
            .ordered_chunk_validators();
        let endorsements = &shards_to_endorsements[shard_id as usize];
        assert_eq!(assignments.len(), endorsements.len());
        bitmap
            .set(shard_id, endorsements.iter().map(|signature| signature.is_some()).collect_vec());
    }
    header_info.chunk_endorsements = Some(bitmap);
    Ok(header_info)
}

fn create_replay_store(home_dir: &Path, near_config: &NearConfig) -> Store {
    let store_opener = NodeStorage::opener(
        home_dir,
        near_config.config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    );
    let storage = store_opener.open_in_mode(Mode::ReadOnly).unwrap();

    let read_db = if storage.has_cold() {
        storage.get_split_db().unwrap()
    } else {
        storage.into_inner(Temperature::Hot)
    };
    let write_db = TestDB::new();
    Store::new(MixedDB::new(read_db, write_db, ReadOrder::WriteDBFirst))
}
