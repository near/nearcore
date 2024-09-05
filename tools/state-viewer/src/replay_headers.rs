use std::path::Path;

use itertools::Itertools;
use near_chain::BlockHeader;
use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::EpochManagerHandle;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::{BlockHeight, ValidatorInfoIdentifier};
use near_primitives::version::ProtocolFeature;
use near_store::db::{MixedDB, ReadOrder, TestDB};
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::NearConfig;

/// Replays the headers for the blocks between `start_height` and `end-height`.
/// If `start_height` is not set, uses the genesis height. If `end_height` is not set, uses the chain head.
/// The headers are replayed by updating the [`EpochManager`] with the headers (by calling `add_validator_proposals`)
/// and then comparing the resulting validator information ([`EpochValidatorInfo`]) in the original operation of
/// the chain (from the read-only store) and from the replay of the headers.
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

            let block_info = get_block_info(&header, &chain_store, epoch_manager.as_ref())
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to add chunk endorsements for block height {}: {:#}",
                        header.height(),
                        e
                    )
                });
            epoch_manager_replay
                .add_validator_proposals(block_info, *header.random_value())
                .unwrap()
                .commit()
                .unwrap();

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
                    "Comparing validator infos for epoch height {} block height {}",
                    validator_info.epoch_height,
                    height
                );

                assert_eq!(validator_info, validator_info_replay);
            }
        }
    }
}

/// Returns the [`BlockInfo`] corresponding to the header.
/// This function may override the resulting [`BlockInfo`] based on certain protocol versions.
fn get_block_info(
    header: &BlockHeader,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<BlockInfo, Error> {
    // Note(#11900): Until the chunk endorsements in block header are enabled, we generate the chunk endorsement bitmap
    // in the following from the chunk endorsement signatures in the block body.
    // TODO(#11900): Remove this code after ChunkEndorsementsInBlockHeader is stabilized.
    let protocol_version = epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
    let chunk_endorsements_bitmap: Option<ChunkEndorsementsBitmap> =
        if ProtocolFeature::StatelessValidation.enabled(protocol_version)
            && header.chunk_endorsements().is_none()
        {
            let block = chain_store.get_block(header.hash())?;
            let chunks = block.chunks();

            let endorsement_signatures = block.chunk_endorsements().to_vec();
            assert_eq!(endorsement_signatures.len(), chunks.len());

            let mut bitmap = ChunkEndorsementsBitmap::new(chunks.len());

            let height = header.height();
            let prev_block_epoch_id =
                epoch_manager.get_epoch_id_from_prev_block(header.prev_hash())?;
            for chunk_header in chunks.iter() {
                let shard_id = chunk_header.shard_id();
                let endorsements = &endorsement_signatures[shard_id as usize];
                if !chunk_header.is_new_chunk(height) {
                    assert_eq!(endorsements.len(), 0);
                    bitmap.add_endorsements(shard_id, vec![]);
                } else {
                    let assignments = epoch_manager
                        .get_chunk_validator_assignments(
                            &prev_block_epoch_id,
                            shard_id,
                            chunk_header.height_created(),
                        )?
                        .ordered_chunk_validators();
                    assert_eq!(endorsements.len(), assignments.len());
                    bitmap.add_endorsements(
                        shard_id,
                        endorsements.iter().map(|signature| signature.is_some()).collect_vec(),
                    );
                }
            }
            Some(bitmap)
        } else {
            None
        };
    Ok(BlockInfo::from_header_and_endorsements(
        &header,
        chain_store.get_block_height(header.last_final_block()).unwrap(),
        chunk_endorsements_bitmap,
    ))
}

/// Returns a stored that reads from the original chain store, but also allows writes to a temporary DB.
/// This allows to execute a new algorithm for EpochManager without changing the original chain data.
/// If the node has cold storage, the read DB is the split store (Hot+Cold). Write store is a TestDB (in memory).
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
