use std::path::Path;

use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::{types::BlockHeaderInfo, EpochManager};
use near_primitives::types::{BlockHeight, ValidatorInfoIdentifier};
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
            epoch_manager_replay
                .add_validator_proposals(BlockHeaderInfo::new(
                    &header,
                    chain_store.get_block_height(header.last_final_block()).unwrap(),
                ))
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
                    "Comparing validator infos for epoch height {}",
                    validator_info.epoch_height
                );

                assert_eq!(validator_info, validator_info_replay);
            }
        }
    }
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
