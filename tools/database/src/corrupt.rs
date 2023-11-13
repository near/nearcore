use crate::utils::open_state_snapshot;
use clap::Parser;
use near_primitives::shard_layout::ShardLayout;
use near_store::{flat::FlatStorageManager, ShardUId, StoreUpdate};
use std::path::PathBuf;

#[derive(Parser)]
pub(crate) struct CorruptStateSnapshotCommand {}

impl CorruptStateSnapshotCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let store = open_state_snapshot(home, near_store::Mode::ReadWrite)?;
        let flat_storage_manager = FlatStorageManager::new(store.clone());

        let mut store_update = store.store_update();
        // TODO there must be a better way to get the shard uids
        // This only works for the V1 shard layout.
        let shard_uids = ShardLayout::get_simple_nightshade_layout().get_shard_uids();
        for shard_uid in shard_uids {
            corrupt(&mut store_update, &flat_storage_manager, shard_uid)?;
        }
        store_update.commit().unwrap();

        println!("corrupted the state snapshot");

        Ok(())
    }
}

fn corrupt(
    store_update: &mut StoreUpdate,
    flat_storage_manager: &FlatStorageManager,
    shard_uid: ShardUId,
) -> Result<(), anyhow::Error> {
    flat_storage_manager.create_flat_storage_for_shard(shard_uid)?;
    let result = flat_storage_manager.remove_flat_storage_for_shard(shard_uid, store_update)?;
    println!("removed flat storage for shard {shard_uid:?} result is {result}");
    Ok(())
}
