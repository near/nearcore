use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::block::Tip;
use near_store::flat::{FlatStorageManager, FlatStorageStatus};

use crate::flat_storage_resharder::FlatStorageResharder;
use crate::{Chain, ChainStoreAccess};

impl Chain {
    pub fn init_flat_storage(&self) -> Result<(), Error> {
        let chain_head = self.chain_store().head()?;
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        init_flat_storage_for_current_epoch(
            &chain_head,
            self.epoch_manager.as_ref(),
            &flat_storage_manager,
            &self.resharding_manager.flat_storage_resharder,
        )?;

        // Create flat storage for the shards in the next epoch. This only
        // matters during resharding where the shards in the next epoch are
        // different than in the current epoch. The flat storage for the
        // children shards is initially created at the end of the resharding.
        // This method here is only needed when the node is restarted after
        // resharding is finished but before switching to the new shard layout.
        init_flat_storage_for_next_epoch(
            &chain_head,
            self.epoch_manager.as_ref(),
            &flat_storage_manager,
        )?;

        Ok(())
    }
}

fn init_flat_storage_for_current_epoch(
    chain_head: &Tip,
    epoch_manager: &dyn EpochManagerAdapter,
    flat_storage_manager: &FlatStorageManager,
    flat_storage_resharder: &FlatStorageResharder,
) -> Result<(), Error> {
    let epoch_id = &chain_head.epoch_id;
    tracing::debug!(target: "chain", ?epoch_id, "init flat storage for the current epoch");

    let shard_ids = epoch_manager.shard_ids(epoch_id)?;
    for shard_id in shard_ids {
        let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &chain_head.epoch_id)?;
        let status = flat_storage_manager.get_flat_storage_status(shard_uid);
        match status {
            FlatStorageStatus::Ready(_) => {
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
            FlatStorageStatus::Creation(_) => {
                panic!("Flat storage creation is no longer supported");
            }
            FlatStorageStatus::Empty | FlatStorageStatus::Disabled => {}
            FlatStorageStatus::Resharding(status) => {
                flat_storage_resharder.resume(shard_uid, &status)?;
            }
        }
    }
    Ok(())
}

fn init_flat_storage_for_next_epoch(
    chain_head: &Tip,
    epoch_manager: &dyn EpochManagerAdapter,
    flat_storage_manager: &FlatStorageManager,
) -> Result<(), Error> {
    if !epoch_manager.will_shard_layout_change(&chain_head.last_block_hash)? {
        return Ok(());
    }

    let next_epoch_id = &chain_head.next_epoch_id;
    tracing::debug!(target: "chain", ?next_epoch_id, "init flat storage for the next epoch");

    let shard_ids = epoch_manager.shard_ids(next_epoch_id)?;
    for shard_id in shard_ids {
        let shard_uid = shard_id_to_uid(epoch_manager, shard_id, next_epoch_id)?;
        let status = flat_storage_manager.get_flat_storage_status(shard_uid);
        match status {
            FlatStorageStatus::Ready(_) => {
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
            FlatStorageStatus::Creation(_) => {
                panic!("Flat storage creation is no longer supported");
            }
            FlatStorageStatus::Empty | FlatStorageStatus::Disabled => {}
            FlatStorageStatus::Resharding(_) => {
                // The flat storage for children shards will be created
                // separately in the resharding process.
            }
        }
    }
    Ok(())
}
