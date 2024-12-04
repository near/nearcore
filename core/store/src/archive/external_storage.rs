use near_primitives::{shard_layout::ShardLayout, types::BlockHeight};

use crate::archive::cold_storage::ColdMigrationStore;
use crate::{metrics, DBCol, Store};
use std::io;

use super::ExternalStorage;

/// Updates the archival storage for the block at the given height.
pub(crate) fn update_external_storage(
    _storage: &dyn ExternalStorage,
    hot_store: &Store,
    _shard_layout: &ShardLayout,
    height: &BlockHeight,
    _num_threads: usize,
) -> io::Result<bool> {
    let _span = tracing::debug_span!(target: "cold_store", "ArchivalStore::update_for_block", height = height);
    let _timer = metrics::COLD_COPY_DURATION.start_timer();

    if hot_store.get_for_cold(DBCol::BlockHeight, &height.to_le_bytes())?.is_none() {
        return Ok(false);
    }

    unimplemented!()
}
