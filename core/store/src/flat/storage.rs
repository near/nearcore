use std::collections::HashMap;

use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};

use crate::{Store, StoreUpdate};

use super::chunk_view::FlatStorageChunkView;
use super::delta::FlastStorageDelta;
use super::types::{BlockHash, BlockInfo};

pub struct FlatStorage {
    shard_id: ShardId,
    store: Store,
    state: FlatStorageState,
}

enum FlatStorageState {
    Disabled,
    Empty,
    Creation,
    Ready(ReadyState),
}

struct ReadyState {
    flat_head: BlockHash,
    deltas: HashMap<BlockHash, FlastStorageDelta>,
}

enum CreateChunkViewError {
    Disabled,
    NotReady,
    UnknownBlock,
}

#[derive(thiserror::Error, Debug)]
pub enum AddBlockError {
    #[error("unknown block")]
    UnknownBlock,
}

impl FlatStorage {
    pub fn from_store(store: Store, shard_id: ShardId) -> Self {
        todo!("read state from db");
    }

    pub fn create_for_genesis(
        store: Store,
        shard_id: ShardId,
        genesis_block: &CryptoHash,
    ) -> Self {
        todo!("set flat head to genesis_block and return instance with ReadyState")
    }

    pub fn add_block(&self, _block: BlockInfo, _changes: &[RawStateChangesWithTrieKey]) -> Result<Option<StoreUpdate>, AddBlockError> {
        todo!("impl")
    }

    pub fn update_flat_head(&self, block: BlockHash) {
        todo!("chain/chain/src/chain.rs update_flat_storage_for_block")
    }

    pub fn clean(&self, shard_layout: ShardLayout) {
        todo!("core/store/src/flat_state.rs clear_state")
    }

    fn chunk_view(&self, block: BlockHash) -> Result<FlatStorageChunkView, CreateChunkViewError> {
        todo!()
    }
}
