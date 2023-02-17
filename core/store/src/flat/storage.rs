use std::collections::HashMap;

use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;
use near_primitives::types::{RawStateChangesWithTrieKey, ShardId};

use crate::Store;

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
    SavingDeltas,
    FetchingState,
    CatchingUp,
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

    fn add_block(&self, _block: BlockInfo, _changes: &[RawStateChangesWithTrieKey]) {
        match self.state {
            FlatStorageState::Disabled => {}
            FlatStorageState::SavingDeltas
            | FlatStorageState::FetchingState
            | FlatStorageState::CatchingUp => todo!("save delta to the disk"),
            FlatStorageState::Ready(ref _state) => todo!("add delta to the state"),
        }
    }

    fn chunk_view(&self, block: BlockHash) -> Result<FlatStorageChunkView, CreateChunkViewError> {
        todo!()
    }
}
