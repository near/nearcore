use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;

use crate::Store;

use super::FlatStorage;

/// Struct for getting value references from the flat storage, corresponding
/// to some block defined in `blocks_to_head`.
///
/// The main interface is the `get_ref` method, which is called in `Trie::get`
/// and `Trie::get_ref` because they are the same for each shard and they are
/// requested only once during applying chunk.
// TODO (#7327): lock flat state when `get_ref` is called or head is being updated. Otherwise, `apply_chunks` and
// `postprocess_block` parallel execution may corrupt the state.
#[derive(Clone)]
pub struct FlatStorageChunkView {
    /// Used to access flat state stored at the head of flat storage.
    /// It should store all trie keys and values/value refs for the state on top of
    /// flat_storage.head, except for delayed receipt keys.
    #[allow(unused)]
    store: Store,
    /// The block for which key-value pairs of its state will be retrieved. The flat state
    /// will reflect the state AFTER the block is applied.
    block_hash: CryptoHash,
    /// Stores the state of the flat storage, for example, where the head is at and which
    /// blocks' state are stored in flat storage.
    flat_storage: FlatStorage,
}

impl FlatStorageChunkView {
    pub fn new(store: Store, block_hash: CryptoHash, flat_storage: FlatStorage) -> Self {
        Self { store, block_hash, flat_storage }
    }
    /// Returns value reference using raw trie key, taken from the state
    /// corresponding to `FlatStorageChunkView::block_hash`.
    ///
    /// To avoid duplication, we don't store values themselves in flat state,
    /// they are stored in `DBCol::State`. Also the separation is done so we
    /// could charge users for the value length before loading the value.
    // TODO (#7327): consider inlining small values, so we could use only one db access.
    pub fn get_ref(&self, key: &[u8]) -> Result<Option<ValueRef>, crate::StorageError> {
        self.flat_storage.get_ref(&self.block_hash, key)
    }
}
