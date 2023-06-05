//! FlatStorage is created as an additional representation of the state alongside with Tries.
//! It simply stores a mapping for all key value pairs stored in our Tries (leaves of the trie)
//! It is used for fast key value look up in the state. Reading a single key/value in trie
//! requires traversing the trie from the root, loading many nodes from the database. In flat storage,
//! we store a mapping from keys to value references so that key value lookup will only require two
//! db accesses - one to get value reference, one to get the value itself. In fact, in the case of small
//! values, flat storage only needs one read, because value will be stored in the mapping instead of
//! value ref.
//!
//! The main challenge in the flat storage implementation is that we need to able to handle forks,
//! so the flat storage API must support key value lookups for different blocks.
//! To achieve that, we store the key value pairs of the state on a block (head of the flat storage)
//! on disk and also store the change deltas for some other blocks in memory. With these deltas,
//! we can perform lookups for the other blocks. See comments in `FlatStorage` to see
//! which block should be the head of flat storage and which other blocks do flat storage support.
//!
//! This file contains the implementation of FlatStorage. It has three essential structs.
//!
//! `FlatStorageChunkView`: this provides an interface to get value or value references from flat storage. This
//!              is the struct that will be stored as part of Trie. All trie reads will be directed
//!              to the flat state.
//! `FlatStorageManager`: this is to construct flat state.
//! `FlatStorage`: this stores some information about the state of the flat storage itself,
//!                     for example, all block deltas that are stored in flat storage and a representation
//!                     of the chain formed by these blocks (because we can't access ChainStore
//!                     inside flat storage).

mod chunk_view;
pub mod delta;
mod inlining_migration;
mod manager;
mod metrics;
mod storage;
pub mod store_helper;
mod types;

pub use chunk_view::FlatStorageChunkView;
pub use delta::{FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata};
pub use inlining_migration::{inline_flat_state_values, FlatStateValuesInliningMigrationHandle};
pub use manager::FlatStorageManager;
pub use metrics::FlatStorageCreationMetrics;
pub use storage::FlatStorage;
pub use types::{
    BlockInfo, FetchingStateStatus, FlatStateIterator, FlatStorageCreationStatus, FlatStorageError,
    FlatStorageReadyStatus, FlatStorageStatus,
};

pub(crate) const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Number of traversed parts during a single step of fetching state.
pub const NUM_PARTS_IN_ONE_STEP: u64 = 20;

/// Memory limit for state part being fetched.
pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(10 * bytesize::MIB);
