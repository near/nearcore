#![cfg_attr(enable_const_type_id, feature(const_type_id))]

pub use block_processing_utils::BlockProcessingArtifact;
pub use chain::{Chain, check_known, collect_receipts};
pub use chain_update::ChainUpdate;
pub use doomslug::{Doomslug, DoomslugBlockProductionReadiness, DoomslugThresholdMode};
pub use lightclient::{create_light_client_block_view, get_epoch_block_producers_view};
pub use near_chain_primitives::{self, Error};
pub use near_primitives::receipt::ReceiptResult;
pub use store::utils::{
    check_transaction_validity_period, get_chunk_clone_from_header,
    get_incoming_receipts_for_shard, retrieve_headers,
};
pub use store::{
    ChainStore, ChainStoreAccess, ChainStoreUpdate, LatestWitnessesInfo, MerkleProofAccess,
    ReceiptFilter,
};
pub use store_validator::{ErrorMessage, StoreValidator};
pub use types::{Block, BlockHeader, BlockStatus, ChainGenesis, LatestKnown, Provenance};

mod approval_verification;
mod block_processing_utils;
pub mod blocks_delay_tracker;
pub mod chain;
mod chain_update;
pub mod crypto_hash_timer;
mod doomslug;
pub mod flat_storage_init;
pub mod flat_storage_resharder;
mod garbage_collection;
pub mod genesis;
mod lightclient;
pub mod metrics;
pub mod missing_chunks;
pub mod orphan;
pub mod pending;
pub mod rayon_spawner;
pub mod resharding;
pub mod runtime;
pub mod sharding;
pub mod signature_verification;
pub mod state_snapshot_actor;
mod state_sync;
pub mod stateless_validation;
mod store;
pub mod store_validator;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod types;
pub mod update_shard;
pub mod validate;

#[cfg(feature = "byzantine_asserts")]
#[macro_export]
macro_rules! byzantine_assert {
    ($cond: expr) => {
        assert!($cond)
    };
}

#[cfg(not(feature = "byzantine_asserts"))]
#[macro_export]
macro_rules! byzantine_assert {
    ($cond: expr) => {};
}
