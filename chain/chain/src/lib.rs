#![cfg_attr(enable_const_type_id, feature(const_type_id))]

pub use block_processing_utils::BlockProcessingArtifact;
pub use chain::{check_known, collect_receipts, Chain};
pub use chain_update::ChainUpdate;
pub use doomslug::{Doomslug, DoomslugBlockProductionReadiness, DoomslugThresholdMode};
pub use lightclient::{create_light_client_block_view, get_epoch_block_producers_view};
pub use near_chain_primitives::{self, Error};
pub use near_primitives::receipt::ReceiptResult;
pub use store::{ChainStore, ChainStoreAccess, ChainStoreUpdate, LatestWitnessesInfo};
pub use store_validator::{ErrorMessage, StoreValidator};
pub use types::{Block, BlockHeader, BlockStatus, ChainGenesis, LatestKnown, Provenance};

mod block_processing_utils;
pub mod blocks_delay_tracker;
pub mod chain;
mod chain_update;
pub mod chunks_store;
pub mod crypto_hash_timer;
mod doomslug;
pub mod flat_storage_creator;
pub mod flat_storage_resharder;
mod garbage_collection;
mod lightclient;
pub mod metrics;
pub mod migrations;
pub mod missing_chunks;
pub mod orphan;
pub mod resharding;
pub mod runtime;
mod state_request_tracker;
pub mod state_snapshot_actor;
pub mod stateless_validation;
mod store;
pub mod store_validator;
pub mod test_utils;
pub mod types;
pub mod validate;

pub mod rayon_spawner;
pub mod sharding;
#[cfg(test)]
mod tests;
pub mod update_shard;

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
