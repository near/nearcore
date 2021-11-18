#[macro_use]
extern crate lazy_static;

pub use chain::{collect_receipts, Chain, MAX_ORPHAN_SIZE};
pub use doomslug::{Doomslug, DoomslugBlockProductionReadiness, DoomslugThresholdMode};
pub use lightclient::{create_light_client_block_view, get_epoch_block_producers_view};
pub use near_chain_primitives::{self, Error, ErrorKind};
pub use near_primitives::receipt::ReceiptResult;
pub use store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
pub use store_validator::{ErrorMessage, StoreValidator};
pub use types::{Block, BlockHeader, BlockStatus, ChainGenesis, Provenance, RuntimeAdapter};

pub mod chain;
mod doomslug;
mod lightclient;
mod metrics;
pub mod migrations;
pub mod missing_chunks;
mod store;
pub mod store_validator;
pub mod test_utils;
pub mod types;
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
