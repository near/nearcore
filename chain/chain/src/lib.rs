#[macro_use]
extern crate lazy_static;

pub use chain::{check_refcount_map, collect_receipts, Chain, ChainGenesis, MAX_ORPHAN_SIZE};
pub use doomslug::{Doomslug, DoomslugBlockProductionReadiness, DoomslugThresholdMode};
pub use error::{Error, ErrorKind};
pub use lightclient::create_light_client_block_view;
pub use store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
pub use types::{Block, BlockHeader, BlockStatus, Provenance, ReceiptResult, RuntimeAdapter, Tip};

pub mod chain;
mod doomslug;
mod error;
mod lightclient;
mod metrics;
mod store;
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
