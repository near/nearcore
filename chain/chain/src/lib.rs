#[macro_use]
extern crate lazy_static;

pub use chain::{collect_receipts, Chain, ChainGenesis, MAX_ORPHAN_SIZE};
pub use error::{Error, ErrorKind};
pub use store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
pub use types::{
    Block, BlockApproval, BlockHeader, BlockStatus, Provenance, ReceiptResult, RuntimeAdapter, Tip,
    Weight,
};

pub mod chain;
mod error;
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
