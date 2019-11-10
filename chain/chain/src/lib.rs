#[macro_use]
extern crate lazy_static;

pub use chain::{collect_receipts, Chain, ChainGenesis, MAX_ORPHAN_SIZE};
pub use error::{Error, ErrorKind};
pub use finality::{FinalityGadget, FinalityGadgetQuorums};
pub use store::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
pub use types::{
    Block, BlockHeader, BlockStatus, Provenance, ReceiptResult, RuntimeAdapter, Tip,
    ValidTransaction, Weight,
};

pub mod chain;
mod error;
mod finality;
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
