include!(concat!(env!("OUT_DIR"), "/chain.rs"));
include!(concat!(env!("OUT_DIR"), "/network.rs"));
include!(concat!(env!("OUT_DIR"), "/nightshade.rs"));
include!(concat!(env!("OUT_DIR"), "/receipt.rs"));
include!(concat!(env!("OUT_DIR"), "/signed_transaction.rs"));
include!(concat!(env!("OUT_DIR"), "/types.rs"));

pub use protobuf::Message;

#[cfg(feature = "with-serde")]
pub mod serde;
