#[macro_use]
extern crate lazy_static;

pub use crate::client::Client;
pub use crate::client_actor::{start_client, ClientActor};
pub use crate::types::{
    Error, GetBlock, GetBlockProof, GetBlockProofResponse, GetBlockWithMerkleTree, GetChunk,
    GetExecutionOutcome, GetExecutionOutcomeResponse, GetGasPrice, GetNetworkInfo,
    GetNextLightClientBlock, GetStateChanges, GetStateChangesInBlock, GetValidatorInfo,
    GetValidatorOrdered, Query, Status, StatusResponse, SyncStatus, TxStatus, TxStatusError,
};
pub use crate::view_client::{start_view_client, ViewClientActor};

mod client;
mod client_actor;
mod info;
mod metrics;
pub mod sync;
pub mod test_utils;
mod types;
mod view_client;
