#[macro_use]
extern crate lazy_static;

pub use near_client_primitives::types::{
    Error, GetBlock, GetBlockProof, GetBlockProofResponse, GetBlockWithMerkleTree, GetChunk,
    GetExecutionOutcome, GetExecutionOutcomeResponse, GetExecutionOutcomesForBlock, GetGasPrice,
    GetNetworkInfo, GetNextLightClientBlock, GetProtocolConfig, GetReceipt, GetStateChanges,
    GetStateChangesInBlock, GetStateChangesWithCauseInBlock, GetValidatorInfo, GetValidatorOrdered,
    Query, Status, StatusResponse, SyncStatus, TxStatus, TxStatusError,
};

pub use crate::client::Client;
pub use crate::client_actor::{start_client, ClientActor};
#[cfg(feature = "adversarial")]
pub use crate::view_client::AdversarialControls;
pub use crate::view_client::{start_view_client, ViewClientActor};

mod client;
mod client_actor;
mod info;
mod metrics;
pub mod sync;
pub mod test_utils;
mod view_client;
