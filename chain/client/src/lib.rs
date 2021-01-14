#[macro_use]
extern crate lazy_static;

#[cfg(feature = "adversarial")]
pub use crate::view_client::AdversarialControls;
pub use crate::{
    client::Client,
    client_actor::{start_client, ClientActor},
    types::{
        Error, GetBlock, GetBlockProof, GetBlockProofResponse, GetBlockWithMerkleTree, GetChunk,
        GetExecutionOutcome, GetExecutionOutcomeResponse, GetExecutionOutcomesForBlock,
        GetGasPrice, GetNetworkInfo, GetNextLightClientBlock, GetReceipt, GetStateChanges,
        GetStateChangesInBlock, GetStateChangesWithCauseInBlock, GetValidatorInfo,
        GetValidatorOrdered, Query, Status, StatusResponse, SyncStatus, TxStatus, TxStatusError,
    },
    view_client::{start_view_client, ViewClientActor},
};

mod client;
mod client_actor;
mod info;
mod metrics;
pub mod sync;
pub mod test_utils;
mod types;
mod view_client;
