use near_async::messaging::AsyncSender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::ShardId;

/// Request to the chain to validate a state sync header.
#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "Result<(), near_chain::Error>")]
pub struct StateHeaderValidationRequest {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub header: ShardStateSyncResponseHeader,
}

/// Request to the chain to finalize a state sync.
#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "Result<(), near_chain::Error>")]
pub struct ChainFinalizationRequest {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

#[derive(Clone, MultiSend, MultiSendMessage, MultiSenderFrom)]
pub struct ChainSenderForStateSync {
    pub state_header_validation:
        AsyncSender<StateHeaderValidationRequest, Result<(), near_chain::Error>>,
    pub chain_finalization: AsyncSender<ChainFinalizationRequest, Result<(), near_chain::Error>>,
}
