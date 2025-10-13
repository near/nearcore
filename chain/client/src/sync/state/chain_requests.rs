use near_async::messaging::AsyncSender;
use near_async::{Message, MultiSend, MultiSenderFrom};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::ShardId;

/// Request to the chain to validate a state sync header.
#[derive(Message, Debug, Clone)]
pub struct StateHeaderValidationRequest {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub header: ShardStateSyncResponseHeader,
}

/// Request to the chain to finalize a state sync.
#[derive(Message, Debug, Clone)]
pub struct ChainFinalizationRequest {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ChainSenderForStateSync {
    pub state_header_validation:
        AsyncSender<SpanWrapped<StateHeaderValidationRequest>, Result<(), near_chain::Error>>,
    pub chain_finalization:
        AsyncSender<SpanWrapped<ChainFinalizationRequest>, Result<(), near_chain::Error>>,
}
