use near_async::messaging::AsyncSender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::ShardId;

/// Request to the chain to validate a state sync header.
#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "Result<(), near_chain::Error>")]
pub struct StateHeaderValidationRequestInner {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub header: ShardStateSyncResponseHeader,
}

pub type StateHeaderValidationRequest = SpanWrapped<StateHeaderValidationRequestInner>;

/// Request to the chain to finalize a state sync.
#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "Result<(), near_chain::Error>")]
pub struct ChainFinalizationRequestInner {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}

pub type ChainFinalizationRequest = SpanWrapped<ChainFinalizationRequestInner>;

#[derive(Clone, MultiSend, MultiSendMessage, MultiSenderFrom)]
pub struct ChainSenderForStateSync {
    pub state_header_validation:
        AsyncSender<StateHeaderValidationRequest, Result<(), near_chain::Error>>,
    pub chain_finalization: AsyncSender<ChainFinalizationRequest, Result<(), near_chain::Error>>,
}
