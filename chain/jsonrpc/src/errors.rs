use near_jsonrpc_primitives::errors::{RpcError, ServerError};

macro_rules! _rpc_try {
    ($val:expr) => {
        match $val.into_rpc_result() {
            Ok(val) => val,
            Err(err) => return Err(err),
        }
    };
}

pub(crate) use _rpc_try as rpc_try;

pub trait RpcFrom<T> {
    fn rpc_from(_: T) -> Self;
}

pub trait IntoRpcResult<T, E> {
    fn into_rpc_result(self) -> Result<T, E>;
}

impl<T, E, F> IntoRpcResult<T, F> for Result<T, E>
where
    F: RpcFrom<E>,
{
    fn into_rpc_result(self) -> Result<T, F> {
        self.map_err(RpcFrom::rpc_from)
    }
}

impl<B, E, T, A> IntoRpcResult<T, E> for Result<Result<T, B>, A>
where
    E: RpcFrom<A>,
    E: RpcFrom<B>,
{
    fn into_rpc_result(self) -> Result<T, E> {
        self.map_err(RpcFrom::rpc_from).and_then(|res| res.map_err(RpcFrom::rpc_from))
    }
}

// --

impl RpcFrom<actix::MailboxError> for RpcError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        RpcError::new(
            -32_000,
            "Server error".to_string(),
            Some(serde_json::Value::String(error.to_string())),
        )
    }
}

impl RpcFrom<actix::MailboxError> for ServerError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        match error {
            actix::MailboxError::Closed => ServerError::Closed,
            actix::MailboxError::Timeout => ServerError::Timeout,
        }
    }
}

impl RpcFrom<near_primitives::errors::InvalidTxError> for ServerError {
    fn rpc_from(e: near_primitives::errors::InvalidTxError) -> ServerError {
        ServerError::TxExecutionError(near_primitives::errors::TxExecutionError::InvalidTxError(e))
    }
}

// -- block --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::blocks::RpcBlockError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetBlockError>
    for near_jsonrpc_primitives::types::blocks::RpcBlockError
{
    fn rpc_from(error: near_client_primitives::types::GetBlockError) -> Self {
        match error {
            near_client_primitives::types::GetBlockError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetBlockError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcBlockError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- block / changes --

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::changes::RpcStateChangesError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetBlockError>
    for near_jsonrpc_primitives::types::changes::RpcStateChangesError
{
    fn rpc_from(error: near_client_primitives::types::GetBlockError) -> Self {
        match error {
            near_client_primitives::types::GetBlockError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetBlockError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<near_client_primitives::types::GetStateChangesError>
    for near_jsonrpc_primitives::types::changes::RpcStateChangesError
{
    fn rpc_from(error: near_client_primitives::types::GetStateChangesError) -> Self {
        match error {
            near_client_primitives::types::GetStateChangesError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetStateChangesError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetStateChangesError::NotSyncedYet => Self::NotSyncedYet,
            near_client_primitives::types::GetStateChangesError::Unreachable {
                ref error_message,
            } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- changes / chunk --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::chunks::RpcChunkError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_jsonrpc_primitives::types::chunks::ChunkReference>
    for near_client_primitives::types::GetChunk
{
    fn rpc_from(chunk_reference: near_jsonrpc_primitives::types::chunks::ChunkReference) -> Self {
        match chunk_reference {
            near_jsonrpc_primitives::types::chunks::ChunkReference::BlockShardId {
                block_id,
                shard_id,
            } => match block_id {
                near_primitives::types::BlockId::Height(height) => Self::Height(height, shard_id),
                near_primitives::types::BlockId::Hash(block_hash) => {
                    Self::BlockHash(block_hash, shard_id)
                }
            },
            near_jsonrpc_primitives::types::chunks::ChunkReference::ChunkHash { chunk_id } => {
                Self::ChunkHash(chunk_id.into())
            }
        }
    }
}

impl RpcFrom<near_client_primitives::types::GetChunkError>
    for near_jsonrpc_primitives::types::chunks::RpcChunkError
{
    fn rpc_from(error: near_client_primitives::types::GetChunkError) -> Self {
        match error {
            near_client_primitives::types::GetChunkError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetChunkError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetChunkError::InvalidShardId { shard_id } => {
                Self::InvalidShardId { shard_id }
            }
            near_client_primitives::types::GetChunkError::UnknownChunk { chunk_hash } => {
                Self::UnknownChunk { chunk_hash }
            }
            near_client_primitives::types::GetChunkError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcChunkError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- chunk / config --

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::config::RpcProtocolConfigError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetProtocolConfigError>
    for near_jsonrpc_primitives::types::config::RpcProtocolConfigError
{
    fn rpc_from(error: near_client_primitives::types::GetProtocolConfigError) -> Self {
        match error {
            near_client_primitives::types::GetProtocolConfigError::UnknownBlock(error_message) => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetProtocolConfigError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetProtocolConfigError::Unreachable(
                ref error_message,
            ) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcProtocolConfigError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- config / gas_price --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::gas_price::RpcGasPriceError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetGasPriceError>
    for near_jsonrpc_primitives::types::gas_price::RpcGasPriceError
{
    fn rpc_from(error: near_client_primitives::types::GetGasPriceError) -> Self {
        match error {
            near_client_primitives::types::GetGasPriceError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetGasPriceError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetGasPriceError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcGasPriceError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- gas_price / light_client --

impl RpcFrom<Option<std::sync::Arc<near_primitives::views::LightClientBlockView>>>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse
{
    fn rpc_from(
        light_client_block: Option<std::sync::Arc<near_primitives::views::LightClientBlockView>>,
    ) -> Self {
        Self { light_client_block }
    }
}

impl RpcFrom<near_client_primitives::types::GetExecutionOutcomeError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientProofError
{
    fn rpc_from(error: near_client_primitives::types::GetExecutionOutcomeError) -> Self {
        match error {
            near_client_primitives::types::GetExecutionOutcomeError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            },
            near_client_primitives::types::GetExecutionOutcomeError::InconsistentState {
                number_or_shards, execution_outcome_shard_id
            } => Self::InconsistentState { number_or_shards, execution_outcome_shard_id },
            near_client_primitives::types::GetExecutionOutcomeError::NotConfirmed {
                transaction_or_receipt_id
            } => Self::NotConfirmed { transaction_or_receipt_id },
            near_client_primitives::types::GetExecutionOutcomeError::UnknownTransactionOrReceipt {
                transaction_or_receipt_id
            } => Self::UnknownTransactionOrReceipt { transaction_or_receipt_id },
            near_client_primitives::types::GetExecutionOutcomeError::UnavailableShard {
                transaction_or_receipt_id,
                shard_id
            } => Self::UnavailableShard { transaction_or_receipt_id, shard_id },
            near_client_primitives::types::GetExecutionOutcomeError::InternalError { error_message } => {
                Self::InternalError { error_message }
            },
            near_client_primitives::types::GetExecutionOutcomeError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT.with_label_values(
                    &["RpcLightClientProofError"],
                ).inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientProofError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetBlockProofError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientProofError
{
    fn rpc_from(error: near_client_primitives::types::GetBlockProofError) -> Self {
        match error {
            near_client_primitives::types::GetBlockProofError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockProofError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockProofError::Unreachable {
                ref error_message,
            } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcLightClientProofError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetNextLightClientBlockError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError
{
    fn rpc_from(error: near_client_primitives::types::GetNextLightClientBlockError) -> Self {
        match error {
            near_client_primitives::types::GetNextLightClientBlockError::InternalError {
                error_message,
            } => Self::InternalError { error_message },
            near_client_primitives::types::GetNextLightClientBlockError::UnknownBlock {
                error_message,
            } => Self::UnknownBlock { error_message },
            near_client_primitives::types::GetNextLightClientBlockError::EpochOutOfBounds {
                epoch_id,
            } => Self::EpochOutOfBounds { epoch_id },
            near_client_primitives::types::GetNextLightClientBlockError::Unreachable {
                ref error_message,
            } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcLightClientNextBlockError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- light_client / network_info --

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_network_primitives::types::PeerInfo>
    for near_jsonrpc_primitives::types::network_info::RpcPeerInfo
{
    fn rpc_from(peer_info: near_network_primitives::types::PeerInfo) -> Self {
        Self { id: peer_info.id, addr: peer_info.addr, account_id: peer_info.account_id }
    }
}

impl RpcFrom<near_network_primitives::types::KnownProducer>
    for near_jsonrpc_primitives::types::network_info::RpcKnownProducer
{
    fn rpc_from(known_producer: near_network_primitives::types::KnownProducer) -> Self {
        Self {
            account_id: known_producer.account_id,
            addr: known_producer.addr,
            peer_id: known_producer.peer_id,
        }
    }
}

impl RpcFrom<near_client_primitives::types::NetworkInfoResponse>
    for near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse
{
    fn rpc_from(network_info_response: near_client_primitives::types::NetworkInfoResponse) -> Self {
        Self {
            active_peers: network_info_response
                .connected_peers
                .iter()
                .map(|pi| RpcFrom::rpc_from(pi.clone()))
                .collect(),
            num_active_peers: network_info_response.num_connected_peers,
            peer_max_count: network_info_response.peer_max_count,
            sent_bytes_per_sec: network_info_response.sent_bytes_per_sec,
            received_bytes_per_sec: network_info_response.received_bytes_per_sec,
            known_producers: network_info_response
                .known_producers
                .iter()
                .map(|kp| RpcFrom::rpc_from(kp.clone()))
                .collect(),
        }
    }
}

impl RpcFrom<String> for near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError {
    fn rpc_from(error_message: String) -> Self {
        Self::InternalError { error_message }
    }
}

// -- network_info / query --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::query::RpcQueryError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::QueryError>
    for near_jsonrpc_primitives::types::query::RpcQueryError
{
    fn rpc_from(error: near_client_primitives::types::QueryError) -> Self {
        match error {
            near_client_primitives::types::QueryError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::QueryError::NoSyncedBlocks => Self::NoSyncedBlocks,
            near_client_primitives::types::QueryError::UnavailableShard { requested_shard_id } => {
                Self::UnavailableShard { requested_shard_id }
            }
            near_client_primitives::types::QueryError::UnknownBlock { block_reference } => {
                Self::UnknownBlock { block_reference }
            }
            near_client_primitives::types::QueryError::GarbageCollectedBlock {
                block_height,
                block_hash,
            } => Self::GarbageCollectedBlock { block_height, block_hash },
            near_client_primitives::types::QueryError::InvalidAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::InvalidAccount { requested_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::UnknownAccount {
                requested_account_id,
                block_height,
                block_hash,
            } => Self::UnknownAccount { requested_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::NoContractCode {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::NoContractCode { contract_account_id, block_height, block_hash },
            near_client_primitives::types::QueryError::UnknownAccessKey {
                public_key,
                block_height,
                block_hash,
            } => Self::UnknownAccessKey { public_key, block_height, block_hash },
            near_client_primitives::types::QueryError::ContractExecutionError {
                vm_error,
                block_height,
                block_hash,
            } => Self::ContractExecutionError { vm_error, block_height, block_hash },
            near_client_primitives::types::QueryError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcQueryError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
            near_client_primitives::types::QueryError::TooLargeContractState {
                contract_account_id,
                block_height,
                block_hash,
            } => Self::TooLargeContractState { contract_account_id, block_height, block_hash },
        }
    }
}

impl RpcFrom<near_primitives::views::QueryResponse>
    for near_jsonrpc_primitives::types::query::RpcQueryResponse
{
    fn rpc_from(query_response: near_primitives::views::QueryResponse) -> Self {
        Self {
            kind: RpcFrom::rpc_from(query_response.kind),
            block_hash: query_response.block_hash,
            block_height: query_response.block_height,
        }
    }
}

impl RpcFrom<near_primitives::views::QueryResponseKind>
    for near_jsonrpc_primitives::types::query::QueryResponseKind
{
    fn rpc_from(query_response_kind: near_primitives::views::QueryResponseKind) -> Self {
        match query_response_kind {
            near_primitives::views::QueryResponseKind::ViewAccount(account_view) => {
                Self::ViewAccount(account_view)
            }
            near_primitives::views::QueryResponseKind::ViewCode(contract_code_view) => {
                Self::ViewCode(contract_code_view)
            }
            near_primitives::views::QueryResponseKind::ViewState(view_state_result) => {
                Self::ViewState(view_state_result)
            }
            near_primitives::views::QueryResponseKind::CallResult(call_result) => {
                Self::CallResult(call_result)
            }
            near_primitives::views::QueryResponseKind::AccessKey(access_key_view) => {
                Self::AccessKey(access_key_view)
            }
            near_primitives::views::QueryResponseKind::AccessKeyList(access_key_list) => {
                Self::AccessKeyList(access_key_list)
            }
        }
    }
}

// -- query / receipts --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::receipts::RpcReceiptError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_jsonrpc_primitives::types::receipts::ReceiptReference>
    for near_client_primitives::types::GetReceipt
{
    fn rpc_from(
        receipt_reference: near_jsonrpc_primitives::types::receipts::ReceiptReference,
    ) -> Self {
        Self { receipt_id: receipt_reference.receipt_id }
    }
}

impl RpcFrom<near_client_primitives::types::GetReceiptError>
    for near_jsonrpc_primitives::types::receipts::RpcReceiptError
{
    fn rpc_from(error: near_client_primitives::types::GetReceiptError) -> Self {
        match error {
            near_client_primitives::types::GetReceiptError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetReceiptError::UnknownReceipt(hash) => {
                Self::UnknownReceipt { receipt_id: hash }
            }
            near_client_primitives::types::GetReceiptError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcReceiptError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- receipts / sandbox --

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::sandbox::RpcSandboxPatchStateError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::sandbox::RpcSandboxFastForwardError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

// -- sandbox / status --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::status::RpcStatusError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_primitives::views::StatusResponse>
    for near_jsonrpc_primitives::types::status::RpcStatusResponse
{
    fn rpc_from(status_response: near_primitives::views::StatusResponse) -> Self {
        Self { status_response }
    }
}

impl RpcFrom<near_primitives::views::StatusResponse>
    for near_jsonrpc_primitives::types::status::RpcHealthResponse
{
    fn rpc_from(_status_response: near_primitives::views::StatusResponse) -> Self {
        Self {}
    }
}

impl RpcFrom<near_client_primitives::types::StatusError>
    for near_jsonrpc_primitives::types::status::RpcStatusError
{
    fn rpc_from(error: near_client_primitives::types::StatusError) -> Self {
        match error {
            near_client_primitives::types::StatusError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::StatusError::NodeIsSyncing => Self::NodeIsSyncing,
            near_client_primitives::types::StatusError::NoNewBlocks { elapsed } => {
                Self::NoNewBlocks { elapsed }
            }
            near_client_primitives::types::StatusError::EpochOutOfBounds { epoch_id } => {
                Self::EpochOutOfBounds { epoch_id }
            }
            near_client_primitives::types::StatusError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStatusError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- status / transactions --

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::transactions::RpcTransactionError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::TxStatusError>
    for near_jsonrpc_primitives::types::transactions::RpcTransactionError
{
    fn rpc_from(error: near_client_primitives::types::TxStatusError) -> Self {
        match error {
            near_client_primitives::types::TxStatusError::ChainError(err) => {
                Self::InternalError { debug_info: format!("{:?}", err) }
            }
            near_client_primitives::types::TxStatusError::MissingTransaction(
                requested_transaction_hash,
            ) => Self::UnknownTransaction { requested_transaction_hash },
            near_client_primitives::types::TxStatusError::InvalidTx(context) => {
                Self::InvalidTransaction { context }
            }
            near_client_primitives::types::TxStatusError::InternalError(debug_info) => {
                Self::InternalError { debug_info }
            }
            near_client_primitives::types::TxStatusError::TimeoutError => Self::TimeoutError,
        }
    }
}

impl RpcFrom<near_primitives::views::FinalExecutionOutcomeViewEnum>
    for near_jsonrpc_primitives::types::transactions::RpcTransactionResponse
{
    fn rpc_from(
        final_execution_outcome: near_primitives::views::FinalExecutionOutcomeViewEnum,
    ) -> Self {
        Self { final_execution_outcome }
    }
}

// -- transactions / validator --

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::validator::RpcValidatorError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<near_client_primitives::types::GetValidatorInfoError>
    for near_jsonrpc_primitives::types::validator::RpcValidatorError
{
    fn rpc_from(error: near_client_primitives::types::GetValidatorInfoError) -> Self {
        match error {
            near_client_primitives::types::GetValidatorInfoError::UnknownEpoch => {
                Self::UnknownEpoch
            }
            near_client_primitives::types::GetValidatorInfoError::ValidatorInfoUnavailable => {
                Self::ValidatorInfoUnavailable
            }
            near_client_primitives::types::GetValidatorInfoError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetValidatorInfoError::Unreachable(
                ref error_message,
            ) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcValidatorError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

// -- validator
