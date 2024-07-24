use near_client::ProcessTxResponse;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_primitives::{hash::CryptoHash, transaction::SignedTransaction, types::AccountId};

use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::Future;

pub trait JsonRpcHandlerTrait {
    fn send_tx_async(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcSendTransactionRequest,
    ) -> impl Future<Output = CryptoHash> + Send;
    fn tx_exists(
        &self,
        tx_hash: CryptoHash,
        signer_account_id: &AccountId,
    ) -> impl Future<
        Output = Result<bool, near_jsonrpc_primitives::types::transactions::RpcTransactionError>,
    > + Send;
    fn tx_status_fetch(
        &self,
        tx_info: near_jsonrpc_primitives::types::transactions::TransactionInfo,
        finality: near_primitives::views::TxExecutionStatus,
        fetch_receipt: bool,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
            near_jsonrpc_primitives::types::transactions::RpcTransactionError,
        >,
    > + Send;
    fn send_tx_internal(
        &self,
        tx: SignedTransaction,
        check_only: bool,
    ) -> impl Future<
        Output = Result<
            ProcessTxResponse,
            near_jsonrpc_primitives::types::transactions::RpcTransactionError,
        >,
    > + Send;
    fn send_tx(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcSendTransactionRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
            near_jsonrpc_primitives::types::transactions::RpcTransactionError,
        >,
    > + Send;
    fn send_tx_commit(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcSendTransactionRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
            near_jsonrpc_primitives::types::transactions::RpcTransactionError,
        >,
    > + Send;
    fn health(
        &self,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::status::RpcHealthResponse,
            near_jsonrpc_primitives::types::status::RpcStatusError,
        >,
    > + Send;
    fn status(
        &self,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::status::RpcStatusResponse,
            near_jsonrpc_primitives::types::status::RpcStatusError,
        >,
    > + Send;
    fn protocol_config(
        &self,
        request_data: near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse,
            near_jsonrpc_primitives::types::config::RpcProtocolConfigError,
        >,
    > + Send;
    fn query(
        &self,
        request_data: near_jsonrpc_primitives::types::query::RpcQueryRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::query::RpcQueryResponse,
            near_jsonrpc_primitives::types::query::RpcQueryError,
        >,
    > + Send;
    fn tx_status_common(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcTransactionStatusRequest,
        fetch_receipt: bool,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
            near_jsonrpc_primitives::types::transactions::RpcTransactionError,
        >,
    > + Send;
    fn block(
        &self,
        request_data: near_jsonrpc_primitives::types::blocks::RpcBlockRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::blocks::RpcBlockResponse,
            near_jsonrpc_primitives::types::blocks::RpcBlockError,
        >,
    > + Send;
    fn chunk(
        &self,
        request_data: near_jsonrpc_primitives::types::chunks::RpcChunkRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::chunks::RpcChunkResponse,
            near_jsonrpc_primitives::types::chunks::RpcChunkError,
        >,
    > + Send;
    fn congestion_level(
        &self,
        request_data: near_jsonrpc_primitives::types::congestion::RpcCongestionLevelRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::congestion::RpcCongestionLevelResponse,
            near_jsonrpc_primitives::types::congestion::RpcCongestionLevelError,
        >,
    > + Send;
    fn receipt(
        &self,
        request_data: near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::receipts::RpcReceiptResponse,
            near_jsonrpc_primitives::types::receipts::RpcReceiptError,
        >,
    > + Send;
    fn changes_in_block(
        &self,
        request: near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse,
            near_jsonrpc_primitives::types::changes::RpcStateChangesError,
        >,
    > + Send;
    fn changes_in_block_by_type(
        &self,
        request: near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse,
            near_jsonrpc_primitives::types::changes::RpcStateChangesError,
        >,
    > + Send;
    fn next_light_client_block(
        &self,
        request: near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse,
            near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError,
        >,
    > + Send;
    fn light_client_execution_outcome_proof(
        &self,
        request: near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse,
            near_jsonrpc_primitives::types::light_client::RpcLightClientProofError,
        >,
    > + Send;
    fn light_client_block_proof(
        &self,
        request: near_jsonrpc_primitives::types::light_client::RpcLightClientBlockProofRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::light_client::RpcLightClientBlockProofResponse,
            near_jsonrpc_primitives::types::light_client::RpcLightClientProofError,
        >,
    > + Send;
    fn network_info(
        &self,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse,
            near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError,
        >,
    > + Send;
    fn gas_price(
        &self,
        request_data: near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse,
            near_jsonrpc_primitives::types::gas_price::RpcGasPriceError,
        >,
    > + Send;
    fn validators(
        &self,
        request_data: near_jsonrpc_primitives::types::validator::RpcValidatorRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::validator::RpcValidatorResponse,
            near_jsonrpc_primitives::types::validator::RpcValidatorError,
        >,
    > + Send;
    fn validators_ordered(
        &self,
        request: near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedResponse,
            near_jsonrpc_primitives::types::validator::RpcValidatorError,
        >,
    > + Send;
    fn client_config(
        &self,
    ) -> impl Future<
        Output = Result<
            near_jsonrpc_primitives::types::client_config::RpcClientConfigResponse,
            near_jsonrpc_primitives::types::client_config::RpcClientConfigError,
        >,
    > + Send;
}

pub trait ParamsTrait<T> {
    fn new(params: Value) -> Self;
    fn parse(value: Value) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned;
    fn unwrap_or_parse(self) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned;
    fn try_singleton<U: DeserializeOwned>(
        self,
        func: impl FnOnce(U) -> Result<T, RpcParseError>,
    ) -> Self;
    fn try_pair<U: DeserializeOwned, V: DeserializeOwned>(
        self,
        func: impl FnOnce(U, V) -> Result<T, RpcParseError>,
    ) -> Self;
}
