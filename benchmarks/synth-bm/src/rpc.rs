use std::time::Instant;

use near_crypto::{InMemorySigner, PublicKey, Signer};
use near_jsonrpc_client::JsonRpcClient;
use near_jsonrpc_client::errors::JsonRpcError;
use near_jsonrpc_client::methods::block::RpcBlockRequest;
use near_jsonrpc_client::methods::query::RpcQueryRequest;
use near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest;
use near_jsonrpc_client::methods::tx::{RpcTransactionError, RpcTransactionResponse};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::{
    transaction::Transaction,
    types::{AccountId, BlockReference, Finality},
    views::{
        AccessKeyView, BlockView, ExecutionStatusView, FinalExecutionStatus, QueryRequest,
        TxExecutionStatus,
    },
};
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info, warn};

pub fn new_request(
    transaction: Transaction,
    wait_until: TxExecutionStatus,
    signer: InMemorySigner,
) -> RpcSendTransactionRequest {
    RpcSendTransactionRequest {
        signed_transaction: transaction.sign(&Signer::from(signer)),
        wait_until,
    }
}

pub async fn get_latest_block(client: &JsonRpcClient) -> anyhow::Result<BlockView> {
    get_block(client, BlockReference::Finality(Finality::Final)).await
}

pub async fn get_block(
    client: &JsonRpcClient,
    block_ref: BlockReference,
) -> anyhow::Result<BlockView> {
    let request = RpcBlockRequest { block_reference: block_ref };
    let block_view = client.call(request).await?;
    Ok(block_view)
}

pub async fn view_access_key(
    client: &JsonRpcClient,
    account_id: AccountId,
    public_key: PublicKey,
) -> anyhow::Result<AccessKeyView> {
    let request = RpcQueryRequest {
        block_reference: BlockReference::latest(),
        request: QueryRequest::ViewAccessKey { account_id, public_key },
    };
    let response = client.call(request).await?;
    match response.kind {
        QueryResponseKind::AccessKey(access_key_view) => Ok(access_key_view),
        _ => Err(anyhow::anyhow!("unexpected query response")),
    }
}

pub type RpcCallResult = Result<RpcTransactionResponse, JsonRpcError<RpcTransactionError>>;

pub struct RpcResponseHandler {
    receiver: Receiver<RpcCallResult>,
    /// The `wait_until` value passed to transactions.
    wait_until: TxExecutionStatus,
    response_check_severity: ResponseCheckSeverity,
    num_expected_responses: u64,
}

#[derive(Copy, Clone, Debug)]
pub enum ResponseCheckSeverity {
    /// An unexpected response or transaction failure will be logged as warning.
    Log,
    /// An unexpected response or transaction failure will raise a panic.
    Assert,
}

impl RpcResponseHandler {
    pub fn new(
        receiver: Receiver<RpcCallResult>,
        wait_until: TxExecutionStatus,
        response_check_severity: ResponseCheckSeverity,
        num_expected_responses: u64,
    ) -> Self {
        Self { receiver, wait_until, response_check_severity, num_expected_responses }
    }

    pub async fn handle_all_responses(&mut self) {
        // Start timer after receiving the first response.
        let mut timer: Option<Instant> = None;

        let mut num_received = 0;
        let mut num_succeeded = 0;
        let mut num_rpc_error = 0;
        while num_received < self.num_expected_responses {
            let response = match self.receiver.recv().await {
                Some(res) => res,
                None => {
                    warn!(
                        "Expected {} responses but channel closed after {num_received}",
                        self.num_expected_responses
                    );
                    break;
                }
            };

            num_received += 1;
            if timer.is_none() {
                timer = Some(Instant::now());
            }

            match response {
                Ok(rpc_response) => {
                    if check_tx_response(
                        rpc_response,
                        self.wait_until.clone(),
                        self.response_check_severity,
                    ) {
                        num_succeeded += 1;
                    }
                }
                Err(err) => {
                    warn!("Got error response from rpc: {err}");
                    num_rpc_error += 1;
                }
            };

            debug!(
                "Received {} responses; num_success={} num_rpc_error={}",
                num_received, num_succeeded, num_rpc_error
            );
        }

        if let Some(timer) = timer {
            info!(
                "Received {num_received} tx responses in {:.2} seconds",
                timer.elapsed().as_secs_f64()
            );
        }
    }
}

/// Maps `TxExecutionStatus` to integers s.t. higher numbers represent a higher finality.
fn tx_execution_level(status: &TxExecutionStatus) -> u8 {
    match status {
        TxExecutionStatus::None => 0,
        TxExecutionStatus::Included => 1,
        TxExecutionStatus::ExecutedOptimistic => 2,
        TxExecutionStatus::IncludedFinal => 3,
        TxExecutionStatus::Executed => 4,
        TxExecutionStatus::Final => 5,
    }
}

/// For now not inspecting success values or receipt ids.
fn check_outcome(
    response: RpcTransactionResponse,
    response_check_severity: ResponseCheckSeverity,
) -> bool {
    let outcome =
        response.final_execution_outcome.expect("response should have an outcome").into_outcome();

    if !matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)) {
        let msg = format!(
            "got outcome.status {:#?}, expected FinalExecutionStatus::SuccessValue",
            outcome.status
        );
        warn_or_panic(&msg, response_check_severity);
        return false;
    }

    for receipt_outcome in outcome.receipts_outcome.iter() {
        match &receipt_outcome.outcome.status {
            ExecutionStatusView::Unknown => {
                warn_or_panic("unknown receipt outcome", response_check_severity);
                return false;
            }
            ExecutionStatusView::Failure(err) => {
                warn_or_panic(&format!("receipt failed: {err}"), response_check_severity);
                return false;
            }
            ExecutionStatusView::SuccessValue(_) => {}
            ExecutionStatusView::SuccessReceiptId(_) => {}
        }
    }

    true
}

/// Checks the rpc request to send a transaction succeeded. Depending on `wait_until`, the status
/// of receipts might be checked too. Logs warnings on request failures.
///
/// For now, only handling empty transaction success values and not inspecting success values of
/// receipts.
///
/// # Panics
pub fn check_tx_response(
    response: RpcTransactionResponse,
    wait_until: TxExecutionStatus,
    response_check_severity: ResponseCheckSeverity,
) -> bool {
    if tx_execution_level(&response.final_execution_status) < tx_execution_level(&wait_until) {
        let msg = format!(
            "got final execution status {:#?}, expected at least {:#?}",
            response.final_execution_status, wait_until
        );
        warn_or_panic(&msg, response_check_severity);
        return false;
    }

    // Check the outcome, if applicable.
    match response.final_execution_status {
        TxExecutionStatus::None => {
            // The response to a transaction with `wait_until: None` contains no outcome.
            // If that ever changes, the outcome must be checked, hence the assert.
            assert!(response.final_execution_outcome.is_none());
            false
        }
        TxExecutionStatus::Included => {
            unimplemented!("given how transactions are sent, this status is not yet returned")
        }
        TxExecutionStatus::ExecutedOptimistic
        | TxExecutionStatus::IncludedFinal
        | TxExecutionStatus::Executed
        | TxExecutionStatus::Final => {
            // For now, only sending transactions that expect an empty success value.
            check_outcome(response, response_check_severity)
        }
    }
}

fn warn_or_panic(msg: &str, response_check_severity: ResponseCheckSeverity) {
    match response_check_severity {
        ResponseCheckSeverity::Log => warn!("{msg}"),
        ResponseCheckSeverity::Assert => panic!("{msg}"),
    }
}
