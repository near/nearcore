pub(super) use crate::setup::builder::TestLoopBuilder;
pub(super) use crate::utils::account::create_account_id;
pub(super) use near_async::messaging::Handler;
pub(super) use near_async::time::Duration;
pub(super) use near_client_primitives::types::{GetReceiptToTx, GetReceiptToTxError};
pub(super) use near_jsonrpc_primitives::types::receipts::{
    ReceiptReference, RpcReceiptToTxRequest,
};
pub(super) use near_o11y::testonly::init_test_logger;
pub(super) use near_primitives::hash::CryptoHash;
pub(super) use near_primitives::receipt::{
    ProcessedReceiptMetadata, ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction,
    ReceiptSource, ReceiptToTxInfo, ReceiptToTxInfoV1,
};
pub(super) use near_primitives::test_utils::create_user_test_signer;
pub(super) use near_primitives::transaction::SignedTransaction;
pub(super) use near_primitives::types::{AccountId, Balance, BlockHeightDelta, Gas, ShardId};
pub(super) use near_primitives::utils::get_block_shard_id;
pub(super) use near_store::DBCol;

mod budget;
mod classifier;
mod column;
mod errors;
mod hint;

pub(super) const EPOCH_LENGTH: u64 = 5;

/// Column-only `GetReceiptToTx` message (no hint). Most pre-hint tests
/// exercise the column path only.
pub(super) fn receipt_to_tx_req(receipt_id: CryptoHash) -> GetReceiptToTx {
    GetReceiptToTx { receipt_id, block_height: None, shard_id: None, window: None }
}

/// Column-only `RpcReceiptToTxRequest` (no hint).
pub(super) fn receipt_to_tx_rpc_req(receipt_id: CryptoHash) -> RpcReceiptToTxRequest {
    RpcReceiptToTxRequest {
        receipt_reference: ReceiptReference { receipt_id },
        block_height: None,
        shard_id: None,
        window: None,
    }
}

pub(super) fn send_self_money(
    env: &mut crate::setup::env::TestLoopEnv,
    user_account: &AccountId,
    nonce: u64,
) -> (CryptoHash, CryptoHash, u64) {
    let signer = create_user_test_signer(user_account);
    let head = env.validator().head();
    let tx = SignedTransaction::send_money(
        nonce,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::from_yoctonear(100),
        head.last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.validator_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let block_hash = outcome.transaction_outcome.block_hash;
    let height = env.validator().client().chain.get_block_header(&block_hash).unwrap().height();
    (tx_hash, receipt_id, height)
}

pub(super) fn handle(
    env: &mut crate::setup::env::TestLoopEnv,
    msg: GetReceiptToTx,
) -> Result<near_client_primitives::types::GetReceiptToTxResponse, GetReceiptToTxError> {
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    view_client.handle(msg)
}

pub(super) fn shard_containing_outcome(
    env: &crate::setup::env::TestLoopEnv,
    height: u64,
    outcome_id: CryptoHash,
    shard_ids: &[ShardId],
) -> ShardId {
    let chain_store = env.validator().client().chain.chain_store();
    let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
    shard_ids
        .iter()
        .copied()
        .find(|shard_id| {
            chain_store
                .get_outcomes_by_block_hash_and_shard_id(&block_hash, *shard_id)
                .contains(&outcome_id)
        })
        .expect("outcome exists on one of expected shards")
}
