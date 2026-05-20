use crate::setup::builder::TestLoopBuilder;
use near_async::messaging::Handler;
use near_client::{TxStatus, TxStatusError};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::hash;

/// A `Pending` entry whose base block hash falls outside the validity window
/// resolves to `Expired`, not `Unknown` or `Ok(None)`.
#[test]
fn tx_status_reports_expired_for_pending_past_validity_window() {
    init_test_logger();

    let validity_period = 5;
    let mut env = TestLoopBuilder::new()
        .transaction_validity_period(validity_period)
        .epoch_length(20)
        .build();

    let head = env.validator().head();
    let base_block_hash = head.last_block_hash;
    let start_height = head.height;
    let signer_account_id = env.node_datas[0].account_id.clone();

    let tx_hash = hash(b"never-submitted-tx");
    env.node_datas[0].tx_fate_cache.lock().record_pending(tx_hash, base_block_hash);

    env.validator_runner().run_until_head_height(start_height + validity_period + 2);

    let result = env.validator_mut().view_client_actor().handle(TxStatus {
        tx_hash,
        signer_account_id,
        fetch_receipt: false,
    });

    assert!(
        matches!(result, Err(TxStatusError::Expired(h)) if h == tx_hash),
        "expected Expired, got {result:?}"
    );
}
