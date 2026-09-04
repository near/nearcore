//! An epoch's light client block row can carry a layout an older release wrote, which this
//! build cannot decode. A node holding one answers the query that reads it.

use crate::setup::builder::TestLoopBuilder;
use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time::Duration;
use near_crypto::Signature;
use near_jsonrpc_primitives::message::Message;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_primitives::views::validator_stake_view::ValidatorStakeView;
use near_primitives::views::{BlockHeaderInnerLiteView, LightClientBlockView};
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};

/// The stored shape before `chunk_execution_root` was appended.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
struct OldBlockHeaderInnerLiteView {
    height: BlockHeight,
    epoch_id: CryptoHash,
    next_epoch_id: CryptoHash,
    prev_state_root: CryptoHash,
    outcome_root: CryptoHash,
    timestamp: u64,
    timestamp_nanosec: u64,
    next_bp_hash: CryptoHash,
    block_merkle_root: CryptoHash,
}

/// The row it sits in.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
struct OldLightClientBlockView {
    prev_block_hash: CryptoHash,
    next_block_inner_hash: CryptoHash,
    inner_lite: OldBlockHeaderInnerLiteView,
    inner_rest_hash: CryptoHash,
    next_bps: Option<Vec<ValidatorStakeView>>,
    approvals_after_next: Vec<Option<Box<Signature>>>,
}

/// Serializes `row`'s block through the older types.
fn as_written_by_an_older_release(row: &[u8]) -> Vec<u8> {
    let view = LightClientBlockView::try_from_slice(row).expect("this build wrote this row");
    assert_eq!(
        view.inner_lite.chunk_execution_root, None,
        "a pre-spice header carries no chunk execution root",
    );
    // Destructured, so a field added to either view stops this compiling.
    let LightClientBlockView {
        prev_block_hash,
        next_block_inner_hash,
        inner_lite,
        inner_rest_hash,
        next_bps,
        approvals_after_next,
    } = view;
    let BlockHeaderInnerLiteView {
        height,
        epoch_id,
        next_epoch_id,
        prev_state_root,
        outcome_root,
        timestamp,
        timestamp_nanosec,
        next_bp_hash,
        block_merkle_root,
        chunk_execution_root: _,
    } = inner_lite;
    let old_view = OldLightClientBlockView {
        prev_block_hash,
        next_block_inner_hash,
        inner_lite: OldBlockHeaderInnerLiteView {
            height,
            epoch_id,
            next_epoch_id,
            prev_state_root,
            outcome_root,
            timestamp,
            timestamp_nanosec,
            next_bp_hash,
            block_merkle_root,
        },
        inner_rest_hash,
        next_bps,
        approvals_after_next,
    };
    let old_row = borsh::to_vec(&old_view).expect("borsh cannot fail");
    assert!(
        LightClientBlockView::try_from_slice(&old_row).is_err(),
        "this build must not read the older layout, or the test proves nothing",
    );
    old_row
}

/// Rewrites one row into the older layout, then asks for it over RPC.
#[test]
fn test_next_light_client_block_on_an_older_row() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .enable_rpc()
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(100)
        .build();

    env.rpc_runner().run_until_head_height(5 * epoch_length);

    let store: Store = env.rpc_node().store();

    // The handler reads the stored row only when neither the block's epoch nor its next epoch
    // is the head's.
    let mut seed = None;
    let rpc = env.rpc_node();
    let head_epoch_id = rpc.head().epoch_id;
    for height in 1..5 * epoch_length {
        let Ok(header) = rpc.client().chain.get_block_header_by_height(height) else {
            continue;
        };
        let row_key = header.next_epoch_id().0;
        if *header.epoch_id() != head_epoch_id
            && row_key != head_epoch_id.0
            && store.get(DBCol::EpochLightClientBlocks, row_key.as_ref()).is_some()
        {
            seed = Some((*header.hash(), row_key));
            break;
        }
    }
    let (seed_hash, row_key) = seed.expect("some old block has a stored row for its next epoch");
    let row = store
        .get(DBCol::EpochLightClientBlocks, row_key.as_ref())
        .expect("the epoch boundary must have written the row")
        .to_vec();

    let old_row = as_written_by_an_older_release(&row);

    let mut update = store.store_update();
    update.set_raw_bytes(DBCol::EpochLightClientBlocks, row_key.as_ref(), &old_row);
    update.commit();

    let params = serde_json::json!({ "last_block_hash": seed_hash });
    let response = env.rpc_runner().run_with_jsonrpc_client(
        |client| {
            client.transport.send_jsonrpc_request(
                Message::request("next_light_client_block".to_string(), params),
                false,
            )
        },
        Duration::seconds(10),
    );

    response.expect("the node has to answer the query");

    assert!(
        store.chain_store().get_epoch_light_client_block(&row_key).is_err(),
        "the row must still be unreadable, or the answer above proves nothing",
    );
}
