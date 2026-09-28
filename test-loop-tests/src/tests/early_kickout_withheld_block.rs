//! Regression for the testnet halt at height 270420307. A withheld block arrives before
//! its canonical sibling and advances the aggregator past the canonical last-final basis.
//! The resulting epoch walk used to evict uncommitted `BlockInfo`, blocking every retry.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::node::TestLoopNode;
use near_async::messaging::CanSend as _;
use near_client::BlockResponse;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::StoreAdapter;
use parking_lot::Mutex;
use std::sync::Arc;

const EPOCH_LENGTH: u64 = 1100;
/// Past both the kickout grace period and the `BlockInfo` cache capacity.
const SAME_EPOCH_FORK_OFFSET: u64 = 1040;

/// Deliver the withheld block to every node just before its canonical sibling.
fn deliver_late(env: &mut TestLoopEnv, withheld: BlockHeight) {
    let late: Arc<Mutex<Option<Arc<Block>>>> = Arc::new(Mutex::new(None));
    let client_senders: Vec<_> =
        env.node_datas.iter().map(|data| data.client_sender.clone()).collect();
    for data in &env.node_datas {
        let late = late.clone();
        let client_senders = client_senders.clone();
        let peer_id = data.peer_id.clone();
        let peer_manager = env.test_loop.data.get_mut(&data.peer_manager_sender.actor_handle());
        peer_manager.register_override_handler(Box::new(move |request| {
            let NetworkRequests::Block { block } = &request else {
                return HandlerResult::Unhandled(request);
            };
            if block.header().height() == withheld {
                *late.lock() = Some(block.clone());
                return HandlerResult::Handled(NetworkResponses::NoResponse);
            }
            if block.header().height() > withheld {
                if let Some(late) = late.lock().take() {
                    for sender in &client_senders {
                        let response = BlockResponse {
                            block: late.clone(),
                            peer_id: peer_id.clone(),
                            was_requested: false,
                        };
                        sender.send(response.span_wrap());
                    }
                }
            }
            HandlerResult::Unhandled(request)
        }));
    }
}

fn withhold_block(mut env: TestLoopEnv, withheld: BlockHeight) -> (TestLoopEnv, CryptoHash) {
    deliver_late(&mut env, withheld);
    env.node_runner(0).run_until_head_height(withheld - 1);
    let parent = env.node(0).client().chain.get_block_hash_by_height(withheld - 1).unwrap();

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&parent).unwrap();
    let producer = epoch_manager.get_block_producer_info(&epoch_id, withheld).unwrap();
    let next_producer = epoch_manager.get_block_producer_info(&epoch_id, withheld + 1).unwrap();
    assert_ne!(
        producer.account_id(),
        next_producer.account_id(),
        "the next height's producer must be someone else, or it builds on the withheld block"
    );

    let accounts: Vec<_> = env.node_datas.iter().map(|data| data.account_id.clone()).collect();
    for account in accounts {
        env.runner_for_account(&account).run_until_head_height(withheld + 3);
    }
    assert_fork_resolved(&env, producer.account_id(), withheld, parent);
    (env, parent)
}

fn assert_fork_resolved(
    env: &TestLoopEnv,
    producer: &AccountId,
    withheld: BlockHeight,
    parent: CryptoHash,
) {
    let withholder = env.node_for_account(producer);
    let canonical = withholder
        .client()
        .chain
        .get_block_by_height(withheld + 1)
        .expect("the chain must keep the block built on the late block's parent");
    assert_eq!(
        canonical.header().prev_hash(),
        &parent,
        "the canonical chain must skip the withheld height"
    );
    for data in &env.node_datas {
        let node = env.node_for_account(&data.account_id);
        assert_eq!(
            node.client().chain.get_block_hash_by_height(withheld + 1).unwrap(),
            *canonical.hash(),
            "{} must be on the canonical chain",
            data.account_id
        );
    }

    for data in &env.node_datas {
        only_block_at_height(&env.node_for_account(&data.account_id), withheld);
    }
    let withheld_block = only_block_at_height(&withholder, withheld);
    let final_height = |block: &Block| {
        withholder
            .client()
            .chain
            .get_block_header(block.header().last_final_block())
            .unwrap()
            .height()
    };
    assert!(
        final_height(&withheld_block) > final_height(&canonical),
        "the withheld block must finalize past the canonical basis ({} vs {})",
        final_height(&withheld_block),
        final_height(&canonical)
    );
}

fn only_block_at_height(node: &TestLoopNode<'_>, height: BlockHeight) -> Arc<Block> {
    let hashes: Vec<CryptoHash> = node
        .store()
        .chain_store()
        .get_all_block_hashes_by_height(height)
        .values()
        .flatten()
        .copied()
        .collect();
    assert_eq!(hashes.len(), 1, "a node must hold exactly the late block at {height}");
    node.block(hashes[0])
}

fn build_env() -> TestLoopEnv {
    TestLoopBuilder::new().validators(4, 0).num_shards(1).epoch_length(EPOCH_LENGTH).build()
}

#[test]
// TODO(spice-test): Check relevance to SPICE and enable if applicable.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_withheld_block_mid_epoch() {
    init_test_logger();
    let env = build_env();
    let genesis_height = env.node(0).head().height;
    let (env, parent) = withhold_block(env, genesis_height + SAME_EPOCH_FORK_OFFSET);
    let epoch_manager = env.node(0).client().epoch_manager.clone();
    assert!(
        !epoch_manager.is_next_block_epoch_start(&parent).unwrap(),
        "the fork must be mid-epoch"
    );
}

/// Both siblings open the next epoch, leaving the canonical basis in the previous epoch.
#[test]
// TODO(spice-test): Check relevance to SPICE and enable if applicable.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_withheld_block_at_epoch_boundary() {
    init_test_logger();
    let mut env = build_env();
    let genesis_height = env.node(0).head().height;
    // The epoch boundary depends on finality lag; measure it shortly before the boundary.
    env.node_runner(0).run_until_head_height(genesis_height + EPOCH_LENGTH - 20);
    let first_block_of_next_epoch = {
        let node = env.node(0);
        let client = node.client();
        let head = node.head();
        let head_header = client.chain.get_block_header(&head.last_block_hash).unwrap();
        let final_height =
            client.chain.get_block_header(head_header.last_final_block()).unwrap().height();
        let finality_lag = head.height - final_height;
        let epoch_start =
            client.epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
        epoch_start + EPOCH_LENGTH - 3 + finality_lag + 1
    };
    let (env, parent) = withhold_block(env, first_block_of_next_epoch);
    let epoch_manager = env.node(0).client().epoch_manager.clone();
    assert!(
        epoch_manager.is_next_block_epoch_start(&parent).unwrap(),
        "the withheld block must be the first of an epoch"
    );
}
