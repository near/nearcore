//! Adversarial test for the verified-peer-height gate in `syncing_info`.
//!
//! A synced node at the tip (fresh head) is fed a fabricated far-ahead peer
//! height. The network layer records any peer's advertised block height
//! unconditionally, so this models an adversary advertising a false position
//! many epochs ahead. The gate must ignore the unvalidated claim: the node
//! keeps following the chain, never enters sync, and is never wiped.

use std::sync::Arc;

use super::util::{TEST_EPOCH_SYNC_HORIZON, run_until_synced, track_sync_status};
use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::TestLoopNetworkBlockInfo;
use crate::tests::sync::util::far_horizon_height;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::messaging::{CanSend, Sender};
use near_chain_configs::TrackedShardsConfig;
use near_client::BlockResponse;
use near_crypto::{KeyType, SecretKey};
use near_network::types::PeerInfo;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::Balance;

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_synced_node_ignores_unverified_far_ahead_height() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height((TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length);

    let victim_account = create_account_id("victim");
    let node_state = env
        .node_state_builder()
        .account_id(&victim_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("victim", node_state);
    let victim_idx = env.node_datas.len() - 1;
    run_until_synced(&mut env.test_loop, &env.node_datas, victim_idx, 0);

    // A real header with its height bumped past the epoch sync horizon, attributed
    // to an adversarial peer. `last_block_headers` keeps the max, so the claim
    // persists in every SetNetworkInfo push.
    let victim_handle = env.node_datas[victim_idx].client_sender.actor_handle();
    let head = env.test_loop.data.get(&victim_handle).client.chain.head().unwrap();
    let head_before = head.height;
    let head_header = env
        .test_loop
        .data
        .get(&victim_handle)
        .client
        .chain
        .get_block_header(&head.last_block_hash)
        .unwrap();
    let mut fake_header = head_header.as_ref().clone();
    fake_header.set_height(head.height + (TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length);
    let adversary = PeerInfo {
        id: PeerId::new(SecretKey::from_seed(KeyType::ED25519, "adversary").public_key()),
        addr: None,
        account_id: None,
    };
    Sender::<TestLoopNetworkBlockInfo>::from(&env.node_datas[victim_idx])
        .send(TestLoopNetworkBlockInfo { peer: adversary, block_header: fake_header });

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, victim_idx);
    env.node_runner(0).run_for_number_of_blocks(2 * epoch_length as usize);

    let victim_client = &env.test_loop.data.get(&victim_handle).client;
    let victim_head = victim_client.chain.head().unwrap().height;
    let status = victim_client.sync_handler.sync_status.as_variant_name();
    assert_eq!(status, "NoSync", "victim should stay NoSync, was {status}");
    assert!(victim_head > head_before, "victim should keep following the chain");
    assert!(
        sync_history.borrow().iter().all(|s| s == "NoSync"),
        "victim must not enter any sync phase after the adversarial far-ahead claim, saw {:?}",
        sync_history.borrow(),
    );
    assert!(!env.test_loop.is_denylisted("victim"), "victim must not be wiped");
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_genesis_node_bootstraps() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .track_all_shards()
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    let syncer_account = create_account_id("syncer");
    let node_state = env
        .node_state_builder()
        .account_id(&syncer_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("syncer", node_state);
    let syncer_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, syncer_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, syncer_idx, 0);

    // The genesis node runs the full far-horizon pipeline and returns to NoSync.
    let expected =
        ["AwaitingPeers", "NoSync", "EpochSync", "HeaderSync", "StateSync", "BlockSync", "NoSync"]
            .map(String::from);
    assert_eq!(sync_history.borrow().as_slice(), expected.as_slice());
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_stale_node_syncs() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .track_all_shards()
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    // Sync fully first so the head is past genesis (past the bootstrap exemption).
    let syncer_account = create_account_id("syncer");
    let node_state = env
        .node_state_builder()
        .account_id(&syncer_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("syncer", node_state);
    let syncer_idx = env.node_datas.len() - 1;
    run_until_synced(&mut env.test_loop, &env.node_datas, syncer_idx, 0);

    // Advance past the horizon while it is down: the virtual clock moves with block
    // production, so its head goes stale and the tip lands in epochs it can't resolve.
    let killed_state = env.kill_node("syncer");
    env.node_runner(0).run_for_number_of_blocks(far_horizon_height(epoch_length) as usize);

    let restart_id = "syncer_restarted";
    env.restart_node(restart_id, killed_state);
    let restarted_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);
    env.node_runner(0).run_for_number_of_blocks(2 * epoch_length as usize);

    // The staleness gate lets the node enter epoch sync, which then triggers the
    // epoch-sync data reset (the separately guarded issue), recorded by the test
    // loop as a denylist.
    let expected = ["AwaitingPeers", "NoSync", "EpochSync"].map(String::from);
    assert_eq!(sync_history.borrow().as_slice(), expected.as_slice());
    assert!(
        env.test_loop.is_denylisted(restart_id),
        "stale node should reach epoch sync and trigger the data reset"
    );
}

#[test]
fn test_max_prev_height_does_not_abort_node() {
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .track_all_shards()
        .build();
    env.node_runner(0).run_until_head_height(2 * epoch_length);

    // A real block re-signed by the producer the victim samples for the forged
    // height, so it passes the producer signature check that guards the
    // ancestry-free approval verification.
    let head = env.node(0).head();
    let head_block = env.node(0).client().chain.get_block(&head.last_block_hash).unwrap();
    let forged_height = head.height + far_horizon_height(epoch_length);
    let block_producer = env
        .node(0)
        .client()
        .epoch_manager
        .get_block_producer(&head.epoch_id, forged_height)
        .unwrap();
    let mut forged_block = head_block.as_ref().clone();
    forged_block.mut_header().set_height(forged_height);
    forged_block.mut_header().set_prev_height(u64::MAX);
    forged_block.mut_header().resign(&create_test_signer(block_producer.as_str()));

    let adversary = PeerId::new(SecretKey::from_seed(KeyType::ED25519, "adversary").public_key());
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, 0);
    env.node_datas[0].client_sender.send(
        BlockResponse { block: Arc::new(forged_block), peer_id: adversary, was_requested: false }
            .span_wrap(),
    );
    env.node_runner(0).run_for_number_of_blocks(2 * epoch_length as usize);

    assert!(env.node(0).head().height > head.height, "victim should keep following the chain");
    assert!(
        sync_history.borrow().iter().all(|status| status == "NoSync"),
        "victim must not enter any sync phase after the max prev_height claim, saw {:?}",
        sync_history.borrow(),
    );
}
