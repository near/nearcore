use super::activation_scenarios::oracles::{
    check_certified_via_fallback, check_fallback_has_enough_stake,
};
use super::activation_scenarios::resolve::fallback_validators_spec;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::all_stake_fallback::{
    SPICE_FALLBACK_CERTIFICATION_DELAY, all_stake_fallback_assignment, is_fallback_only_chunk,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::BlockHeader;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, NumShards, ShardId, SpiceChunkId,
};
use std::collections::HashSet;

/// Shards of the fallback tests.
const NUM_SHARDS: NumShards = 6;

/// The builder of the fallback tests: the fourteen equal-stake validators of the scenario
/// framework's profile B over `NUM_SHARDS` shards with two mandates each, keeping each
/// chunk's designated subset under a third of the stake.
fn fallback_builder() -> TestLoopBuilder {
    TestLoopBuilder::new()
        .validators_spec(fallback_validators_spec())
        .shard_layout(ShardLayout::multi_shard(NUM_SHARDS, 0))
        .target_validator_mandates_per_shard(2)
}

/// Asserts every (height, shard) of the genesis epoch has designated validators holding at most 1/3
/// of total stake, so the non-designated remainder alone can certify via the fallback.
fn assert_fallback_has_enough_stake(node: &TestLoopNode) {
    check_fallback_has_enough_stake(node).unwrap_or_else(|error| panic!("{error}"));
}

/// Asserts `header`'s block is certified and at least one of its chunks certified via the all-stake
/// fallback: the present designated endorsements alone don't meet the designated 2/3 threshold, so
/// the non-designated remainder must have carried it.
fn assert_certified_via_fallback(node: &TestLoopNode, header: &BlockHeader) {
    check_certified_via_fallback(node, header).unwrap_or_else(|error| panic!("{error}"));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_without_designated_endorsements() {
    init_test_logger();

    let mut env = fallback_builder().build().drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.node(0));

    // The certified frontier must keep advancing via the all-stake fallback though every designated
    // endorsement is dropped (slowly: ~1 block per fallback window under a total outage).
    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(300),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_without_witness_requests() {
    init_test_logger();

    // Every request for partial data is dropped, so a non-designated validator can endorse only if
    // the producers pushed the witness to it when the fallback opened.
    let mut env = fallback_builder()
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements)
        .drop(DropCondition::SpiceDataRequests);
    assert_fallback_has_enough_stake(&env.node(0));

    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(300),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_without_accesses_messages() {
    init_test_logger();

    // The non-designated validators get the witness through the fallback push but never the
    // producers' contract accesses, so they can only endorse from what the witness itself lists.
    let mut env = fallback_builder().build().drop(DropCondition::DesignatedSpiceEndorsements);
    let (faults, observed) = env.install_spice_partial_data_faults();
    faults
        .lock()
        .drop_contract_accesses_from
        .extend(env.node_datas.iter().map(|data| data.account_id.clone()));
    assert_fallback_has_enough_stake(&env.node(0));

    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(300),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
    assert!(observed.lock().dropped_contract_accesses > 0, "no accesses message was dropped");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_across_epoch_boundary() {
    init_test_logger();

    let epoch_length = 25;
    let mut env = fallback_builder().epoch_length(epoch_length).build();
    assert_fallback_has_enough_stake(&env.node(0));

    // Run normally up to just before the epoch boundary, then drop designated endorsements: the
    // all-stake fallback alone must carry certification across into the next epoch.
    env.node_runner(0).run_until_head_height(epoch_length - 5);
    let initial_epoch = env.node(0).head().epoch_id;
    let mut env = env.drop(DropCondition::DesignatedSpiceEndorsements);

    // The certified frontier must reach the second epoch (height past the boundary, with a different
    // epoch id), proving the fallback certified the boundary chunks under the new assignments.
    env.node_runner(0).run_until(
        |node| {
            let header = node.last_certified_block_header();
            let epoch_id = node.client().epoch_manager.get_epoch_id(header.hash()).unwrap();
            header.height() > epoch_length && epoch_id != initial_epoch
        },
        Duration::seconds(300),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_when_validators_track_all_shards() {
    init_test_logger();

    // Every validator tracks every shard, so non-designated validators self-execute instead of
    // pulling witnesses; their stake must still certify the chunks via the fallback.
    let mut env = fallback_builder()
        .track_all_shards()
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.node(0));

    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(300),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_chunk_accessing_contract_code() {
    init_test_logger();

    let contract_user = create_account_id("contract-user");
    // The no-op compiled contract cache forces validators to fetch contract code via a
    // SpiceContractCodeRequest; otherwise the fallback code-request gate is never exercised.
    // epoch_length 100 keeps the call chunk (~height 9) within the genesis epoch the precondition
    // checks.
    let mut env = fallback_builder()
        .epoch_length(100)
        .add_user_account(&contract_user, Balance::from_near(100))
        .enable_rpc()
        .disable_compiled_contract_cache()
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.rpc_node());

    // Wait for the deploy to be included before submitting the call, so the call runs against an
    // already-settled contract (`record_contract_call` skips newly-deployed code).
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&contract_user);
    let deploy_hash = env.rpc_node().submit_tx(deploy_tx);
    env.rpc_runner().run_until_included(&[deploy_hash]);

    // The call's chunk witness omits the code, so a cold non-designated fallback validator must
    // fetch it via a contract-code request.
    let call_tx = env.rpc_node().tx_call(
        &contract_user,
        &contract_user,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_hash = env.rpc_node().submit_tx(call_tx);
    let call_height = env.rpc_runner().run_until_included(&[call_hash]);

    // Execution advances only as the fallback certifies; wait for the certified frontier to reach
    // the call chunk. Pre-fix its code requests are rejected, so the frontier stalls below it.
    env.rpc_runner().run_until(
        |node| node.last_certified_block_header().height() >= call_height,
        Duration::seconds(120),
    );
    let frontier = env.rpc_node().last_certified_block_header();
    assert_certified_via_fallback(&env.rpc_node(), frontier.as_ref());
}

// The protocol upgrade with every designated endorsement dropped is the `fallback` scenario
// of `activation_scenarios`, in its `fallback` group.

fn fallback_only_certification_schedule(
    node: &TestLoopNode,
    from_height: BlockHeight,
    to_height: BlockHeight,
) -> Vec<(BlockHeight, ShardId)> {
    let client = node.client();
    let chain_store = client.chain.chain_store();
    let epoch_manager = client.epoch_manager.as_ref();
    let mut fallback_schedule = Vec::new();
    for height in from_height..=to_height {
        let block = node.block(chain_store.get_block_hash_by_height(height).unwrap());
        let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
        for shard_id in shard_layout.shard_ids() {
            if is_fallback_only_chunk(epoch_manager, block.header(), shard_id).unwrap() {
                fallback_schedule.push((height, shard_id));
            }
        }
    }
    fallback_schedule
}

/// Asserts the chunk certified with non-designated validators endorsing it on chain. An ordinary
/// chunk never admits those on a healthy network: it is not fallback-eligible until it has been
/// stuck for the full window.
fn assert_certified_by_all_stake(
    node: &TestLoopNode,
    chunk_height: BlockHeight,
    shard_id: ShardId,
) {
    let client = node.client();
    let chain_store = client.chain.chain_store();
    let epoch_manager = client.epoch_manager.as_ref();
    let chunk_block_hash = chain_store.get_block_hash_by_height(chunk_height).unwrap();
    let chunk_id = SpiceChunkId { block_hash: chunk_block_hash, shard_id };
    let chunk_block = chain_store.get_block_header(&chunk_block_hash).unwrap();

    // The designated set alone must not hold more than 1/3 of total stake, or the chunk could
    // certify before any non-designated endorsement lands.
    let designated = epoch_manager
        .get_chunk_validator_assignments(chunk_block.epoch_id(), shard_id, chunk_height)
        .unwrap();
    let all_stake = all_stake_fallback_assignment(epoch_manager, chunk_block.epoch_id()).unwrap();
    let designated_accounts: HashSet<AccountId> =
        designated.assignments().iter().map(|(account_id, _)| account_id.clone()).collect();
    assert!(!all_stake.is_endorsed_by_more_than_one_third(&designated_accounts));

    let (certifying_height, endorsers) = node
        .certifying_height_and_endorsers(&chunk_id)
        .unwrap_or_else(|| panic!("fallback-only chunk {chunk_id:?} was not certified"));
    // Inside the ordinary fallback window, so the schedule opened the all-stake path rather than
    // the chunk sitting stuck long enough for the normal one to open.
    let delay = certifying_height - chunk_height;
    assert!(
        delay < SPICE_FALLBACK_CERTIFICATION_DELAY,
        "fallback-only chunk {chunk_id:?} took {delay} blocks, no better than the ordinary window",
    );

    assert!(
        endorsers.iter().any(|account_id| !designated.contains(account_id)),
        "fallback-only chunk {chunk_id:?} has only designated endorsers on chain",
    );
}

/// Runs to `epoch_length`, then asserts every chunk the schedule marked fallback-only over that
/// span certified through the all-stake path.
fn assert_every_fallback_only_chunk_certifies(
    env: &mut TestLoopEnv,
    epoch_length: BlockHeightDelta,
) {
    // The schedule is read off block headers, so the blocks have to exist first.
    env.node_runner(0).run_until_head_height(epoch_length);
    let fallback_schedule = fallback_only_certification_schedule(&env.node(0), 1, epoch_length);
    assert_eq!(
        fallback_schedule.len() as u64,
        NUM_SHARDS,
        "one slot per shard over the first epoch_length blocks"
    );

    // The schedule is ascending, so certifying the last entry means every one of them is certified.
    let (last_height, _) = *fallback_schedule.last().unwrap();
    env.node_runner(0).run_until_certified(last_height);
    for &(chunk_height, shard_id) in &fallback_schedule {
        assert_certified_by_all_stake(&env.node(0), chunk_height, shard_id);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_fallback_only_chunk_certifies_on_a_healthy_network() {
    init_test_logger();

    let epoch_length = 25;
    // Unlike the outage tests above, no endorsements are dropped: the all-stake path runs because
    // the chunk is fallback-only, not because the designated set went missing.
    let mut env = fallback_builder().epoch_length(epoch_length).build();

    assert_every_fallback_only_chunk_certifies(&mut env, epoch_length);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_fallback_only_chunk_certifies_when_execution_lags() {
    init_test_logger();

    let epoch_length = 25;
    let env = fallback_builder().epoch_length(epoch_length).delay_warmup().build();
    // Holding endorsements back puts certification behind consensus, so a chunk's endorsements can
    // reach the chain in the same block that first makes the chunk certifiable.
    let execution_delay = 4;
    env.delay_endorsements_propagation(execution_delay);
    let mut env = env.warmup();

    assert_every_fallback_only_chunk_certifies(&mut env, epoch_length);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_fallback_only_chunk_certifies_when_validators_track_all_shards() {
    init_test_logger();

    let epoch_length = 25;
    // Every validator tracks every shard, so they apply the chunk themselves and never need the
    // witness that gets pushed to them.
    let mut env = fallback_builder().epoch_length(epoch_length).track_all_shards().build();

    assert_every_fallback_only_chunk_certifies(&mut env, epoch_length);
}
