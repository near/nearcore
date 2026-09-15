//! Spice data distribution under injected faults. Each test arms a fault against a producer, then
//! asserts the fault fired and that the nodes which produce nothing still endorsed — which they
//! can only do after receiving and validating the witness. The producers certify among themselves
//! either way, so chain progress alone would prove nothing; see
//! `test_spice_partial_data_faults_starve_the_recipients`.
//!
//! Faults are keyed by the sending account, so every recipient sees the same treatment and all of
//! them are asserted on: a fault reaching only some recipients is a harness bug.
// TODO(spice-data-distribution): assert how many chunks the recipient endorsed rather than that it
// endorsed at all. Under dropped or delayed pushes the slower recipient recovers a fraction of what
// the faster one does, which having endorsed cannot tell apart from recovering every chunk.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::spice_partial_data_faults::{
    SpiceDataKind, SpicePartialDataFaults, SpicePartialDataObserved,
};
use crate::setup::state::NodeExecutionData;
use near_async::time::Duration;
use near_client::spice::data_distributor_actor::{DATA_PARTS_RATIO, DATA_REQUEST_INTERVAL};
use near_o11y::testonly::init_test_logger;
use near_primitives::reed_solomon::reed_solomon_num_data_parts;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use parking_lot::Mutex;
use std::sync::Arc;

const NUM_PRODUCERS: usize = 4;
/// Chunk validators that produce nothing, so they have to receive every witness.
const NUM_VALIDATORS: usize = 2;

struct Setup {
    env: TestLoopEnv,
    producers: Vec<AccountId>,
    recipients: Vec<AccountId>,
    faults: Arc<Mutex<SpicePartialDataFaults>>,
    observed: Arc<Mutex<SpicePartialDataObserved>>,
}

fn setup() -> Setup {
    setup_with_shards(1, NUM_PRODUCERS)
}

/// A setup with `num_shards` shards and `num_producers` producers spread over them. Receipt proofs
/// only travel between shards, so testing them needs more than one.
fn setup_with_shards(num_shards: usize, num_producers: usize) -> Setup {
    init_test_logger();
    let mut builder = TestLoopBuilder::new().validators(num_producers, NUM_VALIDATORS);
    if num_shards > 1 {
        builder = builder.num_shards(num_shards);
    }
    let mut env = builder.build();
    let account_ids = |nodes: &[NodeExecutionData]| {
        nodes.iter().map(|n| n.account_id.clone()).collect::<Vec<_>>()
    };
    let producers = account_ids(&env.node_datas[..num_producers]);
    let recipients = account_ids(&env.node_datas[num_producers..]);
    let (faults, observed) = env.install_spice_partial_data_faults();
    Setup { env, producers, recipients, faults, observed }
}

/// The shards of the current epoch, and the producers of each.
fn shard_producers(env: &TestLoopEnv, observer: &AccountId) -> Vec<(ShardId, Vec<AccountId>)> {
    let node = env.node_for_account(observer);
    let epoch_id = node.client().chain.head().unwrap().epoch_id;
    let epoch_manager = node.client().epoch_manager.as_ref();
    epoch_manager
        .shard_ids(&epoch_id)
        .unwrap()
        .into_iter()
        .map(|shard_id| {
            let producers =
                epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap();
            (shard_id, producers)
        })
        .collect()
}

/// Runs `heights` blocks past the certified frontier, panicking if the chain stalls.
fn run_past_certified_frontier(env: &mut TestLoopEnv, observer: &AccountId, heights: BlockHeight) {
    let target = env.node_for_account(observer).last_certified_block_header().height() + heights;
    env.runner_for_account(observer).run_until_certified(target);
}

/// Heights to run when the data arrives by push.
const PUSH_HEIGHTS: BlockHeight = 4;
/// Heights to run when the data has to be pulled. Four is not enough: pulls all go to the one
/// producer still answering, and it serves its requesters unevenly.
const PULL_HEIGHTS: BlockHeight = 8;

/// Parts of an item that have to arrive for a recipient to decode it without asking anyone. Faults
/// are armed against whole producer sets, which only lines up with the data while there is one
/// shard, so this asserts that rather than assuming it.
fn single_shard_decode_threshold(env: &TestLoopEnv, observer: &AccountId) -> usize {
    let shards = shard_producers(env, observer);
    let [(_, producers)] = &shards[..] else {
        panic!("faults are armed per producer set, which needs a single shard");
    };
    reed_solomon_num_data_parts(producers.len(), DATA_PARTS_RATIO)
}

fn assert_endorsed(observed: &SpicePartialDataObserved, recipients: &[AccountId]) {
    for recipient in recipients {
        assert!(observed.endorsements.contains_key(recipient), "{recipient} never endorsed");
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_dropped_pushes() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // All but one producer goes silent, leaving too few parts to decode from pushes alone, so a
    // recipient can only finish an item by requesting it from the one that still answers.
    let silent = &producers[1..];
    assert!(
        NUM_PRODUCERS - silent.len() < single_shard_decode_threshold(&env, &recipients[0]),
        "the producers left alone still push enough parts to decode, so recovery never runs"
    );
    faults.lock().drop_from.extend(silent.iter().cloned());
    // With every accesses message gone too, a recipient has nothing but what it pulls.
    faults.lock().drop_contract_accesses_from.extend(producers.iter().cloned());

    run_past_certified_frontier(&mut env, &recipients[0], PULL_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.dropped > 0, "no push was dropped");
    assert!(observed.dropped_contract_accesses > 0, "no accesses message was dropped");
    for recipient in &recipients {
        assert!(
            observed.data_requests.contains_key(recipient),
            "{recipient} never had to request the dropped data"
        );
    }
    assert_endorsed(&observed, &recipients);
}

/// The receipt-proof twin of `dropped_pushes`: drop the receipt proofs most of one
/// shard's producers send, leaving too few pushed parts to decode, so the other shard's
/// producers can only keep applying — and certification advancing — by requesting the
/// data from the producer still answering. Contrast `starve_receipt_proofs`, where the
/// whole shard goes silent and certification stops for good.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_dropped_receipt_proof_pushes() {
    let Setup { mut env, producers, recipients, faults, observed } = setup_with_shards(2, 8);
    let mut shards = shard_producers(&env, &recipients[0]);
    shards.sort_by_key(|(shard_id, _)| *shard_id);
    let [(_, silenced_shard), (_, other_shard)] = &shards[..] else {
        panic!("expected two shards")
    };
    assert_eq!(silenced_shard.len() + other_shard.len(), producers.len());
    // With four producers per shard a proof decodes from two parts, so three silent
    // producers leave one pushed part — not enough without requesting.
    let silent = &silenced_shard[1..];
    assert!(
        silenced_shard.len() - silent.len()
            < reed_solomon_num_data_parts(silenced_shard.len(), DATA_PARTS_RATIO),
        "the producers left alone still push enough parts to decode, so recovery never runs"
    );
    faults.lock().only_kind = Some(SpiceDataKind::ReceiptProof);
    faults.lock().drop_from.extend(silent.iter().cloned());

    run_past_certified_frontier(&mut env, &recipients[0], 2 * PULL_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.dropped > 0, "no receipt proof was dropped");
    // The dropped proofs' recipients are the other shard's producers; every one of them
    // has to request the missing parts to keep applying its shard.
    for recipient in other_shard {
        assert!(
            observed.data_requests.contains_key(recipient),
            "{recipient} never had to request the dropped receipt proofs"
        );
    }
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_delayed_pushes() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    let delayed = &producers[1..];
    assert!(
        NUM_PRODUCERS - delayed.len() < single_shard_decode_threshold(&env, &recipients[0]),
        "the producers left alone still push enough parts to decode, so recovery never runs"
    );
    // A recipient has to start requesting before the pushes land.
    let delay = Duration::seconds(2);
    assert!(delay > DATA_REQUEST_INTERVAL, "the delay has to outlast a request round");
    faults.lock().delay_from.extend(delayed.iter().cloned().map(|producer| (producer, delay)));

    run_past_certified_frontier(&mut env, &recipients[0], PULL_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.delayed > 0, "no push was delayed");
    for recipient in &recipients {
        assert!(
            observed.data_requests.contains_key(recipient),
            "the delay never outlasted {recipient}'s request interval"
        );
    }
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_corrupted_parts() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // The part is signed but fails its merkle proof, so a recipient has to reject the part without
    // rejecting the item. The producers left alone still push enough parts to decode.
    assert!(
        NUM_PRODUCERS - 1 >= single_shard_decode_threshold(&env, &recipients[0]),
        "one silent producer alone stops the decode"
    );
    faults.lock().corrupt_from.insert(producers[0].clone());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.corrupted > 0, "no part was corrupted");
    assert!(
        observed.data_requests.is_empty(),
        "the corrupted part pushed a recipient into pulling"
    );
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_equivocating_producer() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // Two commitments for one item from one producer, both holding a part that verifies. The
    // honest one still decodes, so the conflicting tracker must not displace it.
    faults.lock().equivocate_from.insert(producers[0].clone());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.equivocated > 0, "no conflicting commitment was sent");
    assert!(
        observed.data_requests.is_empty(),
        "the conflicting commitment pushed a recipient into pulling"
    );
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_with_dropped_contract_accesses() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    faults.lock().drop_contract_accesses_from.extend(producers.iter().cloned());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.dropped_contract_accesses > 0, "no accesses message was dropped");
    assert!(observed.data_requests.is_empty(), "the pushed parts were enough; nothing to pull");
    assert_endorsed(&observed, &recipients);
}

/// The fault-free baseline for the fault tests: pushes alone carry every witness, so the recipients
/// endorse without ever requesting. Without it the `data_requests` assertions elsewhere would also
/// pass on a chain that requests all the time for unrelated reasons.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_none_armed() {
    let Setup { mut env, producers: _, recipients, faults: _, observed } = setup();

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert_eq!(observed.dropped + observed.delayed + observed.corrupted + observed.equivocated, 0);
    assert!(observed.data_requests.is_empty(), "a healthy chain requested data");
    assert_endorsed(&observed, &recipients);
}

/// The control for the fault tests: with every producer silent the recipients have nothing to
/// validate and never endorse, while the producers keep certifying among themselves. If this ever
/// starts endorsing, the others are passing without the data path being exercised.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_starve_the_recipients() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    faults.lock().drop_from.extend(producers.iter().cloned());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.dropped > 0, "no push was dropped");
    for recipient in &recipients {
        assert!(
            !observed.endorsements.contains_key(recipient),
            "{recipient} endorsed without data"
        );
    }
}

/// Receipt proofs only travel between shards, so this is the one test with more than one. It
/// certifies first with nothing armed, proving the proofs flow, then drops the ones a whole shard
/// sends: without them the other shard cannot apply, and certification stops for good while blocks
/// keep being produced.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_starve_receipt_proofs() {
    /// Enough for the shards to exchange receipt proofs and certify a few chunks.
    const HEALTHY_BLOCKS: usize = 4;
    /// Enough for anything already in flight to land, and then for a stall to be unambiguous.
    const STARVED_BLOCKS: usize = 8;

    let Setup { mut env, producers, recipients, faults, observed } = setup_with_shards(2, 8);
    let mut shards = shard_producers(&env, &recipients[0]);
    shards.sort_by_key(|(shard_id, _)| *shard_id);
    let [(_, silenced), (_, other)] = &shards[..] else { panic!("expected two shards") };
    assert_eq!(silenced.len() + other.len(), producers.len(), "every producer serves a shard");

    env.runner_for_account(&recipients[0]).run_for_number_of_blocks(HEALTHY_BLOCKS);
    let healthy = env.node_for_account(&recipients[0]).last_certified_block_header().height();
    assert!(healthy > 0, "nothing certified before a fault was armed");

    faults.lock().only_kind = Some(SpiceDataKind::ReceiptProof);
    faults.lock().drop_from.extend(silenced.iter().cloned());

    env.runner_for_account(&recipients[0]).run_for_number_of_blocks(STARVED_BLOCKS);
    let draining = env.node_for_account(&recipients[0]).last_certified_block_header().height();
    env.runner_for_account(&recipients[0]).run_for_number_of_blocks(STARVED_BLOCKS);

    let node = env.node_for_account(&recipients[0]);
    let certified = node.last_certified_block_header().height();
    assert_eq!(certified, draining, "certification kept going without the receipt proofs");
    assert!(node.head().height > certified, "blocks stopped being produced too");
    assert!(observed.lock().dropped > 0, "no receipt proof was dropped");
}

/// Faults and delayed endorsements are separate handlers on the same peer manager, and the
/// endorsement one consumes what it delays. Counting endorsements is an observer rather than a
/// handler so that arming both still records them.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_partial_data_faults_alongside_delayed_endorsements() {
    /// Blocks an endorsement is held back for.
    const ENDORSEMENT_DELAY: BlockHeight = 2;
    /// Blocks to run. Certification crawls while endorsements are held back, so this waits on
    /// blocks rather than on certified height.
    const BLOCKS: usize = 14;

    let Setup { mut env, producers, recipients, faults, observed } = setup();
    env.delay_endorsements_propagation(ENDORSEMENT_DELAY);
    faults.lock().corrupt_from.insert(producers[0].clone());

    env.runner_for_account(&recipients[0]).run_for_number_of_blocks(BLOCKS);

    let observed = observed.lock();
    assert!(observed.corrupted > 0, "no part was corrupted");
    assert_endorsed(&observed, &recipients);
}
