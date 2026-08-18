//! Spice data distribution under injected faults. Each test arms a fault against a producer, then
//! asserts the fault fired and that the nodes which produce nothing still endorsed — which they
//! can only do after receiving and validating the witness. The producers certify among themselves
//! either way, so chain progress alone would prove nothing; see
//! `test_spice_data_faults_starve_the_recipients`.
//!
//! Faults are keyed by the sending account, so every recipient sees the same treatment and all of
//! them are asserted on: a fault reaching only some recipients is a harness bug.
// TODO(spice-data-distribution): assert how many chunks the recipient endorsed rather than that it
// endorsed at all. Under dropped or delayed pushes it recovers one chunk of the four it runs for,
// which having endorsed cannot tell apart from recovering every one.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::{NodeExecutionData, SpiceDataFaults, SpiceDataObserved};
use near_async::time::Duration;
use near_client::spice::data_distributor_actor::DATA_PARTS_RATIO;
use near_o11y::testonly::init_test_logger;
use near_primitives::reed_solomon::reed_solomon_num_data_parts;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::sync::Arc;

const NUM_PRODUCERS: usize = 4;
/// Chunk validators that produce nothing, so they have to receive every witness.
const NUM_VALIDATORS: usize = 2;

struct Setup {
    env: TestLoopEnv,
    producers: Vec<AccountId>,
    recipients: Vec<AccountId>,
    faults: Arc<Mutex<SpiceDataFaults>>,
    observed: Arc<Mutex<SpiceDataObserved>>,
}

fn setup() -> Setup {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(NUM_PRODUCERS, NUM_VALIDATORS).build();
    let account_ids = |nodes: &[NodeExecutionData]| {
        nodes.iter().map(|n| n.account_id.clone()).collect::<Vec<_>>()
    };
    let producers = account_ids(&env.node_datas[..NUM_PRODUCERS]);
    let recipients = account_ids(&env.node_datas[NUM_PRODUCERS..]);
    let (faults, observed) = env.install_spice_data_faults();
    Setup { env, producers, recipients, faults, observed }
}

/// Runs `heights` blocks past the certified frontier, panicking if the chain stalls.
fn run_past_certified_frontier(env: &mut TestLoopEnv, observer: &AccountId, heights: BlockHeight) {
    let target = env.node_for_account(observer).last_certified_block_header().height() + heights;
    env.runner_for_account(observer).run_until_certified(target);
}

/// Heights a push needs to reach every recipient.
const PUSH_HEIGHTS: BlockHeight = 4;
/// Heights recovery needs. Requests go to one producer, which serves the recipients unevenly, so
/// the slowest of them wants roughly twice the room the push path does.
const RECOVERY_HEIGHTS: BlockHeight = 8;

/// Parts that have to arrive by push for a recipient to decode without asking anyone.
fn decode_threshold() -> usize {
    reed_solomon_num_data_parts(NUM_PRODUCERS, DATA_PARTS_RATIO)
}

fn assert_endorsed(observed: &SpiceDataObserved, recipients: &[AccountId]) {
    for recipient in recipients {
        assert!(observed.endorsements.contains_key(recipient), "{recipient} never endorsed");
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_data_faults_with_dropped_pushes() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // All but one producer goes silent, leaving too few parts to decode from pushes alone, so a
    // recipient can only finish an item by requesting it from the one that still answers.
    let silent = &producers[1..];
    assert!(
        NUM_PRODUCERS - silent.len() < decode_threshold(),
        "the producers left alone still push enough parts to decode, so recovery never runs"
    );
    faults.lock().drop_from.extend(silent.iter().cloned());

    run_past_certified_frontier(&mut env, &recipients[0], RECOVERY_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.dropped > 0, "no push was dropped");
    for recipient in &recipients {
        assert!(
            observed.data_requests.contains_key(recipient),
            "{recipient} never had to request the dropped data"
        );
    }
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_data_faults_with_delayed_pushes() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // Longer than the request interval, so a recipient starts requesting before the pushes land.
    let delayed = &producers[1..];
    assert!(
        NUM_PRODUCERS - delayed.len() < decode_threshold(),
        "the producers left alone still push enough parts to decode, so recovery never runs"
    );
    faults
        .lock()
        .delay_from
        .extend(delayed.iter().cloned().map(|producer| (producer, Duration::seconds(2))));

    run_past_certified_frontier(&mut env, &recipients[0], RECOVERY_HEIGHTS);

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
fn test_spice_data_faults_with_corrupted_parts() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // The part is signed but fails its merkle proof, so a recipient has to reject the part without
    // rejecting the item. The producers left alone still push enough parts to decode.
    assert!(NUM_PRODUCERS - 1 >= decode_threshold(), "one silent producer alone stops the decode");
    faults.lock().corrupt_from.insert(producers[0].clone());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.corrupted > 0, "no part was corrupted");
    assert_endorsed(&observed, &recipients);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_data_faults_with_equivocating_producer() {
    let Setup { mut env, producers, recipients, faults, observed } = setup();
    // Two commitments for one item from one producer, both holding a part that verifies. The
    // honest one still decodes, so the conflicting tracker must not displace it.
    faults.lock().equivocate_from.insert(producers[0].clone());

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert!(observed.equivocated > 0, "no conflicting commitment was sent");
    assert_endorsed(&observed, &recipients);
}

/// The fault-free baseline for the four tests above: pushes alone carry every witness, so the
/// recipients endorse without ever requesting. Without this the `data_requests` assertions there
/// would also pass on a chain that requests all the time for unrelated reasons.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_data_faults_none_armed() {
    let Setup { mut env, producers: _, recipients, faults: _, observed } = setup();

    run_past_certified_frontier(&mut env, &recipients[0], PUSH_HEIGHTS);

    let observed = observed.lock();
    assert_eq!(observed.dropped + observed.delayed + observed.corrupted + observed.equivocated, 0);
    assert!(observed.data_requests.is_empty(), "a healthy chain requested data");
    assert_endorsed(&observed, &recipients);
}

/// The control for the four tests above: with every producer silent the recipients have nothing to
/// validate and never endorse, while the producers keep certifying among themselves. If this ever
/// starts endorsing, the other tests are passing without the data path being exercised.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_data_faults_starve_the_recipients() {
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
