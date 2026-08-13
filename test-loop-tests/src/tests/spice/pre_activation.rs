//! A spice-channel binary must run a chain whose protocol version predates spice
//! activation. In a spice build the four spice actors are spawned unconditionally,
//! so on a pre-spice chain they must stay inert at runtime.
#![cfg(feature = "test_features")] // required for the actors' drop tallies

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::create_account_id;
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::spice::activation::SpiceMessageKind;
use near_client::spice::chunk_validator_actor::SpiceChunkStateWitnessMessage;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeRequestMessage,
    SpiceContractCodeResponseMessage, SpiceIncomingPartialData, SpicePartialDataRequest,
    SpicePartialDataRequestMessage,
};
use near_network::types::NetworkRequests;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataIdentifier, testonly_create_spice_partial_data,
};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::test_utils::{create_test_signer, pre_spice_protocol_version};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{Balance, ChunkExecutionResult, SpiceChunkId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_store::DBCol;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use strum::IntoEnumIterator as _;

const EPOCH_LENGTH: u64 = 5;

/// A chain pinned to a pre-spice protocol version. `gc_num_epochs_to_keep` of 1 makes
/// garbage collection run within the test; a larger window is needed when a node is
/// killed and has to catch up afterwards.
fn setup_pre_spice_chain(num_validators: usize, gc_num_epochs_to_keep: u64) -> TestLoopEnv {
    let env = TestLoopBuilder::new()
        .validators(num_validators, 0)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            pre_spice_protocol_version(),
        ))
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .add_user_account(&create_account_id("user"), Balance::from_near(100))
        .build();
    assert!(
        !env.node(0).head_block().is_spice_block(),
        "test must run a pre-spice chain, got a spice block at the head",
    );
    env
}

/// Spice columns that must stay untouched on a pre-spice chain.
fn spice_columns() -> Vec<DBCol> {
    vec![
        DBCol::all_next_block_hashes(),
        DBCol::receipt_proofs(),
        DBCol::witnesses(),
        DBCol::endorsements(),
        DBCol::execution_results(),
        DBCol::uncertified_execution_results(),
        DBCol::uncertified_chunks(),
        DBCol::spice_endorsement_stats(),
        DBCol::contract_accesses(),
        DBCol::chunk_certifying_block(),
    ]
}

fn assert_spice_columns_empty(env: &TestLoopEnv) {
    for i in 0..env.node_datas.len() {
        let node = env.node(i);
        let identifier = &env.node_datas[i].identifier;
        for col in spice_columns() {
            let count = node.store().iter(col).count();
            assert_eq!(
                count, 0,
                "node {identifier} wrote spice column {col:?} on a pre-spice chain",
            );
        }
    }
}

/// Whether a network request carries spice traffic. Keyed on the variant name so that a
/// spice request variant added later is counted without touching this test.
fn is_spice_request(request: &NetworkRequests) -> bool {
    request.as_ref().starts_with("Spice")
}

/// Counts the network requests the nodes emit, spice ones separately.
#[derive(Clone, Default)]
struct SpiceTrafficCounter {
    total: Arc<AtomicUsize>,
    spice: Arc<AtomicUsize>,
}

impl SpiceTrafficCounter {
    /// Installs the counter on every node's peer manager.
    fn install(env: &mut TestLoopEnv) -> Self {
        let counter = Self::default();
        for node in &env.node_datas {
            counter.install_on(&mut env.test_loop.data, node);
        }
        counter
    }

    fn install_on(&self, data: &mut TestLoopData, node: &NodeExecutionData) {
        let counter = self.clone();
        let peer_actor = data.get_mut(&node.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
            counter.total.fetch_add(1, Ordering::Relaxed);
            if is_spice_request(&request) {
                counter.spice.fetch_add(1, Ordering::Relaxed);
            }
            HandlerResult::Unhandled(request)
        }));
    }

    /// Asserts no spice request was emitted. Also asserts the counter saw *some*
    /// traffic, so that a handler that silently failed to install cannot pass.
    fn assert_no_spice_traffic(&self) {
        assert!(
            self.total.load(Ordering::Relaxed) > 0,
            "the traffic counter observed no requests at all; it is not installed",
        );
        assert_eq!(
            self.spice.load(Ordering::Relaxed),
            0,
            "a spice actor sent a network request on a pre-spice chain",
        );
    }
}

/// A pre-spice chain produces, validates, executes and garbage-collects normally under a
/// spice build, and the spice actors neither write a spice column nor emit any traffic.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_pre_spice_chain_runs_under_spice_build() {
    init_test_logger();

    let mut env = setup_pre_spice_chain(2, 1);
    let traffic = SpiceTrafficCounter::install(&mut env);

    // Do real work on the chain, so the assertions below cover an executing chain and
    // not just empty blocks.
    let user = create_account_id("user");
    let tx =
        env.node(0).tx_send_money(&user, &create_account_id("validator1"), Balance::from_near(1));
    env.node_runner(0).run_tx(tx, Duration::seconds(20));

    let genesis_height = env.node(0).client().chain.genesis().height();
    assert_eq!(env.node(0).tail(), genesis_height, "GC must not have collected anything yet");
    let collected_hash =
        *env.node(0).client().chain.get_block_by_height(genesis_height + 1).unwrap().hash();

    env.node_runner(0).run_until(|node| node.tail() >= 2 * EPOCH_LENGTH, Duration::seconds(30));

    assert!(
        env.node(0).client().chain.get_block(&collected_hash).is_err(),
        "GC did not collect the block below the tail",
    );

    assert!(!env.node(0).head_block().is_spice_block(), "chain must stay pre-spice");
    traffic.assert_no_spice_traffic();
    assert_spice_columns_empty(&env);
}

/// Restarting a node on a pre-spice chain must be inert too: the actors' `start_actor`
/// recovery paths read the store head, and an ungated one would request missing spice
/// data or try to execute the chain from scratch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_pre_spice_chain_survives_node_restart() {
    init_test_logger();

    // Four validators, so the chain keeps making progress while one is down, and a wide
    // GC window, so the blocks the restarted node needs to catch up are still there.
    let mut env = setup_pre_spice_chain(4, 20);
    let traffic = SpiceTrafficCounter::install(&mut env);

    let restart_identifier = env.node_datas[0].identifier.clone();
    let restart_account = env.node_datas[0].account_id.clone();

    let kill_height = 2 * EPOCH_LENGTH;
    env.runner_for_account(&restart_account).run_until_head_height(kill_height);
    let killed_node_state = env.kill_node(&restart_identifier);

    env.node_runner(1).run_until_head_height(kill_height + EPOCH_LENGTH);

    let new_identifier = format!("{restart_identifier}-restart");
    env.restart_node(&new_identifier, killed_node_state);
    // `restart_node` appends the new node rather than replacing the killed one, which
    // still holds an entry for the same account, so address the restarted node by index.
    let restarted_index = env.node_datas.len() - 1;
    // It also builds a fresh peer manager, so re-install the counter on it.
    let restarted = env.node_datas[restarted_index].clone();
    traffic.install_on(&mut env.test_loop.data, &restarted);

    // Drive the restarted node until it has caught up to where the chain got while it
    // was down; `run_until_head_height` panics if it never does.
    let catch_up_height = env.node(1).head().height;
    env.node_runner(restarted_index).run_until_head_height(catch_up_height);

    assert!(!env.node(restarted_index).head_block().is_spice_block(), "chain must stay pre-spice",);
    traffic.assert_no_spice_traffic();
    assert_spice_columns_empty(&env);
}

/// Every spice message kind a peer can route to us is dropped, counted, and leaves no
/// trace while spice is not active.
///
/// The drop counts are read off each actor's own gate, so the assertions also pin down
/// *which* actor gated each kind.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_network_messages_are_dropped_on_pre_spice_chain() {
    init_test_logger();

    let mut env = setup_pre_spice_chain(2, 1);
    let traffic = SpiceTrafficCounter::install(&mut env);

    let node_data = env.node_datas[0].clone();
    let signer = create_test_signer(node_data.account_id.as_str());
    let requester = node_data.account_id.clone();
    let block = env.node(0).head_block();
    let shard_id = block.chunks()[0].shard_id();
    let chunk_id = SpiceChunkId { block_hash: *block.hash(), shard_id };

    node_data.spice_core_writer_sender.send(SpiceChunkEndorsementMessage(
        SpiceChunkEndorsement::new(chunk_id.clone(), new_test_execution_result(), &signer),
        RecvMessagePermit::none(),
    ));

    let data_id = SpiceDataIdentifier::Witness { block_hash: *block.hash(), shard_id };
    node_data.spice_data_distributor_sender.send(SpiceIncomingPartialData {
        data: testonly_create_spice_partial_data(
            data_id.clone(),
            SpiceDataCommitment {
                hash: CryptoHash::default(),
                root: CryptoHash::default(),
                encoded_length: 0,
            },
            vec![],
            Default::default(),
            requester.clone(),
        ),
        recv_permit: RecvMessagePermit::none(),
    });
    node_data.spice_data_distributor_sender.send(SpicePartialDataRequestMessage {
        request: SpicePartialDataRequest { data_id, requester },
        recv_permit: RecvMessagePermit::none(),
    });
    node_data.spice_data_distributor_sender.send(SpiceContractCodeRequestMessage(
        SpiceContractCodeRequest::new(chunk_id.clone(), HashSet::new(), &signer),
        RecvMessagePermit::none(),
    ));

    // The distributor forwards these two to the validator, which is where they are
    // gated, so each is counted once at the end of a two-hop route the test does not
    // have to replay by hand.
    node_data.spice_data_distributor_sender.send(SpiceChunkContractAccessesMessage(
        SpiceChunkContractAccesses::new(chunk_id.clone(), HashSet::new(), &signer),
        RecvMessagePermit::none(),
    ));
    node_data.spice_data_distributor_sender.send(SpiceContractCodeResponseMessage(
        SpiceContractCodeResponse::encode(chunk_id.clone(), &vec![]).unwrap(),
        RecvMessagePermit::none(),
    ));

    node_data.spice_chunk_validator_sender.send(
        SpiceChunkStateWitnessMessage { witness: new_test_witness(chunk_id), raw_witness_size: 0 }
            .span_wrap(),
    );

    // A message naming a block we do not have falls back to the head, which is
    // pre-spice, so it is dropped too. This is the branch a peer reaches by naming a
    // block hash we have never seen.
    let unknown_block_hash = CryptoHash::hash_bytes(b"a block this node has never seen");
    assert!(
        env.node(0).client().chain.chain_store.get_block_header(&unknown_block_hash).is_err(),
        "test must name a block the store does not have",
    );
    node_data.spice_core_writer_sender.send(SpiceChunkEndorsementMessage(
        SpiceChunkEndorsement::new(
            SpiceChunkId { block_hash: unknown_block_hash, shard_id },
            new_test_execution_result(),
            &signer,
        ),
        RecvMessagePermit::none(),
    ));

    // Let the loop deliver everything, including the distributor's forwarding hops.
    env.node_runner(0).run_for_number_of_blocks(2);

    let core_writer = env.test_loop.data.get(&node_data.spice_core_writer_sender.actor_handle());
    let distributor =
        env.test_loop.data.get(&node_data.spice_data_distributor_sender.actor_handle());
    let validator = env.test_loop.data.get(&node_data.spice_chunk_validator_sender.actor_handle());

    for kind in SpiceMessageKind::iter() {
        let (dropped, expected) = match kind {
            // Injected twice, once naming a known pre-spice block and once an unknown one.
            SpiceMessageKind::ChunkEndorsement => (core_writer.spice_dropped_count(kind), 2),
            SpiceMessageKind::PartialData
            | SpiceMessageKind::PartialDataRequest
            | SpiceMessageKind::ContractCodeRequest => (distributor.spice_dropped_count(kind), 1),
            // Gated at the validator, at the far end of the distributor's forwarding hop.
            SpiceMessageKind::ContractAccesses
            | SpiceMessageKind::ContractCodeResponse
            | SpiceMessageKind::StateWitness => (validator.spice_dropped_count(kind), 1),
        };
        assert_eq!(dropped, expected, "unexpected drop count for {} messages", kind.as_str());
    }

    traffic.assert_no_spice_traffic();
    assert_spice_columns_empty(&env);
}

fn new_test_execution_result() -> ChunkExecutionResult {
    ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
        outgoing_receipts_root: CryptoHash::default(),
    }
}

fn new_test_witness(chunk_id: SpiceChunkId) -> SpiceChunkStateWitness {
    SpiceChunkStateWitness::new(
        chunk_id,
        PartialState::TrieValues(vec![]),
        HashMap::new(),
        CryptoHash::default(),
        vec![],
        BTreeSet::new(),
        None,
    )
}
