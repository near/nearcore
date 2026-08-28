//! Fault injection for spice partial data: the knobs a test arms, what the handler saw, and the
//! network handler that applies them.

use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::{HandlerResult, TestLoopNetworkSharedState};
use crate::setup::state::{NETWORK_DELAY, NodeExecutionData};
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_client::spice::data_distributor_actor::SpiceDataDistributorActor;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::SpiceIncomingPartialData;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_primitives::hash::hash;
use near_primitives::merkle::compute_root_from_path_and_item;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataIdentifier, SpicePartialData,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, SpiceChunkId};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Shared state for the spice partial data fault handler installed by
/// `TestLoopEnv::install_spice_partial_data_faults`. Locked separately so a test can read what the
/// handler saw without touching the faults it armed.
#[derive(Clone, Default)]
pub struct SpicePartialDataFaultState {
    pub faults: Arc<Mutex<SpicePartialDataFaults>>,
    pub observed: Arc<Mutex<SpicePartialDataObserved>>,
    wiring: Arc<Mutex<SpicePartialDataFaultWiring>>,
}

/// Faults applied to spice partial data on its way out of a node, keyed by the account sending it,
/// so a test names producers rather than nodes. Every set starts empty, and arming one mid-run
/// takes effect on the next message.
///
/// Keying by sender covers every way partial data leaves a node: the push, the all-stake fallback
/// push, and the response to a data request. Contract accesses and contract code travel as their
/// own messages and are never faulted.
// TODO(spice-data-distribution): fault contract code responses too, so a test can drop them or
// answer with bytes that do not hash to the requested code.
#[derive(Default)]
pub struct SpicePartialDataFaults {
    /// Partial data these accounts send never arrives.
    pub drop_from: HashSet<AccountId>,
    /// Partial data these accounts send arrives this much later than usual (test-loop time).
    pub delay_from: HashMap<AccountId, Duration>,
    /// One byte of one part is flipped and the message re-signed, so the fault is attributable to
    /// the sender rather than to a broken signature.
    pub corrupt_from: HashSet<AccountId>,
    /// These accounts send their parts a second time under a conflicting commitment.
    pub equivocate_from: HashSet<AccountId>,
    /// Limits every fault above to one kind of data. `None` faults both kinds.
    pub only_kind: Option<SpiceDataKind>,
    /// Contract accesses these accounts send never arrive. Partial data is unaffected.
    pub drop_contract_accesses_from: HashSet<AccountId>,
}

/// Which kind of partial data a fault applies to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpiceDataKind {
    Witness,
    ReceiptProof,
}

impl SpiceDataKind {
    fn of(id: &SpiceDataIdentifier) -> Self {
        match id {
            SpiceDataIdentifier::Witness { .. } => Self::Witness,
            SpiceDataIdentifier::ReceiptProof { .. } => Self::ReceiptProof,
        }
    }
}

/// Where the handler delivers the faulty copies it makes, and which nodes already have it.
#[derive(Default)]
struct SpicePartialDataFaultWiring {
    installed_for: HashSet<String>,
    senders: HashMap<AccountId, TestLoopSender<SpiceDataDistributorActor>>,
}

/// What the spice partial data fault handler saw. Counted per fault kind, so a test arming several
/// can still tell which of them fired.
#[derive(Default)]
pub struct SpicePartialDataObserved {
    /// Messages dropped, delayed, corrupted, and sent again under a conflicting commitment. One
    /// message can be counted by more than one of these.
    pub dropped: usize,
    pub delayed: usize,
    pub corrupted: usize,
    pub equivocated: usize,
    /// Contract accesses messages dropped.
    pub dropped_contract_accesses: usize,
    /// Data requests seen per requesting node. Empty on a healthy chain, where pushes are enough,
    /// so it shows which nodes a fault pushed onto the recovery path.
    // TODO(spice-data-distribution): record the producer asked and the ordinals requested, so a
    // test can assert how a missing set was split across sources and how often one source is
    // asked.
    pub data_requests: HashMap<AccountId, usize>,
    /// Chunks each account endorsed. A node that produces nothing can only endorse after receiving
    /// and validating the witness, so this is where data delivery is observable. Endorsements are
    /// sent once per recipient and re-broadcast until they are on chain, so they are collected as a
    /// set of chunks rather than counted.
    pub endorsements: HashMap<AccountId, HashSet<SpiceChunkId>>,
}

impl TestLoopEnv {
    /// Installs spice partial data fault injection on every node and returns the shared knobs
    /// alongside what the handler sees. Call again after `add_node` / `restart_node` to instrument
    /// nodes added since.
    pub fn install_spice_partial_data_faults(
        &mut self,
    ) -> (Arc<Mutex<SpicePartialDataFaults>>, Arc<Mutex<SpicePartialDataObserved>>) {
        let state = self.shared_state.spice_partial_data_faults.clone();
        {
            let mut wiring = state.wiring.lock();
            wiring.senders = self
                .node_datas
                .iter()
                .map(|n| (n.account_id.clone(), n.spice_data_distributor_sender.clone()))
                .collect();
        }
        let network = self.shared_state.network_shared_state.clone();
        for node in &self.node_datas {
            if !state.wiring.lock().installed_for.insert(node.identifier.clone()) {
                continue;
            }
            install_spice_partial_data_fault_handler(
                &mut self.test_loop.data,
                node,
                state.clone(),
                network.clone(),
            );
        }
        (state.faults.clone(), state.observed)
    }
}

fn install_spice_partial_data_fault_handler(
    data: &mut TestLoopData,
    node: &NodeExecutionData,
    state: SpicePartialDataFaultState,
    network: TestLoopNetworkSharedState,
) {
    let me = node.account_id.clone();
    let peer_actor = data.get_mut(&node.peer_manager_sender.actor_handle());

    // Counting is an observer, not a handler: another handler claiming endorsements would otherwise
    // decide whether any of this is seen, depending on which was registered last.
    let observed = state.observed.clone();
    let requester = me.clone();
    peer_actor.register_observer(Box::new(move |request| match request {
        NetworkRequests::SpiceDataRequest { .. } => {
            *observed.lock().data_requests.entry(requester.clone()).or_default() += 1;
        }
        NetworkRequests::SpiceChunkEndorsement(_target, endorsement) => {
            let chunk_id = SpiceChunkId {
                block_hash: *endorsement.block_hash(),
                shard_id: endorsement.shard_id(),
            };
            observed
                .lock()
                .endorsements
                .entry(endorsement.account_id().clone())
                .or_default()
                .insert(chunk_id);
        }
        _ => {}
    }));

    let accesses_state = state.clone();
    let accesses_sender = me.clone();
    peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        let NetworkRequests::SpiceChunkContractAccesses(_, _) = &request else {
            return HandlerResult::Unhandled(request);
        };
        if !accesses_state.faults.lock().drop_contract_accesses_from.contains(&accesses_sender) {
            return HandlerResult::Unhandled(request);
        }
        accesses_state.observed.lock().dropped_contract_accesses += 1;
        HandlerResult::Handled(NetworkResponses::NoResponse)
    }));

    peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        let NetworkRequests::SpicePartialData { partial_data, recipients } = &request else {
            return HandlerResult::Unhandled(request);
        };
        let sender = partial_data.sender().clone();
        let kind = SpiceDataKind::of(partial_data.id());
        let (drop_it, delay, corrupt, equivocate) = {
            let faults = state.faults.lock();
            if faults.only_kind.is_some_and(|only| only != kind) {
                return HandlerResult::Unhandled(request);
            }
            (
                faults.drop_from.contains(&sender),
                faults.delay_from.get(&sender).copied(),
                faults.corrupt_from.contains(&sender),
                faults.equivocate_from.contains(&sender),
            )
        };
        if drop_it {
            state.observed.lock().dropped += 1;
            return HandlerResult::Handled(NetworkResponses::NoResponse);
        }
        if delay.is_none() && !corrupt && !equivocate {
            return HandlerResult::Unhandled(request);
        }
        {
            let mut observed = state.observed.lock();
            observed.delayed += delay.is_some() as usize;
            observed.corrupted += corrupt as usize;
            observed.equivocated += equivocate as usize;
        }

        let mut to_send = Vec::new();
        if equivocate {
            to_send.push(with_conflicting_commitment(partial_data));
        }
        to_send.push(if corrupt {
            with_corrupted_part(partial_data)
        } else {
            partial_data.clone()
        });

        // The peer manager would deliver a rewritten request, but it cannot delay one or turn one
        // request into two, which is what the delay and equivocate faults need. So these messages
        // go out from here, which skips the routing — including its severed-link check, redone
        // below.
        // TODO(spice-data-distribution): count what each recipient is handed, so a test can assert
        // on delivery directly instead of inferring it from whether the recipient endorsed.
        let delay = NETWORK_DELAY + delay.unwrap_or(Duration::ZERO);
        let reachable = network.reachable_from(&me, recipients);
        let recipient_actors: Vec<_> = {
            let wiring = state.wiring.lock();
            reachable
                .iter()
                .filter_map(|recipient| wiring.senders.get(recipient))
                .map(|actor| actor.clone().with_delay(delay))
                .collect()
        };
        for recipient_actor in recipient_actors {
            for data in &to_send {
                recipient_actor.send(SpiceIncomingPartialData {
                    data: data.clone(),
                    recv_permit: RecvMessagePermit::none(),
                });
            }
        }
        HandlerResult::Handled(NetworkResponses::NoResponse)
    }));
}

/// Builds a `SpicePartialData` whose first part has a flipped byte, keeping the commitment, so the
/// part fails its merkle proof. A push carries one part, so such a message contributes nothing.
// TODO(spice-data-distribution): a pull response carries several parts, and how much of one the
// receiver keeps depends on where the bad part sits. Corrupting a chosen ordinal is only worth
// testing once the receiver verifies every part before inserting any.
fn with_corrupted_part(data: &SpicePartialData) -> SpicePartialData {
    let signer = create_test_signer(data.sender().as_str());
    let mut verified = data
        .clone()
        .into_verified(&signer.public_key())
        .expect("test-loop nodes sign with create_test_signer keys");
    let part = verified.parts.first_mut().expect("pushed data carries a part");
    let byte = part.part.first_mut().expect("part is not empty");
    *byte ^= 1;
    SpicePartialData::new(verified.id, verified.commitment, verified.parts, &signer)
}

/// Builds a second `SpicePartialData` carrying the sender's first part with a flipped byte, under a
/// commitment of its own: the root comes from the part's own merkle path, so the part verifies and
/// the recipient tracks a second commitment for the item, holding parts under both.
fn with_conflicting_commitment(data: &SpicePartialData) -> SpicePartialData {
    let signer = create_test_signer(data.sender().as_str());
    let verified = data
        .clone()
        .into_verified(&signer.public_key())
        .expect("test-loop nodes sign with create_test_signer keys");
    let mut part = verified.parts.into_iter().next().expect("partial data carries a part");
    let byte = part.part.first_mut().expect("part is not empty");
    *byte ^= 1;
    let commitment = SpiceDataCommitment {
        root: compute_root_from_path_and_item(&part.merkle_proof, &part.part),
        hash: hash(verified.commitment.hash.as_bytes()),
        encoded_length: verified.commitment.encoded_length,
    };
    SpicePartialData::new(verified.id, commitment, vec![part], &signer)
}
