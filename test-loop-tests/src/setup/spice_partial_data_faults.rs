//! Fault injection for spice partial data: the knobs a test arms, what the rules saw, and the
//! network rules that apply them.

use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::Action;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
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

/// The tag of the rules `TestLoopEnv::install_spice_partial_data_faults` installs on each
/// account.
const SPICE_PARTIAL_DATA_RULES: &str = "spice_partial_data_faults";

/// Shared state for the spice partial data fault rules installed by
/// `TestLoopEnv::install_spice_partial_data_faults`. Locked separately so a test can read what the
/// rules saw without touching the faults it armed.
#[derive(Clone, Default)]
pub struct SpicePartialDataFaultState {
    pub faults: Arc<Mutex<SpicePartialDataFaults>>,
    pub observed: Arc<Mutex<SpicePartialDataObserved>>,
}

/// Faults applied to spice partial data on its way out of a node, keyed by the account sending it,
/// so a test names producers rather than nodes. Every set starts empty, and arming one mid-run
/// takes effect on the next message.
///
/// Keying by sender covers every way partial data leaves a node: the push, the all-stake fallback
/// push, and the response to a data request. Contract accesses travel as their own message and
/// are dropped separately by `drop_contract_accesses_from`; contract code is never faulted.
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

impl SpicePartialDataFaults {
    /// Whether the faults apply to `partial_data` at all: its sender is faulted somehow and its
    /// kind is not excluded.
    fn applies_to(&self, partial_data: &SpicePartialData) -> bool {
        let kind = SpiceDataKind::of(partial_data.id());
        !self.only_kind.is_some_and(|only| only != kind)
    }
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

/// What the spice partial data fault rules saw. Counted per fault kind, so a test arming several
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
    /// alongside what the rules see. The rules are keyed by account, so a restarted node keeps
    /// them; call again after `add_node` to cover an account added since.
    pub fn install_spice_partial_data_faults(
        &self,
    ) -> (Arc<Mutex<SpicePartialDataFaults>>, Arc<Mutex<SpicePartialDataObserved>>) {
        let state = self.shared_state.spice_partial_data_faults.clone();
        let rules = &self.shared_state.network_rules;
        for node in &self.node_datas {
            if !rules.tagged(&node.account_id, SPICE_PARTIAL_DATA_RULES).is_empty() {
                continue;
            }
            install_spice_partial_data_fault_rules(self, &node.account_id, &state);
        }
        (state.faults.clone(), state.observed)
    }
}

fn install_spice_partial_data_fault_rules(
    env: &TestLoopEnv,
    me: &AccountId,
    state: &SpicePartialDataFaultState,
) {
    let rules = &env.shared_state.network_rules;
    let add = |matches: Box<dyn Fn(&NetworkRequests) -> bool + Send + Sync>, action| {
        rules.add_tagged(me.clone(), SPICE_PARTIAL_DATA_RULES, matches, action)
    };

    let observed = state.observed.clone();
    let requester = me.clone();
    add(
        Box::new(|request| {
            matches!(
                request,
                NetworkRequests::SpiceDataRequest { .. }
                    | NetworkRequests::SpiceChunkEndorsement(..)
            )
        }),
        Action::Observe(Box::new(move |request| match request {
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
        })),
    );

    let faults = state.faults.clone();
    let observed = state.observed.clone();
    let sender = me.clone();
    add(
        Box::new(move |request| {
            matches!(request, NetworkRequests::SpiceChunkContractAccesses(_, _))
                && faults.lock().drop_contract_accesses_from.contains(&sender)
        }),
        Action::Replace(Box::new(move |_request| {
            observed.lock().dropped_contract_accesses += 1;
            Vec::new()
        })),
    );

    let faults = state.faults.clone();
    let observed = state.observed.clone();
    add(
        Box::new(move |request| {
            let NetworkRequests::SpicePartialData { partial_data, .. } = request else {
                return false;
            };
            let faults = faults.lock();
            faults.applies_to(partial_data) && faults.drop_from.contains(partial_data.sender())
        }),
        Action::Replace(Box::new(move |_request| {
            observed.lock().dropped += 1;
            Vec::new()
        })),
    );

    let faults = state.faults.clone();
    let rewrite_faults = state.faults.clone();
    let observed = state.observed.clone();
    add(
        Box::new(move |request| {
            let NetworkRequests::SpicePartialData { partial_data, .. } = request else {
                return false;
            };
            let faults = faults.lock();
            let sender = partial_data.sender();
            faults.applies_to(partial_data)
                && (faults.corrupt_from.contains(sender) || faults.equivocate_from.contains(sender))
        }),
        Action::Replace(Box::new(move |request| {
            let NetworkRequests::SpicePartialData { partial_data, recipients } = request else {
                unreachable!("the rule matches partial data only");
            };
            let (corrupt, equivocate) = {
                let faults = rewrite_faults.lock();
                let sender = partial_data.sender();
                (faults.corrupt_from.contains(sender), faults.equivocate_from.contains(sender))
            };
            {
                let mut observed = observed.lock();
                observed.corrupted += corrupt as usize;
                observed.equivocated += equivocate as usize;
            }
            let mut to_send = Vec::new();
            if equivocate {
                to_send.push(with_conflicting_commitment(&partial_data));
            }
            to_send.push(if corrupt { with_corrupted_part(&partial_data) } else { partial_data });
            to_send
                .into_iter()
                .map(|partial_data| NetworkRequests::SpicePartialData {
                    partial_data,
                    recipients: recipients.clone(),
                })
                .collect()
        })),
    );

    let faults = state.faults.clone();
    let delay_faults = state.faults.clone();
    let observed = state.observed.clone();
    add(
        Box::new(move |request| {
            let NetworkRequests::SpicePartialData { partial_data, .. } = request else {
                return false;
            };
            let faults = faults.lock();
            faults.applies_to(partial_data) && faults.delay_from.contains_key(partial_data.sender())
        }),
        Action::DelayBy(Box::new(move |request| {
            let NetworkRequests::SpicePartialData { partial_data, .. } = request else {
                unreachable!("the rule matches partial data only");
            };
            observed.lock().delayed += 1;
            delay_faults.lock().delay_from[partial_data.sender()]
        })),
    );
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
        .into_verified(&[signer.public_key()])
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
        .into_verified(&[signer.public_key()])
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
