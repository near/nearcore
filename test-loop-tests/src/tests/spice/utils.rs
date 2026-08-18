use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::{HandlerResult, TestLoopNetworkSharedState};
use crate::setup::state::{
    NETWORK_DELAY, NodeExecutionData, SpiceDataFaultState, SpiceDataFaults, SpiceDataObserved,
    SpiceEndorsementDelayState,
};
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::SpiceIncomingPartialData;
use near_network::types::NetworkRequests;
use near_network::types::NetworkResponses;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::compute_root_from_path_and_item;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpicePartialData};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, BlockHeight, SpiceChunkId};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl TestLoopEnv {
    /// Set the endorsement propagation delay to `delay_height` blocks. Safe
    /// to call unconditionally; installs the network handler lazily on the
    /// first non-zero call, and re-running it after `add_node` /
    /// `restart_node` instruments any node that doesn't have the handler yet.
    pub fn delay_endorsements_propagation(&mut self, delay_height: u64) {
        let state = self.shared_state.spice_endorsement_delay.clone();
        if state.lock().installed_for.is_empty() && delay_height == 0 {
            return;
        }
        for node in &self.node_datas {
            node.set_expected_execution_delay(delay_height);
        }
        // Refresh routing so handlers installed earlier can still deliver
        // endorsements to nodes added since.
        {
            let mut state = state.lock();
            state.senders = self
                .node_datas
                .iter()
                .map(|n| (n.account_id.clone(), n.spice_core_writer_sender.clone()))
                .collect();
        }
        for node in &self.node_datas {
            if !state.lock().installed_for.insert(node.identifier.clone()) {
                continue;
            }
            install_endorsement_delay_handler(&mut self.test_loop.data, node, state.clone());
        }
    }
}

impl TestLoopEnv {
    /// Installs spice data fault injection on every node and returns the shared knobs alongside
    /// what the handler sees. Call again after `add_node` / `restart_node` to instrument nodes
    /// added since.
    pub fn install_spice_data_faults(
        &mut self,
    ) -> (Arc<Mutex<SpiceDataFaults>>, Arc<Mutex<SpiceDataObserved>>) {
        let state = self.shared_state.spice_data_faults.clone();
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
            install_spice_data_fault_handler(
                &mut self.test_loop.data,
                node,
                state.clone(),
                network.clone(),
            );
        }
        (state.faults.clone(), state.observed)
    }
}

fn install_spice_data_fault_handler(
    data: &mut TestLoopData,
    node: &NodeExecutionData,
    state: SpiceDataFaultState,
    network: TestLoopNetworkSharedState,
) {
    let me = node.account_id.clone();
    let peer_actor = data.get_mut(&node.peer_manager_sender.actor_handle());
    peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        if let NetworkRequests::SpiceDataRequest { .. } = &request {
            *state.observed.lock().data_requests.entry(me.clone()).or_default() += 1;
            return HandlerResult::Unhandled(request);
        }
        if let NetworkRequests::SpiceChunkEndorsement(_target, endorsement) = &request {
            let chunk_id = SpiceChunkId {
                block_hash: *endorsement.block_hash(),
                shard_id: endorsement.shard_id(),
            };
            state
                .observed
                .lock()
                .endorsements
                .entry(endorsement.account_id().clone())
                .or_default()
                .insert(chunk_id);
            return HandlerResult::Unhandled(request);
        }
        let NetworkRequests::SpicePartialData { partial_data, recipients } = &request else {
            return HandlerResult::Unhandled(request);
        };
        let sender = partial_data.sender().clone();
        let (drop_it, delay, corrupt, equivocate) = {
            let faults = state.faults.lock();
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

        // Delivered here rather than passed on, so the faulty copies keep the delay and the extra
        // message the normal path cannot express. Severed links have to be honoured by hand
        // because that check lives in the routing this bypasses.
        // TODO(spice-data-distribution): count what each recipient is handed, so a test can assert
        // on delivery directly instead of inferring it from whether the recipient endorsed.
        let delay = NETWORK_DELAY + delay.unwrap_or(Duration::ZERO);
        let senders: Vec<_> = {
            let wiring = state.wiring.lock();
            recipients
                .iter()
                .filter(|recipient| network.is_link_allowed(&me, recipient))
                .filter_map(|recipient| wiring.senders.get(recipient))
                .map(|sender| sender.clone().with_delay(delay))
                .collect()
        };
        for recipient_sender in senders {
            for data in &to_send {
                recipient_sender.send(SpiceIncomingPartialData {
                    data: data.clone(),
                    recv_permit: RecvMessagePermit::none(),
                });
            }
        }
        HandlerResult::Handled(NetworkResponses::NoResponse)
    }));
}

/// Flips a byte of the first part, keeping the commitment, so the part fails its merkle proof.
fn with_corrupted_part(data: &SpicePartialData) -> SpicePartialData {
    let signer = create_test_signer(data.sender().as_str());
    let mut verified = data.clone().into_verified(&signer.public_key()).unwrap();
    let part = verified.parts.first_mut().expect("pushed data carries a part");
    let byte = part.part.first_mut().expect("part is not empty");
    *byte ^= 1;
    SpicePartialData::new(verified.id, verified.commitment, verified.parts, &signer)
}

/// Re-sends the sender's first part with a flipped byte under a commitment built around it: the
/// root comes from the part's own merkle path, so the part verifies and the recipient tracks a
/// second commitment for the item, holding parts under both.
fn with_conflicting_commitment(data: &SpicePartialData) -> SpicePartialData {
    let signer = create_test_signer(data.sender().as_str());
    let verified = data.clone().into_verified(&signer.public_key()).unwrap();
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

fn install_endorsement_delay_handler(
    data: &mut TestLoopData,
    node: &NodeExecutionData,
    state: Arc<Mutex<SpiceEndorsementDelayState>>,
) {
    let delay = node.expected_execution_delay_handle();
    let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> = Default::default();
    let delayed_endorsements: Arc<
        RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
    > = Default::default();
    let peer_actor = data.get_mut(&node.peer_manager_sender.actor_handle());
    peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        let delay_height = delay.load(Ordering::Relaxed);
        match request {
            NetworkRequests::Block { ref block } => {
                block_heights.write().insert(*block.hash(), block.header().height());

                let mut delayed_endorsements = delayed_endorsements.write();
                loop {
                    let Some(front) = delayed_endorsements.front() else {
                        break;
                    };
                    let Some(&height) = block_heights.read().get(&front.0) else {
                        // Endorsed block not observed on this handler yet; wait
                        // until it arrives before deciding if the delay has
                        // elapsed.
                        break;
                    };
                    if height + delay_height >= block.header().height() {
                        break;
                    }
                    let (_, target, endorsement) = delayed_endorsements.pop_front().unwrap();
                    let Some(sender) = state.lock().senders.get(&target).cloned() else {
                        continue;
                    };
                    sender
                        .send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
                }
                HandlerResult::Unhandled(request)
            }
            NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                if delay_height == 0 {
                    return HandlerResult::Unhandled(NetworkRequests::SpiceChunkEndorsement(
                        target,
                        endorsement,
                    ));
                }
                delayed_endorsements.write().push_back((
                    *endorsement.block_hash(),
                    target,
                    endorsement,
                ));
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            _ => HandlerResult::Unhandled(request),
        }
    }));
}
