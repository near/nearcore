use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::setup::state::{NodeExecutionData, SpiceEndorsementDelayState};
use near_async::messaging::CanSend as _;
use near_async::test_loop::data::TestLoopData;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::types::NetworkRequests;
use near_network::types::NetworkResponses;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::adapter::StoreAdapter;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl TestLoopEnv {
    /// Set the endorsement propagation delay to `delay_height` blocks: an endorsement
    /// of a block at height `h` is held until the node has seen a block above
    /// `h + delay_height`, which it shows by broadcasting one or by approving one above
    /// it. Safe to call unconditionally; installs the network handler lazily on the
    /// first non-zero call, and re-running it after `add_node` / `restart_node`
    /// instruments any node that doesn't have the handler yet.
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

fn install_endorsement_delay_handler(
    data: &mut TestLoopData,
    node: &NodeExecutionData,
    state: Arc<Mutex<SpiceEndorsementDelayState>>,
) {
    let delay = node.expected_execution_delay_handle();
    let chain_store =
        data.get(&node.client_sender.actor_handle()).client.chain.chain_store.store().chain_store();
    let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> = Default::default();
    let delayed_endorsements: Arc<
        RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
    > = Default::default();
    // Releases the held endorsements of every block at least `delay_height + 1` below
    // `seen_height`, the latest height this node has shown it knows: a block it broadcast,
    // or the parent of a block it approved. A chunk-only validator broadcasts no block, so
    // its approvals are what move it along.
    let release = {
        let delay = delay.clone();
        let block_heights = block_heights.clone();
        let delayed_endorsements = delayed_endorsements.clone();
        let state = state.clone();
        move |seen_height: BlockHeight| {
            let delay_height = delay.load(Ordering::Relaxed);
            let mut delayed_endorsements = delayed_endorsements.write();
            loop {
                let Some(front) = delayed_endorsements.front() else {
                    break;
                };
                let height = block_heights.read().get(&front.0).copied().or_else(|| {
                    chain_store.get_block_header(&front.0).ok().map(|header| header.height())
                });
                let Some(height) = height else {
                    // Endorsed block not known here yet; wait until it is before deciding
                    // if the delay has elapsed.
                    break;
                };
                if height + delay_height >= seen_height {
                    break;
                }
                let (_, target, endorsement) = delayed_endorsements.pop_front().unwrap();
                let Some(sender) = state.lock().senders.get(&target).cloned() else {
                    continue;
                };
                sender.send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
            }
        }
    };
    node.register_override_handler(data, move |request| {
        let delay_height = delay.load(Ordering::Relaxed);
        match request {
            NetworkRequests::Block { ref block } => {
                block_heights.write().insert(*block.hash(), block.header().height());
                release(block.header().height());
                HandlerResult::Unhandled(request)
            }
            NetworkRequests::Approval { ref approval_message } => {
                release(approval_message.approval.target_height.saturating_sub(1));
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
                state.lock().delayed += 1;
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            _ => HandlerResult::Unhandled(request),
        }
    });
}
