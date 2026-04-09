use crate::setup::env::TestLoopEnv;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerMessage;
use near_network::{T1MessageBody, TieredMessageBody};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::types::BlockHeight;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub(super) fn delay_endorsements_propagation(env: &TestLoopEnv, delay_height: u64) {
    for node in &env.node_datas {
        node.set_expected_execution_delay(delay_height);
    }

    // Use the shared node_network_states to look up senders dynamically.
    // This handles node restart correctly — the restarted node's NetworkState
    // is re-registered in the shared map, so we always get the current sender.
    let node_states = env.shared_state.node_network_states.clone();

    let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let delayed_endorsements: Arc<RwLock<VecDeque<(CryptoHash, PeerId, SpiceChunkEndorsement)>>> =
        Arc::new(RwLock::new(VecDeque::new()));

    env.shared_state.network_shared_state.register_message_filter(
        move |_from, to, msg| match msg {
            PeerMessage::Block(block) => {
                block_heights.write().insert(*block.hash(), block.header().height());

                let mut delayed = delayed_endorsements.write();
                loop {
                    let Some(front) = delayed.front() else {
                        break;
                    };
                    let height = block_heights.read()[&front.0];
                    if height + delay_height >= block.header().height() {
                        break;
                    }
                    let (_, target, endorsement) = delayed.pop_front().unwrap();
                    // Look up the current sender dynamically (handles restart).
                    if let Some(state) = node_states.lock().get(&target) {
                        state
                            .spice_core_writer_adapter
                            .send(SpiceChunkEndorsementMessage(endorsement));
                    }
                }
                Some(msg.clone())
            }
            PeerMessage::Routed(routed_msg) => {
                if let TieredMessageBody::T1(body) = routed_msg.body() {
                    if let T1MessageBody::SpiceChunkEndorsement(endorsement) = body.as_ref() {
                        delayed_endorsements.write().push_back((
                            *endorsement.block_hash(),
                            to.clone(),
                            endorsement.clone(),
                        ));
                        return None;
                    }
                }
                Some(msg.clone())
            }
            _ => Some(msg.clone()),
        },
    );
}
