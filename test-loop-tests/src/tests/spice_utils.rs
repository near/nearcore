use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::messaging::CanSend as _;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::NetworkRequests;
use near_network::types::NetworkResponses;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl TestLoopEnv {
    /// Set the endorsement propagation delay to `delay_height` blocks.
    ///
    /// The first call with a non-zero delay installs a per-node network
    /// handler that holds back endorsements until later blocks catch up.
    /// Subsequent calls just update the delay value (including setting it to 0
    /// to let queued endorsements drain as new blocks arrive); the handler
    /// picks up the new value on its next invocation. A call with
    /// `delay_height == 0` before the handler has been installed is a no-op,
    /// so conditional callsites can always call this unconditionally.
    pub fn delay_endorsements_propagation(&mut self, delay_height: u64) {
        let installed =
            self.shared_state.endorsement_delay_handlers_installed.load(Ordering::Relaxed);
        if !installed && delay_height == 0 {
            return;
        }
        for node in &self.node_datas {
            node.set_expected_execution_delay(delay_height);
        }
        if installed {
            return;
        }
        self.shared_state.endorsement_delay_handlers_installed.store(true, Ordering::Relaxed);

        let core_writer_senders: HashMap<_, _> = self
            .node_datas
            .iter()
            .map(|datas| (datas.account_id.clone(), datas.spice_core_writer_sender.clone()))
            .collect();

        for node in &self.node_datas {
            let senders = core_writer_senders.clone();
            let delay = node.expected_execution_delay_handle();
            let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> =
                Arc::new(RwLock::new(HashMap::new()));
            let delayed_endorsements: Arc<
                RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
            > = Arc::new(RwLock::new(VecDeque::new()));
            let peer_actor = self.test_loop.data.get_mut(&node.peer_manager_sender.actor_handle());
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
                            let height = block_heights.read()[&front.0];
                            if height + delay_height >= block.header().height() {
                                break;
                            }
                            let (_, target, endorsement) =
                                delayed_endorsements.pop_front().unwrap();
                            senders[&target].send(SpiceChunkEndorsementMessage(endorsement));
                        }
                        HandlerResult::Unhandled(request)
                    }
                    NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                        if delay_height == 0 {
                            return HandlerResult::Unhandled(
                                NetworkRequests::SpiceChunkEndorsement(target, endorsement),
                            );
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
    }
}
