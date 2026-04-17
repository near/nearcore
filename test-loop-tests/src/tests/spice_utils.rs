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
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared control for an installed endorsement delay. Use [`Self::set`] to
/// change the delay in flight (including setting it to 0 to let queued
/// endorsements drain as new blocks arrive).
#[derive(Clone)]
pub(super) struct EndorsementDelay(Arc<AtomicU64>);

impl EndorsementDelay {
    fn new(delay_height: u64) -> Self {
        Self(Arc::new(AtomicU64::new(delay_height)))
    }

    pub(super) fn set(&self, delay_height: u64) {
        self.0.store(delay_height, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Install a handler that holds back endorsement propagation by `delay_height`
/// blocks. Use the returned [`EndorsementDelay`] to change the delay later.
pub(super) fn delay_endorsements_propagation(
    env: &mut TestLoopEnv,
    delay_height: u64,
) -> EndorsementDelay {
    for node in &env.node_datas {
        node.set_expected_execution_delay(delay_height);
    }

    let core_writer_senders: HashMap<_, _> = env
        .node_datas
        .iter()
        .map(|datas| (datas.account_id.clone(), datas.spice_core_writer_sender.clone()))
        .collect();

    let delay = EndorsementDelay::new(delay_height);
    for node in &env.node_datas {
        let senders = core_writer_senders.clone();
        let delay = delay.clone();
        let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let delayed_endorsements: Arc<
            RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
        > = Arc::new(RwLock::new(VecDeque::new()));
        let peer_actor = env.test_loop.data.get_mut(&node.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
            let delay_height = delay.load();
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
                        let (_, target, endorsement) = delayed_endorsements.pop_front().unwrap();
                        senders[&target].send(SpiceChunkEndorsementMessage(endorsement));
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
    delay
}
