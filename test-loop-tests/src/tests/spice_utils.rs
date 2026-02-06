use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use near_async::messaging::{CanSend as _, Handler as _};
use near_client::{Query, ViewClientActor};
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::NetworkRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::types::{AccountId, BlockHeight, BlockReference};
use near_primitives::views::{AccountView, QueryRequest, QueryResponseKind};
use parking_lot::RwLock;

use crate::setup::env::TestLoopEnv;

pub(super) fn delay_endorsements_propagation(env: &mut TestLoopEnv, delay_height: u64) {
    let core_writer_senders: HashMap<_, _> = env
        .node_datas
        .iter()
        .map(|datas| (datas.account_id.clone(), datas.spice_core_writer_sender.clone()))
        .collect();

    for node in &env.node_datas {
        let senders = core_writer_senders.clone();
        let block_heights: Arc<RwLock<HashMap<CryptoHash, BlockHeight>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let delayed_endorsements: Arc<
            RwLock<VecDeque<(CryptoHash, AccountId, SpiceChunkEndorsement)>>,
        > = Arc::new(RwLock::new(VecDeque::new()));
        let peer_actor = env.test_loop.data.get_mut(&node.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
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
                    Some(request)
                }
                NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                    delayed_endorsements.write().push_back((
                        *endorsement.block_hash(),
                        target,
                        endorsement,
                    ));
                    None
                }
                _ => Some(request),
            }
        }));
    }
}

pub(super) fn query_view_account(
    view_client: &mut ViewClientActor,
    account_id: AccountId,
) -> AccountView {
    let query_response = view_client
        .handle(Query::new(
            BlockReference::Finality(near_primitives::types::Finality::None),
            QueryRequest::ViewAccount { account_id },
        ))
        .unwrap();
    let QueryResponseKind::ViewAccount(view_account_result) = query_response.kind else {
        panic!();
    };
    view_account_result
}
