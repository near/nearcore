use crate::private_actix::{StopMsg, ValidateEdgeList};
use actix::{Actor, ActorContext, Handler, SyncContext};
use conqueue::{QueueReceiver, QueueSender};
use near_network::types::Edge;
use near_performance_metrics_macros::perf;
use near_primitives::borsh::maybestd::collections::HashMap;
use near_primitives::borsh::maybestd::sync::{Arc, Mutex};
use near_primitives::network::PeerId;
use std::cmp::max;

/// `EdgeListToValidate` contains list of `Edge`, and it's associated with a connected peer.
/// Checks signatures of all edges in `EdgeListToValidate` and if any signature is not valid,
/// we will ban the peer, who sent us incorrect edges.
///
/// TODO(#5230): This code needs to be rewritten to fix memory leak - there is a cache that stores
///              all edges `edges_info_shared` forever in memory.
pub(crate) struct EdgeValidatorActor {}

impl Actor for EdgeValidatorActor {
    type Context = SyncContext<Self>;
}

impl Handler<StopMsg> for EdgeValidatorActor {
    type Result = ();
    fn handle(&mut self, _: StopMsg, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl Handler<ValidateEdgeList> for EdgeValidatorActor {
    type Result = bool;

    #[perf]
    fn handle(&mut self, msg: ValidateEdgeList, _ctx: &mut Self::Context) -> Self::Result {
        for edge in msg.edges {
            let key = edge.key();
            if msg.edges_info_shared.lock().unwrap().get(key).cloned().unwrap_or(0u64)
                >= edge.nonce()
            {
                continue;
            }

            if !edge.verify() {
                return false;
            }
            {
                let mut guard = msg.edges_info_shared.lock().unwrap();
                let entry = guard.entry(key.clone());

                let cur_nonce = entry.or_insert_with(|| edge.nonce());
                *cur_nonce = max(*cur_nonce, edge.nonce());
            }
            msg.sender.push(edge);
        }
        true
    }
}

pub struct EdgeValidatorHelper {
    /// Shared version of `edges_info` used by multiple threads.
    pub edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    /// Queue of edges verified, but not added yes.
    pub edges_to_add_receiver: QueueReceiver<Edge>,
    pub edges_to_add_sender: QueueSender<Edge>,
}

impl Default for EdgeValidatorHelper {
    fn default() -> Self {
        let (tx, rx) = conqueue::Queue::unbounded::<Edge>();
        Self {
            edges_info_shared: Default::default(),
            edges_to_add_sender: tx,
            edges_to_add_receiver: rx,
        }
    }
}
