use crate::network_protocol::Edge;
use crate::private_actix::{StopMsg, ValidateEdgeList};
use actix::{Actor, ActorContext, Handler, SyncContext};
use near_o11y::{handler_span, handler_trace_span, OpenTelemetrySpanExt, WithSpanContext};
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

impl Handler<WithSpanContext<StopMsg>> for EdgeValidatorActor {
    type Result = ();
    fn handle(&mut self, msg: WithSpanContext<StopMsg>, ctx: &mut Self::Context) -> Self::Result {
        let (_span, _msg) = handler_span!("network", msg);
        ctx.stop();
    }
}

impl Handler<WithSpanContext<ValidateEdgeList>> for EdgeValidatorActor {
    type Result = bool;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ValidateEdgeList>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_trace_span!("network", msg);
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
            // Ignore sending error, which can happen only if the channel is closed.
            let _ = msg.sender.send(edge);
        }
        true
    }
}

pub struct EdgeValidatorHelper {
    /// Shared version of `edges_info` used by multiple threads.
    pub edges_info_shared: Arc<Mutex<HashMap<(PeerId, PeerId), u64>>>,
    /// Queue of edges verified, but not added yet.
    pub edges_to_add_receiver: crossbeam_channel::Receiver<Edge>,
    pub edges_to_add_sender: crossbeam_channel::Sender<Edge>,
}

impl Default for EdgeValidatorHelper {
    fn default() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self {
            edges_info_shared: Default::default(),
            edges_to_add_sender: tx,
            edges_to_add_receiver: rx,
        }
    }
}
