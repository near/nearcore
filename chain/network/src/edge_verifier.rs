use std::cmp::max;

use actix::{Actor, Handler, SyncContext, System};

use near_performance_metrics_macros::perf;

use crate::types::{EdgeList, StopMsg};

pub(crate) struct EdgeVerifier {}

impl Actor for EdgeVerifier {
    type Context = SyncContext<Self>;
}

impl Handler<StopMsg> for EdgeVerifier {
    type Result = ();
    fn handle(&mut self, _: StopMsg, _ctx: &mut Self::Context) -> Self::Result {
        System::current().stop();
    }
}

impl Handler<EdgeList> for EdgeVerifier {
    type Result = bool;

    #[perf]
    fn handle(&mut self, msg: EdgeList, _ctx: &mut Self::Context) -> Self::Result {
        for edge in msg.edges {
            let key = (edge.peer0.clone(), edge.peer1.clone());
            if msg.edges_info_shared.lock().unwrap().get(&key).cloned().unwrap_or(0u64)
                >= edge.nonce
            {
                continue;
            }

            #[cfg(feature = "test_features")]
            if !msg.adv_disable_edge_signature_verification && !edge.verify() {
                return false;
            }

            #[cfg(not(feature = "test_features"))]
            if !edge.verify() {
                return false;
            }
            {
                let mut guard = msg.edges_info_shared.lock().unwrap();
                let entry = guard.entry(key);

                let cur_nonce = entry.or_insert(edge.nonce);
                *cur_nonce = max(*cur_nonce, edge.nonce);
            }
            msg.sender.push(edge);
        }
        true
    }
}
