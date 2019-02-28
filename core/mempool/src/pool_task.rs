use std::sync::Arc;

use futures::future;
use futures::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender};
use log::warn;

use primitives::chain::{ChainPayload, PayloadRequest};
use primitives::hash::CryptoHash;
use primitives::types::AuthorityId;

use crate::Pool;

pub fn spawn_pool(
    pool: Arc<Pool>,
    retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
    payload_request_tx: Sender<PayloadRequest>,
    payload_response_rx: Receiver<ChainPayload>,
) {
    let pool1 = pool.clone();
    let task = retrieve_payload_rx.for_each(move |(authority_id, hash)| {
        if !pool1.contains_payload_snapshot(&hash) {
            tokio::spawn(
                payload_request_tx
                    .clone()
                    .send(PayloadRequest::BlockProposal(authority_id, hash))
                    .map(|_| ())
                    .map_err(|e| warn!(target: "pool", "Error sending message: {}", e))
            );
        }
        future::ok(())
    });
    tokio::spawn(task);

    let pool2 = pool.clone();
    let task = payload_response_rx.for_each(move |payload| {
        match pool2.add_payload(payload) {
            Err(e) => warn!(target: "pool", "Failed to add payload: {}", e),
            _ => {},
        };
        future::ok(())
    });
    tokio::spawn(task);
}