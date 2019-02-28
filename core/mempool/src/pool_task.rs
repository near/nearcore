use std::sync::Arc;
use std::time::Duration;

use futures::future;
use futures::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender};
use log::{warn, error};
use tokio::{self, timer::Interval};

use primitives::chain::{ChainPayload, PayloadRequest};
use primitives::hash::CryptoHash;
use primitives::types::AuthorityId;

use crate::Pool;

pub struct MemPoolControl {
    pub authority_id: AuthorityId,
    pub num_authorities: usize,
}

pub fn spawn_pool(
    pool: Arc<Pool>,
    mempool_contorl_rx: Receiver<MemPoolControl>,
    retrieve_payload_rx: Receiver<(AuthorityId, CryptoHash)>,
    payload_announce_tx: Sender<(AuthorityId, ChainPayload)>,
    payload_request_tx: Sender<PayloadRequest>,
    payload_response_rx: Receiver<ChainPayload>,
    payload_announce_period: Duration,
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
    let task = Interval::new_interval(payload_announce_period)
        .for_each(move |_| {
            let authority_payload = pool2.prepare_payload_announce();
            match authority_payload {
                Some((authority_id, payload)) => {
                    tokio::spawn(
                        payload_announce_tx
                            .clone()
                            .send((authority_id, payload))
                            .map(|_| ())
                            .map_err(|e| warn!(target: "pool", "Error sending message: {}", e))
                    );
                },
                _ => {},
            }
            future::ok(())
        })
        .map_err(|e| error!("timer error: {}", e));
    tokio::spawn(task);

    let pool3 = pool.clone();
    let task = payload_response_rx.for_each(move |payload| {
        match pool3.add_payload(payload) {
            Err(e) => warn!(target: "pool", "Failed to add payload: {}", e),
            _ => {},
        };
        future::ok(())
    });
    tokio::spawn(task);

    let pool4 = pool.clone();
    let task = mempool_contorl_rx.for_each(move |control| {
        pool4.reset(control);
        future::ok(())
    });
    tokio::spawn(task);
}