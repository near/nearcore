use std::sync::Arc;
use std::time::Duration;

use futures::future;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender};
use futures::Future;
use log::{error, warn};
use tokio::{self, timer::Interval};

use primitives::chain::{ChainPayload, PayloadRequest};
use primitives::hash::CryptoHash;
use primitives::types::AuthorityId;

use nightshade::nightshade_task::Control;

use crate::Pool;

#[derive(Clone, Debug)]
pub struct MemPoolControl {
    pub authority_id: AuthorityId,
    pub num_authorities: usize,
    pub control: Control,
}

pub fn spawn_pool(
    pool: Arc<Pool>,
    mempool_control_rx: Receiver<MemPoolControl>,
    control_tx: Sender<Control>,
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
                    .map_err(|e| warn!(target: "pool", "Error sending message: {}", e)),
            );
        }
        future::ok(())
    });
    tokio::spawn(task);

    let pool2 = pool.clone();
    let task = Interval::new_interval(payload_announce_period)
        .for_each(move |_| {
            if let Some((authority_id, payload)) = pool2.prepare_payload_announce() {
                tokio::spawn(
                    payload_announce_tx
                        .clone()
                        .send((authority_id, payload))
                        .map(|_| ())
                        .map_err(|e| warn!(target: "pool", "Error sending message: {}", e)),
                );
            }
            future::ok(())
        })
        .map_err(|e| error!("Timer error: {}", e));
    tokio::spawn(task);

    let pool3 = pool.clone();
    let task = payload_response_rx.for_each(move |payload| {
        if let Err(e) = pool3.add_payload(payload) {
            warn!(target: "pool", "Failed to add payload: {}", e);
        }
        future::ok(())
    });
    tokio::spawn(task);

    let pool4 = pool.clone();
    let task = mempool_control_rx.for_each(move |control| {
        pool4.reset(control.clone());
        tokio::spawn(
            control_tx
                .clone()
                .send(control.control)
                .map(|_| ())
                .map_err(|e| error!("Failed to send NS control: {}", e)),
        );
        future::ok(())
    });
    tokio::spawn(task);
}
