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
    // Handle request from NightshadeTask for confirmation on a payload.
    // If the payload can't be built from the mempool task to fetch necessary data is spawned and the
    // request is stored until it is ready.
    let pool1 = pool.clone();
    let control_tx1 = control_tx.clone();
    let task = retrieve_payload_rx.for_each(move |(authority_id, hash)| {
        if !pool1.contains_payload_snapshot(&hash) {
            tokio::spawn(
                payload_request_tx
                    .clone()
                    .send(PayloadRequest::BlockProposal(authority_id, hash))
                    .map(|_| ())
                    .map_err(|e| warn!(target: "pool", "Error sending message: {}", e)),
            );

            pool1.add_pending(authority_id, hash);
        } else {
            let send_confirmation = control_tx1
                .clone()
                .send(Control::PayloadConfirmation(authority_id, hash))
                .map(|_| ())
                .map_err(|_| error!("Fail sending control signal to nightshade"));
            tokio::spawn(send_confirmation);
        }
        future::ok(())
    });
    tokio::spawn(task);

    // Make announcements of new payloads created from this node.
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

    // Receive payload and send confirmation signal of unblocked payloads.
    let pool3 = pool.clone();
    let control_tx2 = control_tx.clone();
    let task = payload_response_rx.for_each(move |payload| {
        if let Err(e) = pool3.add_payload(payload) {
            warn!(target: "pool", "Failed to add payload: {}", e);
        }

        for (authority_id, hash) in pool3.ready_snapshots() {
            let send_confirmation = control_tx2
                .clone()
                .send(Control::PayloadConfirmation(authority_id, hash))
                .map(|_| ())
                .map_err(|_| error!("Fail sending control signal to nightshade"));
            tokio::spawn(send_confirmation);
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
