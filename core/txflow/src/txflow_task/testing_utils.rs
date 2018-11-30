use super::TxFlowTask;

use futures::{Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use futures::future;
use futures::sync::mpsc;
use rand;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use chrono::{DateTime, Utc};

use primitives::traits::{Payload, WitnessSelector};
use primitives::types::{Gossip, GossipBody, UID};

/// Fake witness selector that does not rotate the witnesses.
struct FakeWitnessSelector {
    owner_uid: u64,
    num_witnesses: u64,
    all_witnesses: HashSet<u64>,
}

impl FakeWitnessSelector {
    pub fn new(owner_uid: u64, num_witnesses: u64) -> Self {
        Self {
            owner_uid,
            num_witnesses,
            all_witnesses: (0..num_witnesses).collect(),
        }
    }
}

impl WitnessSelector for FakeWitnessSelector {
    fn epoch_witnesses(&self, _epoch: u64) -> &HashSet<u64> {
        &self.all_witnesses
    }

    fn epoch_leader(&self, epoch: u64) -> u64 {
        epoch % self.num_witnesses
    }

    fn random_witnesses(&self, _epoch: u64, sample_size: usize) -> HashSet<u64> {
        assert!(sample_size as u64 <= self.num_witnesses - 1);
        let mut res = HashSet::new();
        while res.len() < sample_size {
            let next = rand::random::<u64>() % self.num_witnesses;
            if next != self.owner_uid {
                res.insert(next);
            }
        }
        res
    }
}

/// Fake payload that only stores one number.
#[derive(Debug, Hash, Clone)]
struct FakePayload {
    content: u64,
}

impl FakePayload {
    pub fn set_content(&mut self, content: u64) {
        self.content = content;
    }
}

impl Payload for FakePayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn union_update(&mut self, other: Self) {
        self.content += other.content;
    }

    fn is_empty(&self) -> bool {
        self.content == 0
    }

    fn new() -> Self {
        Self { content: 0 }
    }
}

/// Spawns several TxFlowTasks and mediates their communication channels.
struct FakeWitnessNetwork {
    /// Channels used to send gossips to the TxFlow tasks.
    input_gossip_channels: Vec<mpsc::Sender<Gossip<FakePayload>>>,
    /// Channel used to send payload to the TxFlow task.
    output_gossip_channel: mpsc::Receiver<Gossip<FakePayload>>,
    track: bool,
    recent_epochs: HashMap<UID, u64>,
    prev_max_epoch: u64,
    prev_min_epoch: u64,
    prev_avg_epoch: u64,
}

impl FakeWitnessNetwork {
    pub fn spawn_all(num_witnesses: u64) {
        let starting_epoch = 0;
        let sample_size = (num_witnesses as f64)
            .sqrt()
            .max(1.0 as f64)
            .min(num_witnesses as f64 - 1.0) as usize;

        tokio::run(lazy(move || {
            let mut inc_gossip_tx_vec = vec![];
            let mut inc_payload_tx_vec = vec![];
            let mut out_gossip_rx_vec = vec![];

            // Spawn tasks
            for owner_uid in 0..num_witnesses {
                let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1_024);
                let (inc_payload_tx, inc_payload_rx) = mpsc::channel(1_024);
                let (out_gossip_tx, _out_gossip_rx) = mpsc::channel(1_024);
                let selector = FakeWitnessSelector::new(owner_uid, num_witnesses);

                inc_gossip_tx_vec.push(inc_gossip_tx);
                inc_payload_tx_vec.push(inc_payload_tx);
                out_gossip_rx_vec.push(_out_gossip_rx);

                let task = TxFlowTask::<FakePayload, _>::new(
                    owner_uid, starting_epoch, sample_size,
                    inc_gossip_rx, inc_payload_rx, out_gossip_tx,
                    selector);
                tokio::spawn(task.for_each(|_| Ok(())));
            }

            let mut tracker_added = false;
            for out_gossip_rx in out_gossip_rx_vec.drain(..) {
                // Spawn the network itself.
                let inc_gossip_tx_vec_cloned = inc_gossip_tx_vec.clone();
                let f = out_gossip_rx.map(move |gossip| {
                    let receiver_uid = gossip.receiver_uid;
                    let epoch = if let GossipBody::Unsolicited(message) = &gossip.body {
                        Some(message.body.epoch)
                    } else {
                        None
                    };
                    let gossip_input = inc_gossip_tx_vec_cloned[receiver_uid as usize].clone();
                    tokio::spawn(gossip_input.send(gossip)
                        .map(|_| ())
                        .map_err(|e| println!("Error relaying gossip {:?}", e)));
                    epoch
                });
                if tracker_added {
                    tokio::spawn(f.for_each(|_| Ok(())));
                } else {
                    tokio::spawn(f.fold((0, 0), | (min_epoch, max_epoch), _epoch | {
                        if let Some(epoch) = _epoch {
                            let mut max_epoch = max_epoch;
                            let mut min_epoch = min_epoch;
                            if epoch > max_epoch {
                                max_epoch = epoch;
                                println!("{} [{:?}, {:?}]",
                                         Utc::now().format("%H:%M:%S"),
                                         min_epoch, max_epoch);
                            }
                            if epoch < min_epoch {
                                min_epoch = epoch;
                            }
                            future::ok((min_epoch, max_epoch))
                        } else {
                            future::ok((min_epoch, max_epoch))
                        }
                    }).map(|_| ())
                    );
                }
                tracker_added = true;
            }

            // Send kick-off payloads.
            let mut fs = vec![];
            for c in &inc_payload_tx_vec {
                let mut payload = FakePayload::new();
                payload.set_content(1);
                fs.push(c.clone().send(payload)
                    .map(|_| println!("Sending payload"))
                    .map_err(|e| println!("Payload sending error {:?}", e))
                );
            }

            tokio::spawn(join_all(fs).map(|_|()).map_err(|e| println!("Payloads sending error {:?}", e)));
            Ok(())
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::FakeWitnessNetwork;
    #[test]
    fn two_witnesses() {
        FakeWitnessNetwork::spawn_all(5);
    }
}
