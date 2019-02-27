#![allow(dead_code)]
use super::{Control, State, TxFlowTask};

use chrono::Utc;
use futures::future;
use futures::future::{join_all, lazy};
use futures::sync::mpsc;
use futures::{Future, Sink, Stream};
use rand;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::timer::Delay;

use primitives::consensus::{Payload, WitnessSelector};
use primitives::types::GossipBody;

/// Fake witness selector that does not rotate the witnesses.
struct FakeWitnessSelector {
    owner_uid: u64,
    num_witnesses: u64,
    all_witnesses: HashSet<u64>,
}

impl FakeWitnessSelector {
    pub fn new(owner_uid: u64, num_witnesses: u64) -> Self {
        Self { owner_uid, num_witnesses, all_witnesses: (0..num_witnesses).collect() }
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Default)]
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
pub fn spawn_all(num_witnesses: u64) {
    let starting_epoch = 0;
    let gossip_size =
        (num_witnesses as f64).sqrt().max(1.0 as f64).min(num_witnesses as f64 - 1.0) as usize;

    tokio::run(lazy(move || {
        let mut inc_gossip_tx_vec = vec![];
        let mut inc_payload_tx_vec = vec![];
        let mut out_gossip_rx_vec = vec![];

        // Spawn tasks
        for owner_uid in 0..num_witnesses {
            let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
            let (inc_payload_tx, inc_payload_rx) = mpsc::channel(1024);
            let (out_gossip_tx, _out_gossip_rx) = mpsc::channel(1024);
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (consensus_tx, _consensus_rx) = mpsc::channel(1024);
            let witness_selector = Box::new(FakeWitnessSelector::new(owner_uid, num_witnesses));

            inc_gossip_tx_vec.push(inc_gossip_tx);
            inc_payload_tx_vec.push(inc_payload_tx);
            out_gossip_rx_vec.push(_out_gossip_rx);

            let task = TxFlowTask::<FakePayload, FakeWitnessSelector>::new(
                inc_gossip_rx,
                inc_payload_rx,
                out_gossip_tx,
                control_rx,
                consensus_tx,
            );
            tokio::spawn(task.for_each(|_| Ok(())));

            let control_tx1 = control_tx.clone();
            let start_task = control_tx1
                .send(Control::Reset(State {
                    owner_uid,
                    starting_epoch,
                    gossip_size,
                    witness_selector,
                    beacon_block_index: 0,
                }))
                .map(|_| ())
                .map_err(|e| println!("Error sending control {}", e));
            tokio::spawn(start_task);

            // Sends a stop signal to the clients, but most importantly it holds the input of the
            // control channel, because as soon as the control channel is dropped TxFlow task stops.
            let control_tx2 = control_tx.clone();
            let stop_task = Delay::new(Instant::now() + Duration::from_secs(10)).then(|_| {
                control_tx2
                    .send(Control::Stop)
                    .map(|_| ())
                    .map_err(|e| println!("Error sending control {}", e))
            });
            tokio::spawn(stop_task);
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
                tokio::spawn(
                    gossip_input
                        .send(gossip)
                        .map(|_| ())
                        .map_err(|e| println!("Error relaying gossip {:?}", e)),
                );
                epoch
            });
            if tracker_added {
                tokio::spawn(f.for_each(|_| Ok(())));
            } else {
                tokio::spawn(
                    f.fold((0, 0), |(min_epoch, max_epoch), _epoch| {
                        if let Some(epoch) = _epoch {
                            let mut max_epoch = max_epoch;
                            let mut min_epoch = min_epoch;
                            if epoch > max_epoch {
                                max_epoch = epoch;
                                println!(
                                    "{} [{:?}, {:?}]",
                                    Utc::now().format("%H:%M:%S"),
                                    min_epoch,
                                    max_epoch
                                );
                            }
                            if epoch < min_epoch {
                                min_epoch = epoch;
                            }
                            future::ok((min_epoch, max_epoch))
                        } else {
                            future::ok((min_epoch, max_epoch))
                        }
                    })
                    .map(|_| ()),
                );
            }
            tracker_added = true;
        }

        // Send kick-off payloads.
        let mut fs = vec![];
        for c in &inc_payload_tx_vec {
            let mut payload = FakePayload::new();
            payload.set_content(1);
            fs.push(
                c.clone()
                    .send(payload)
                    .map(|_| println!("Sending payload"))
                    .map_err(|e| println!("Payload sending error {:?}", e)),
            );
        }

        tokio::spawn(
            join_all(fs).map(|_| ()).map_err(|e| println!("Payloads sending error {:?}", e)),
        );
        Ok(())
    }));
}

#[cfg(test)]
mod tests {
    use super::spawn_all;
    #[test]
    fn two_witnesses() {
        spawn_all(10);
    }
}
