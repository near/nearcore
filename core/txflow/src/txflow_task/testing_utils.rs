use super::TxFlowTask;

use futures::{Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use futures::sync::mpsc;
use rand;
use std::collections::HashSet;
use std::time::Duration;

use primitives::traits::{Payload, WitnessSelector};
use primitives::types::{Gossip, UID};

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
        assert!(sample_size as u64 >= self.num_witnesses - 1);
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
    /// Channels used to send payload to the TxFlow tasks.
    input_payload_channels: Vec<mpsc::Sender<FakePayload>>,
    output_gossip_channels: Vec<mpsc::Receiver<Gossip<FakePayload>>>,
    closed_outputs: HashSet<UID>,
}

impl FakeWitnessNetwork {
    /// The initial payload that kicks off the TxFlow gossips.
    fn kickoff_payloads(&mut self) -> impl Future<Item=(), Error=()> {
        let mut fs = vec![];
        for c in &self.input_payload_channels {
            let mut payload = FakePayload::new();
            payload.set_content(1);
            fs.push(c.clone().send(payload)
                .map(|_| println!("Sending payload"))
                .map_err(|e| println!("Payload sending error {:?}", e))
            );
        }

        join_all(fs).map(|_|()).map_err(|e| println!("Payloads sending error {:?}", e))
    }

    pub fn spawn_all(num_witnesses: u64) {
        let starting_epoch = 0;
        let sample_size = (num_witnesses as f64)
            .sqrt()
            .min(1.0 as f64)
            .max(num_witnesses as f64 - 1.0) as usize;

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

            let mut net = FakeWitnessNetwork {
                input_gossip_channels: inc_gossip_tx_vec,
                input_payload_channels: inc_payload_tx_vec,
                output_gossip_channels: out_gossip_rx_vec,
                closed_outputs: HashSet::new(),
            };

            // Spawn the kickoff messages.
            tokio::spawn(net.kickoff_payloads());

            // Spawn the network itself.
            tokio::spawn(net.for_each(|_| Ok(())));

            Ok(())
        }));
    }
}

impl Stream for FakeWitnessNetwork {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut new_closed_outputs = HashSet::new();
        for (ind, c) in (&mut self.output_gossip_channels).into_iter().enumerate() {
            match c.poll() {
               Ok(Async::Ready(None)) => {
                   new_closed_outputs.insert(ind as UID);

               },
                Ok(Async::NotReady) => {},
                Err(e) => {println!("Error receiving gossip output {:?}", e)}
                Ok(Async::Ready(Some(gossip))) => {
                    let receiver_uid = gossip.receiver_uid;
                    let gossip_input = self.input_gossip_channels[receiver_uid as usize].clone();
                    std::thread::sleep(Duration::from_millis(300));
                    println!("Relaying gossip {:?}", gossip);
                    tokio::spawn(gossip_input.send(gossip)
                        .map(|_| ())
                        .map_err(|e| println!("Error relaying gossip {:?}", e)));
                    return Ok(Async::Ready(Some(())));
                }
            }
        }

        self.closed_outputs.extend(new_closed_outputs.drain());
        if self.closed_outputs.len() == self.output_gossip_channels.len() {
            return Ok(Async::Ready(None));
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::FakeWitnessNetwork;
    #[test]
    fn two_witnesses() {
        FakeWitnessNetwork::spawn_all(2);
    }
}
