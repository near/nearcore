use tokio::timer::Delay;
use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use std::time::{Duration, Instant};
use log::error;
use primitives::traits::Payload;
use super::nightshade_task::{NightshadeTask, Control, Gossip};

/// Fake payload that only stores one number.
#[derive(Debug, Serialize, Deserialize, Hash, Clone)]
pub struct FakePayload {
    pub content: u64,
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

fn spawn_all(num_authorities: usize) {
    tokio::run(lazy(move || {
        let mut inc_gossip_tx_vec = vec![];
        let mut out_gossip_rx_vec = vec![];
        let mut inc_payload_tx_vec = vec![];
        let mut consensus_rx_vec = vec![];
        let mut control_tx_vec = vec![];
        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
            let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
            let (inc_payload_tx, inc_payload_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);

            control_tx_vec.push(control_tx.clone());
            inc_gossip_tx_vec.push(inc_gossip_tx);
            out_gossip_rx_vec.push(out_gossip_rx);
            inc_payload_tx_vec.push(inc_payload_tx);
            consensus_rx_vec.push(consensus_rx);

            let task = NightshadeTask::<FakePayload>::new(
                owner_id,
                num_authorities,
                control_rx,
                inc_gossip_rx,
                out_gossip_tx,
                inc_payload_rx,
                consensus_tx
            );
            tokio::spawn(task.for_each(|_| Ok(())));

            let start_task = control_tx.clone()
                .send(Control::Reset)
                .map(|_| ()).map_err(|e| error!("Error sending control {}", e));
            tokio::spawn(start_task);
            let control_tx2 = control_tx.clone();
            let stop_task = Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| {
                control_tx2
                    .send(Control::Stop)
                    .map(|_| ()).map_err(|e| error!("Error sending control {}", e))
            });
            tokio::spawn(stop_task);
        }

        for out_message_rx in out_gossip_rx_vec.drain(..) {
            // Spawn the network itself.
            let inc_message_tx_vec_1 = inc_gossip_tx_vec.clone();
            let f = out_message_rx.map(move |gossip: Gossip<FakePayload>| {
                // println!("Sending gossip: {:?}", gossip);
                let gossip_input = inc_message_tx_vec_1[gossip.receiver_id as usize].clone();
                tokio::spawn(gossip_input
                    .send(gossip)
                    .map(|_| ())
                    .map_err(|e| error!("Error relaying gossip {:?}", e)));
            });
            tokio::spawn(f.for_each(|_| Ok(())));
        }

//        for (i, consensus_rx) in consensus_rx_vec.drain(..).enumerate() {
//            let control_tx_vec_1 = control_tx_vec.clone();
//            let f = consensus_rx.map(move |consensus| {
//                let control_input = control_tx_vec_1[i].clone();
//                tokio::spawn(control_input
//                    .send(Control::Stop)
//                    .map(|_| ())
//                    .map_err(|e| error!("Error stopping Nightshade {:?}", e)));
//            });
//            tokio::spawn(f.for_each(|_| Ok(())));
//        }

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
            join_all(fs).map(|_| ()).map_err(|e| error!("Payload sending error {:?}", e))
        );

        Ok(())
    }));
}

#[cfg(test)]
mod tests {
    use super::spawn_all;

    #[test]
    fn one_authority() {
        spawn_all(1);
    }

    #[test]
    fn ten_authority() {
        spawn_all(5);
    }
}