use std::time::{Duration, Instant};

use futures::{Future, Sink, Stream};
use futures::future::{join_all, lazy};
use futures::sync::mpsc;
use log::error;
use tokio::timer::Delay;

use primitives::signature::get_key_pair;

use crate::nightshade::BlockHeader;

use super::nightshade_task::{Control, NightshadeTask};

#[derive(Clone, Hash, Debug, Serialize)]
struct DummyPayload {
    dummy: u64,
}

fn spawn_all(num_authorities: usize) {
    let fake_network = lazy(move || {
        let mut control_tx_vec = vec![];
        let mut inc_gossips_tx_vec = vec![];
        let mut out_gossips_rx_vec = vec![];
        let mut consensus_rx_vec = vec![];

        let (public_keys, secret_keys): (Vec<_>, Vec<_>) = (0..num_authorities).map(|_| get_key_pair()).unzip();

        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossips_tx, inc_gossips_rx) = mpsc::channel(1024);
            let (out_gossips_tx, out_gossips_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);

            control_tx_vec.push(control_tx.clone());
            inc_gossips_tx_vec.push(inc_gossips_tx);
            out_gossips_rx_vec.push(out_gossips_rx);
            consensus_rx_vec.push(consensus_rx);

            let payload = DummyPayload { dummy: owner_id as u64 };

            let task: NightshadeTask<DummyPayload> = NightshadeTask::new(
                owner_id,
                num_authorities,
                payload,
                public_keys.clone(),
                secret_keys[owner_id].clone(),
                inc_gossips_rx,
                out_gossips_tx,
                control_rx,
                consensus_tx,
            );

            tokio::spawn(task.for_each(|_| Ok(())));

            // Start the task using control channels, and stop it after 1 second
            let start_task = control_tx.clone().send(Control::Reset)
                .map(|_| ()).map_err(|e| error!("Error sending control {:?}", e));
            tokio::spawn(start_task);

            let control_tx1 = control_tx.clone();

            let stop_task = Delay::new(Instant::now() + Duration::from_secs(1)).then(|_| {
                control_tx1
                    .send(Control::Stop)
                    .map(|_| ())
                    .map_err(|e| error!("Error sending control {:?}", e))
            });
            tokio::spawn(stop_task);
        }

        // Traffic management
        for out_gossip_rx in out_gossips_rx_vec.drain(..) {
            let inc_gossip_tx_vec1 = inc_gossips_tx_vec.clone();
            let fut = out_gossip_rx.map(move |message| {
                let gossip_input = inc_gossip_tx_vec1[message.receiver_id].clone();
                tokio::spawn(
                    gossip_input.send(message).
                        map(|_| ()).map_err(|e| error!("Error relaying message {:?}", e)));
            });

            tokio::spawn(fut.for_each(|_| Ok(())));
        }

        let futures: Vec<_> = consensus_rx_vec.into_iter().map(|rx| rx.into_future()).collect();

        tokio::spawn(join_all(futures)
            .map(|v: Vec<(Option<BlockHeader>, _)>| {
                // Check every authority committed to the same outcome
                if !v.iter().all(|(outcome, _)| {
                    Some(outcome.clone().expect("Authority not committed")) == v[0].0
                }) {
                    panic!("Authorities committed to different outcomes.");
                }
            })
            .map_err(|e| {
                error!("Failed achieving consensus: {:?}", e)
            }));

        let result: Result<(), ()> = Ok(());
        result
    });

    tokio::run(fake_network);
}

#[cfg(test)]
mod tests {
    use super::spawn_all;

    #[test]
    fn one_authority() {
        spawn_all(1);
    }

    #[test]
    fn five_authorities() {
        spawn_all(5);
    }
}