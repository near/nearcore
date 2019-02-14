use std::time::{Duration, Instant};

use futures::{Future, Sink, Stream};
use futures::future::{join_all, lazy};
use futures::sync::mpsc;
use log::error;
use tokio::timer::Delay;

use super::nightshade_task::{Control, NightshadeTask};

#[derive(Clone, Hash, Debug)]
struct DummyPayload {
    dummy: u64,
}

fn spawn_all(num_authorities: usize) {
    tokio::run(lazy(move || {
        let mut control_tx_vec = vec![];
        let mut inc_gossips_tx_vec = vec![];
        let mut out_gossips_rx_vec = vec![];
        let mut consensus_rx_vec = vec![];

        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossips_tx, inc_gossips_rx) = mpsc::channel(1024);
            let (out_gossips_tx, out_gossips_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);

            control_tx_vec.push(control_tx.clone());
            inc_gossips_tx_vec.push(inc_gossips_tx);
            out_gossips_rx_vec.push(out_gossips_rx);
            consensus_rx_vec.push(consensus_rx);

            let payload = DummyPayload {dummy : 0};

            let task: NightshadeTask<DummyPayload> = NightshadeTask::new(
                owner_id,
                num_authorities,
                payload,
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

        let mut futures = vec![];

        for consensus_rx in consensus_rx_vec.drain(..) {
            futures.push(consensus_rx.into_future());
        }

        tokio::spawn(join_all(futures)
            .map(|v| {
                let mut general_outcome = None;

                for (outcome, _) in v.iter() {
                    let outcome = outcome.clone().expect("Authority not committed");

                    if let Some(cur_outcome) = general_outcome.clone() {
                        if outcome != cur_outcome {
                            panic!("Authorities have committed to different outcomes");
                        }
                    } else {
                        general_outcome = Some(outcome);
                    }
                }

                ()
            })
            .map_err(|e| error!("Failed achieving consensus: {:?}", e)));

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
    fn five_authorities() {
        spawn_all(5);
    }
}