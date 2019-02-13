use std::time::{Duration, Instant};

use futures::{Future, Sink, Stream};
use futures::future::lazy;
use futures::sync::mpsc;
use log::error;
use tokio::timer::Delay;

use super::nightshade_task::{Control, NightshadeTask};

fn spawn_all(num_authorities: usize) {
    tokio::run(lazy(move || {
        let mut control_tx_vec = vec![];
        let mut inc_gossips_tx_vec = vec![];
        let mut out_gossips_rx_vec = vec![];

        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossips_tx, inc_gossips_rx) = mpsc::channel(1024);
            let (out_gossips_tx, out_gossips_rx) = mpsc::channel(1024);

            control_tx_vec.push(control_tx.clone());
            inc_gossips_tx_vec.push(inc_gossips_tx);
            out_gossips_rx_vec.push(out_gossips_rx);

            let task = NightshadeTask::new(
                owner_id,
                num_authorities,
                inc_gossips_rx,
                out_gossips_tx,
                control_rx,
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

        // TODO: Check consensus is reached

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