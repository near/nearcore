use std::collections::HashSet;
use primitives::hash::CryptoHash;
use tokio::timer::Delay;
use futures::sync::mpsc;
use futures::{stream, Async, Future, Poll, Sink, Stream};
use futures::future::{join_all, lazy};
use std::time::{Duration, Instant};
use futures::try_ready;
use log::{info, debug, error};
use std::cmp::min;
use primitives::traits::Payload;
use super::nightshade_task::{NightshadeTask, Control, Gossip};


fn spawn_all(num_authorities: usize) {
    tokio::run(lazy(move || {
        let mut inc_gossip_tx_vec = vec![];
        let mut out_gossip_rx_vec = vec![];
        let mut consensus_rx_vec = vec![];
        let mut control_tx_vec = vec![];
        for owner_id in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossip_tx, inc_gossip_rx) = mpsc::channel(1024);
            let (out_gossip_tx, out_gossip_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);

            inc_gossip_tx_vec.push(inc_gossip_tx);
            out_gossip_rx_vec.push(out_gossip_rx);
            consensus_rx_vec.push(consensus_rx);
            control_tx_vec.push(control_tx.clone());

            let task = NightshadeTask::new(
                owner_id,
                num_authorities,
                control_rx,
                inc_gossip_rx,
                out_gossip_tx,
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
            let f = out_message_rx.map(move |gossip: Gossip| {
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