use std::time::{Duration, Instant};

use futures::{Future, Sink, Stream, future};
use futures::future::{join_all, lazy};
use futures::sync::mpsc;
use log::error;
use tokio::timer::Delay;

use primitives::aggregate_signature::BlsPublicKey;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::hash::CryptoHash;
use primitives::signature::get_key_pair;

use crate::nightshade::ConsensusBlockProposal;

use super::nightshade_task::{Control, NightshadeTask};

const TASK_DURATION_SEC: u64 = 300;

#[derive(Clone, Debug, Serialize)]
struct DummyPayload {
    dummy: u64,
}

fn get_bls_key_pair() -> (BlsPublicKey, BlsSecretKey) {
    let secret_key = BlsSecretKey::generate();
    let public_key = secret_key.get_public_key();
    (public_key, secret_key)
}

fn spawn_all(num_authorities: usize) {
    let fake_network = lazy(move || {
        let mut control_tx_vec = vec![];
        let mut inc_gossips_tx_vec = vec![];
        let mut out_gossips_rx_vec = vec![];
        let mut consensus_rx_vec = vec![];

        let (public_keys, secret_keys): (Vec<_>, Vec<_>) =
            (0..num_authorities).map(|_| get_key_pair()).unzip();
        let (bls_public_keys, bls_secret_keys): (Vec<_>, Vec<_>) =
            (0..num_authorities).map(|_| get_bls_key_pair()).unzip();

        for owner_uid in 0..num_authorities {
            let (control_tx, control_rx) = mpsc::channel(1024);
            let (inc_gossips_tx, inc_gossips_rx) = mpsc::channel(1024);
            let (out_gossips_tx, out_gossips_rx) = mpsc::channel(1024);
            let (consensus_tx, consensus_rx) = mpsc::channel(1024);
            let (retrieve_payload_tx, retrieve_payload_rx) = mpsc::channel(1024);

            control_tx_vec.push(control_tx.clone());
            inc_gossips_tx_vec.push(inc_gossips_tx);
            out_gossips_rx_vec.push(out_gossips_rx);
            consensus_rx_vec.push(consensus_rx);

            let task: NightshadeTask =
                NightshadeTask::new(inc_gossips_rx, out_gossips_tx, control_rx, consensus_tx, retrieve_payload_tx);

            tokio::spawn(task.for_each(|_| Ok(())));

            let block_hash = CryptoHash::default();

            // Start the task using control channels, and stop it after 1 second
            let start_task = control_tx
                .clone()
                .send(Control::Reset {
                    owner_uid: owner_uid as u64,
                    block_index: 0,
                    hash: block_hash,
                    public_keys: public_keys.clone(),
                    owner_secret_key: secret_keys[owner_uid].clone(),
                    bls_public_keys: bls_public_keys.clone(),
                    bls_owner_secret_key: bls_secret_keys[owner_uid].clone(),
                })
                .map(|_| ())
                .map_err(|e| error!("Error sending control {:?}", e));
            tokio::spawn(start_task);

            let control_tx1 = control_tx.clone();

            let stop_task = Delay::new(Instant::now() + Duration::from_secs(TASK_DURATION_SEC))
                .then(|_| {
                    control_tx1
                        .send(Control::Stop)
                        .map(|_| ())
                        .map_err(|e| error!("Error sending control {:?}", e))
                });
            tokio::spawn(stop_task);

            let control_tx2 = control_tx.clone();
            let retrieve_task = retrieve_payload_rx.for_each(move |(authority_id, hash)| {
                let send_confirmation = control_tx2
                    .clone()
                    .send(Control::PayloadConfirmation(authority_id, hash))
                    .map(|_| ())
                    .map_err(|_| error!("Fail sending control signal to nightshade"));
                tokio::spawn(send_confirmation);
                future::ok(())
            });
            tokio::spawn(retrieve_task);
        }

        // Traffic management
        for out_gossip_rx in out_gossips_rx_vec.drain(..) {
            let inc_gossip_tx_vec1 = inc_gossips_tx_vec.clone();
            let fut = out_gossip_rx.map(move |message| {
                let gossip_input = inc_gossip_tx_vec1[message.receiver_id].clone();
                tokio::spawn(
                    gossip_input
                        .send(message)
                        .map(|_| ())
                        .map_err(|e| error!("Error relaying message {:?}", e)),
                );
            });

            tokio::spawn(fut.for_each(|_| Ok(())));
        }

        Ok(consensus_rx_vec)
    });

    let test_network = fake_network.and_then(|v| {
        let futures: Vec<_> = v.into_iter().map(|rx| rx.into_future()).collect();

        join_all(futures)
            .map(|v: Vec<(Option<ConsensusBlockProposal>, _)>| {
                // Check every authority committed to the same outcome
                let headers: Vec<_> = v
                    .iter()
                    .map(|(el, _)| el.clone().expect("Authority not committed").proposal)
                    .collect();
                if !headers.iter().all(|h| h == &headers[0]) {
                    panic!("Authorities committed to different outcomes.");
                }
            })
            .map_err(|e| panic!("Failed achieving consensus: {:?}", e))
    });

    let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
    let res = rt.block_on(test_network);
    assert_eq!(res.is_ok(), true);
}

#[cfg(test)]
mod tests {
    use super::spawn_all;

    #[test]
    #[ignore]
    #[should_panic]
    /// One authority don't reach consensus by itself in the current implementation
    fn one_authority() {
        spawn_all(1);
    }

    #[test]
    fn two_authorities() {
        spawn_all(2);
    }

    #[test]
    fn three_authorities() {
        spawn_all(3);
    }

    #[test]
    fn four_authorities() {
        spawn_all(4);
    }

    #[test]
    fn five_authorities() {
        spawn_all(5);
    }

    #[test]
    fn ten_authorities() {
        spawn_all(10);
    }
}
