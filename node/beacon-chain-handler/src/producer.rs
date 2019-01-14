//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};

use beacon::types::SignedBeaconBlock;
use chain::SignedBlock;
use client::{Client, ChainConsensusBlockBody};
use primitives::traits::Signer;

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
) {
    let task = receiver
        .fold(
            (client, block_announce_tx, new_block_tx),
            |(client, block_announce_tx, new_block_tx), body| {
                let new_block = client.produce_block(body);
                // send beacon block to network
                tokio::spawn({
                    let block_announce_tx = block_announce_tx.clone();
                    block_announce_tx
                        .send(new_block.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending block: {:?}", e))
                });
                // send beacon block to authority handler
                tokio::spawn({
                    let new_block_tx = new_block_tx.clone();
                    new_block_tx
                        .send(new_block.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending block: {:?}", e))
                });
                future::ok((client, block_announce_tx, new_block_tx))
            },
        )
        .and_then(|_| Ok(()));
    tokio::spawn(task);
}
