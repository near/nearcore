//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, stream, Future, Sink, Stream};

use beacon::types::SignedBeaconBlock;
use client::{ChainConsensusBlockBody, Client};
use primitives::types::{ReceiptTransaction, Transaction};

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
    new_receipts_tx: Sender<ReceiptTransaction>,
) {
    let task = receiver
        .fold(
            (client, block_announce_tx, new_block_tx, new_receipts_tx),
            |(client, block_announce_tx, new_block_tx, new_receipts_tx), body| {
                let (new_block, new_shard_block) = client.produce_block(body);
                // send beacon block to network
                tokio::spawn({
                    let block_announce_tx = block_announce_tx.clone();
                    block_announce_tx
                        .send(new_block.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending block: {}", e))
                });
                // send beacon block to authority handler
                tokio::spawn({
                    let new_block_tx = new_block_tx.clone();
                    new_block_tx
                        .send(new_block.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending block: {}", e))
                });
                // Redirect the receipts from the previous block for processing in the next one.
                tokio::spawn({
                    let new_receipts_tx = new_receipts_tx.clone();
                    let receipts: Vec<_> = new_shard_block
                        .body
                        .new_receipts
                        .iter()
                        .filter_map(|t| match t {
                            Transaction::Receipt(r) => Some(r.clone()),
                            Transaction::SignedTransaction(_) => panic!("new_receipts field should contain receipts only.")
                        })
                        .collect();
                    new_receipts_tx
                        .send_all(stream::iter_ok(receipts))
                        .map(|_| ())
                        .map_err(|e| error!("Error sending receipts: {}", e))
                });
                future::ok((client, block_announce_tx, new_block_tx, new_receipts_tx))
            },
        )
        .and_then(|_| Ok(()));
    tokio::spawn(task);
}
