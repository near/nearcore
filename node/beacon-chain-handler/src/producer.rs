//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;
use std::collections::HashMap;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, stream, Future, Sink, Stream};

use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use beacon::authority::AuthorityStake;
use client::{ChainConsensusBlockBody, Client};
use primitives::types::{ReceiptTransaction, Transaction, UID};
use txflow::txflow_task::{Control, State};
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_receipts_tx: Sender<ReceiptTransaction>,
    authority_tx: Sender<HashMap<UID, AuthorityStake>>,
    control_tx: Sender<Control<BeaconWitnessSelector>>,
) {
    let task = receiver
        .fold(
            (client, block_announce_tx, new_receipts_tx, authority_tx, control_tx),
            |(client, block_announce_tx, new_receipts_tx, authority_tx, control_tx), body| {
                let (new_block, new_shard_block) = client.produce_block(body);
                // send beacon block to network
                tokio::spawn({
                    block_announce_tx
                        .clone()
                        .send(new_block.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending block: {}", e))
                });

                // Redirect the receipts from the previous block for processing in the next one.
                tokio::spawn({
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
                        .clone()
                        .send_all(stream::iter_ok(receipts))
                        .map(|_| ())
                        .map_err(|e| error!("Error sending receipts: {}", e))
                });

                // Take care of changed authorities.
                // Notify the network about the new set of UID -> AccountId.
                let (owner_uid, uid_to_authority_map) = client.get_uid_to_authority_map(new_block.header().index());
                tokio::spawn({
                    authority_tx
                        .clone()
                        .send(uid_to_authority_map.clone())
                        .map(|_| ())
                        .map_err(|e| error!("Error sending authorities to the network: {}", e))
                });
                // Send control to TxFlow.
                let control = match owner_uid {
                    None => Control::Stop,
                    Some(owner_uid) => {
                        let witness_selector = Box::new(BeaconWitnessSelector::new(
                            uid_to_authority_map.keys().cloned().collect(),
                            owner_uid,
                        ));
                        Control::Reset(State {
                            owner_uid,
                            starting_epoch: 0,
                            gossip_size: 1, // TODO: Use adaptive gossip size.
                            witness_selector,
                        })}
                };
                tokio::spawn({
                    control_tx
                        .clone()
                        .send(control)
                        .map(|_| ())
                        .map_err(|e| error!("Error sending control to TxFlow: {}", e))
                });
                future::ok((client, block_announce_tx, new_receipts_tx, authority_tx, control_tx))
            },
        )
        .and_then(|_| Ok(()));
    tokio::spawn(task);
}
