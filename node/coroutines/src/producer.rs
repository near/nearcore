//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, stream, Future, Sink, Stream};

use crate::control_builder::get_control;
use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use client::{ChainConsensusBlockBody, Client};
use shard::SignedShardBlock;
use transaction::Transaction;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    new_receipts_tx: Sender<Transaction>,
    control_tx: Sender<Control<BeaconWitnessSelector>>,
) {
    let control = get_control(&*client, client.beacon_chain.chain.best_block().header().index() + 1);
    let kickoff_task = control_tx
        .clone()
        .send(control)
        .map(|_| ())
        .map_err(|e| error!("Error sending kick-off control to TxFlow: {}", e));

    let task = receiver
        .for_each(move |body| {
            if let Some((new_beacon_block, new_shard_block)) = client.produce_block(body) {
                // Send beacon block to network
                tokio::spawn({
                    block_announce_tx
                        .clone()
                        .send((new_beacon_block.clone(), new_shard_block.clone()))
                        .map(|_| ())
                        // TODO: In DevNet this will silently fail, because there is no network and so
                        // the announcements cannot be make. In TestNet the failure should not be silent.
                        .map_err(|_| ())
                });

                let control = get_control(&*client, new_beacon_block.header().index() + 1);
                let needs_receipt_rerouting = match control {
                    Control::Stop => false,
                    Control::Reset(_) => true,
                };
                let txflow_task = control_tx
                    .clone()
                    .send(control)
                    .map(|_| ())
                    .map_err(|e| error!("Error sending control to TxFlow: {}", e));
                if needs_receipt_rerouting {
                    let receipts_task = new_receipts_tx
                        .clone()
                        .send_all(stream::iter_ok(new_shard_block.body.new_receipts.to_vec()))
                        .map(|_| ())
                        .map_err(|e| error!("Error sending receipts: {}", e));
                    // First tells TxFlow to reset.
                    // Then, redirect the receipts from the previous block for processing in the next one.
                    tokio::spawn(txflow_task.and_then(|_| receipts_task));
                } else {
                    // Tells TxFlow to stop.
                    tokio::spawn(txflow_task);
                }
            }
            future::ok(())
        })
        .map(|_| ());
    tokio::spawn(kickoff_task.then(|_| task));
}
