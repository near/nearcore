//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::collections::HashMap;
use std::sync::Arc;

//use futures::future::Either;
use futures::sync::mpsc::{Receiver, Sender};
use futures::{stream, Future, Sink, Stream};

use crate::control_builder::get_control;
use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use client::{ChainConsensusBlockBody, Client};
use primitives::types::{UID, AuthorityStake};
use transaction::Transaction;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use txflow::txflow_task::Control;

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<ChainConsensusBlockBody>,
    block_announce_tx: Sender<SignedBeaconBlock>,
    new_receipts_tx: Sender<Transaction>,
    _authority_tx: &Sender<HashMap<UID, AuthorityStake>>,
    control_tx: Sender<Control<BeaconWitnessSelector>>,
) {
    let task = receiver
        .for_each(move |body| {
            let (new_block, new_shard_block) = client.produce_block(body);
            // Send beacon block to network
            tokio::spawn({
                block_announce_tx
                    .clone()
                    .send(new_block.clone())
                    .map(|_| ())
                    // TODO: In DevNet this will silently fail, because there is no network and so
                    // the announcements cannot be make. In TestNet the failure should not be silent.
                    .map_err(|_| ())
            });

            let control = get_control(&*client, new_block.header().index());
//            let needs_receipt_rerouting = match control {
//                Control::Stop => false,
//                Control::Reset(_) => true,
//            };
            let txflow_task = control_tx
                .clone()
                .send(control)
                .map(|_| ())
                .map_err(|e| error!("Error sending control to TxFlow: {}", e));
                let receipts_task = new_receipts_tx
                    .clone()
                    .send_all(stream::iter_ok(new_shard_block.body.new_receipts.to_vec()))
                    .map(|_| ())
                    .map_err(|e| error!("Error sending receipts: {}", e));
                txflow_task.and_then(|_| receipts_task)
//            if needs_receipt_rerouting {
//                let receipts_task = new_receipts_tx
//                    .clone()
//                    .send_all(stream::iter_ok(new_shard_block.body.new_receipts.to_vec()))
//                    .map(|_| ())
//                    .map_err(|e| error!("Error sending receipts: {}", e));
//                // First tells TxFlow to reset.
//                // Then, redirect the receipts from the previous block for processing in the next one.
//                Either::A(txflow_task.and_then(|_| receipts_task))
//            } else {
//                // Tells TxFlow to stop.
//                Either::B(txflow_task)
//            }
        })
        .map(|_| ());
    tokio::spawn(task);
}
