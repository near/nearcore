//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc::{Receiver, Sender};

use client::Client;
use client::BlockProductionResult;
use nightshade::nightshade::ConsensusBlockHeader;
use nightshade::nightshade_task::Control;
use primitives::hash::CryptoHash;
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::ReceiptBlock;

use crate::ns_control_builder::get_control;

pub fn spawn_block_producer(
    client: Arc<Client>,
    consensus_rx: Receiver<ConsensusBlockHeader>,
    control_tx: Sender<Control<CryptoHash>>,
    receipts_tx: Sender<ReceiptBlock>,
) {
    let task = consensus_rx
        .for_each(move |consensus_block_header| {
            info!(target: "consensus", "Producing block for index {}", consensus_block_header.index);
            if let Some(payload) = client.shard_client.pool.pop_payload_snapshot(&consensus_block_header.header.hash) {
                if let BlockProductionResult::Success(new_beacon_block, new_shard_block) =
                client.try_produce_block(consensus_block_header.index, payload)
                    {
                        // TODO: here should be dealing with receipts for other shards.
                        let receipt_block = client.shard_client.get_receipt_block(new_shard_block.index(), new_shard_block.shard_id());
                        if let Some(receipt_block) = receipt_block {
                            tokio::spawn(
                                receipts_tx
                                    .clone()
                                    .send(receipt_block)
                                    .map(|_| ())
                                    .map_err(|e| error!("Error sending receipts from produced block: {}", e))
                            );
                        }
                        let control = get_control(&*client, new_beacon_block.header().index() + 1);
                        let ns_control_task = control_tx
                            .clone()
                            .send(control)
                            .map(|_| ())
                            .map_err(|e| error!("Error sending control to NightShade: {}", e));
                        tokio::spawn(ns_control_task);
                    }
            } else {
                warn!(target: "consensus", "Failed to find payload for {} from authority {}",
                      consensus_block_header.header.hash, consensus_block_header.header.author);
            }
            future::ok(())
        })
        .map(|_| ());
    tokio::spawn(task);
}
