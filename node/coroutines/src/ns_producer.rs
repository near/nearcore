//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc::{Receiver, Sender};

use client::{ChainConsensusBlockBody, Client};
use client::BlockProductionResult;
use nightshade::nightshade::ConsensusBlockHeader;
use nightshade::nightshade_task::Control;
use primitives::hash::CryptoHash;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};

use crate::ns_control_builder::get_control;

pub fn spawn_block_producer(
    client: Arc<Client>,
    consensus_rx: Receiver<ConsensusBlockHeader>,
    control_tx: Sender<Control<CryptoHash>>,
) {
    let task = consensus_rx
        .for_each(move |consensus_block_header| {
            println!("Block produced for index {}", consensus_block_header.index);
            // TODO: Modify `try_produce_block` method to accept NS consensus, instead of TxFlow
            // consensus.
            if let Some(payload) = client.shard_client.pool.pop_payload_snapshot(&consensus_block_header.header.hash) {
                if let BlockProductionResult::Success(new_beacon_block, _new_shard_block) =
                client.try_produce_block(consensus_block_header.index, payload)
                    {
                        let control = get_control(&*client, new_beacon_block.header().index() + 1);
                        let ns_control_task = control_tx
                            .clone()
                            .send(control)
                            .map(|_| ())
                            .map_err(|e| error!("Error sending control to NightShade: {}", e));
                        tokio::spawn(ns_control_task);
                    }
            }
            future::ok(())
        })
        .map(|_| ());
    tokio::spawn(task);
}
