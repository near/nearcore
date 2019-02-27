//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc::{Receiver, Sender};

use client::{ChainConsensusBlockBody, Client};
use client::BlockProductionResult;
use nightshade::nightshade::BlockHeader;
use nightshade::nightshade_task::Control;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};

use crate::ns_control_builder::get_control;

pub fn spawn_block_producer(
    client: Arc<Client>,
    receiver: Receiver<(BlockHeader, u64)>,
    control_tx: Sender<Control<ChainPayload>>,
) {
    let task = receiver
        .for_each(move |(body, block_index)| {
            println!("Block produced for index {}", block_index);
            // TODO: Modify `try_produce_block` method to accept NS consensus, instead of TxFlow
            // consensus.
            let body =
                ChainConsensusBlockBody { payload: ChainPayload::default(), beacon_block_index: block_index };
            if let BlockProductionResult::Success(new_beacon_block, _new_shard_block) =
                client.try_produce_block(body)
            {
                let control = get_control(&*client, new_beacon_block.header().index() + 1);
                let ns_control_task = control_tx
                    .clone()
                    .send(control)
                    .map(|_| ())
                    .map_err(|e| error!("Error sending control to NightShade: {}", e));
                tokio::spawn(ns_control_task);
            }
            future::ok(())
        })
        .map(|_| ());
    tokio::spawn(task);
}
