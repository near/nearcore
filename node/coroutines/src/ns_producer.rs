//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};

use client::BlockProductionResult;
use client::Client;
use mempool::pool_task::MemPoolControl;
use nightshade::nightshade::ConsensusBlockProposal;
use primitives::beacon::SignedBeaconBlock;
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::SignedShardBlock;

use crate::ns_control_builder::get_control;

// Create new block proposal. Send control signal to NightshadeTask and gossip Block around.
fn spawn_start_proposal(
    client: Arc<Client>,
    block_index: u64,
    mempool_control_tx: Sender<MemPoolControl>,
) {
    let mempool_control = get_control(&client, block_index);

    // Send mempool control.
    let mempool_reset = mempool_control_tx
        .send(mempool_control)
        .map(|_| ())
        .map_err(|e| error!(target: "consensus", "Error sending mempool control: {:?}", e));
    tokio::spawn(mempool_reset);
}

pub fn spawn_block_producer(
    client: Arc<Client>,
    consensus_rx: Receiver<ConsensusBlockProposal>,
    mempool_control_tx: Sender<MemPoolControl>,
    out_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
) {
    // Send proposal for the first block
    spawn_start_proposal(client.clone(), 1, mempool_control_tx.clone());

    let task = consensus_rx
        .for_each(move |consensus_block_header| {
            info!(target: "consensus", "Producing block for account_id={:?}, index {}", client.account_id, consensus_block_header.index);
            if let Some(payload) = client.shard_client.pool.pop_payload_snapshot(&consensus_block_header.proposal.hash) {
                if let BlockProductionResult::Success(new_beacon_block, new_shard_block) =
                client.try_produce_block(consensus_block_header.index, payload)
                    {
                        let next_index = new_beacon_block.header().index() + 1;
                        // Send block announcement.
                        tokio::spawn(
                            out_block_tx.clone().send((new_beacon_block, new_shard_block)).map(|_| ()).map_err(|e| error!(target: "consensus", "Error sending block announcement: {}", e))
                        );
                        // Send proposal for the next block
                        spawn_start_proposal(client.clone(), next_index, mempool_control_tx.clone());
                    } else {
                    info!(target: "consensus", "Block production did not succeed");
                }
            } else {
                // Assumption: This else should never be reached, if an authority achieves consensus on some block hash
                // then it is because this block can be retrieved from the mempool.
                warn!(target: "consensus", "Authority: {} Failed to find payload for {} from authority {}",
                      client.account_id, consensus_block_header.proposal.hash, consensus_block_header.proposal.author);
            }
            future::ok(())
        })
        .map(|_| ());
    tokio::spawn(task);
}
