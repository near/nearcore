//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, Future, Sink, Stream};

use client::BlockImportingResult;
use client::Client;
use mempool::pool_task::MemPoolControl;
use primitives::beacon::SignedBeaconBlock;
use primitives::chain::SignedShardBlock;

use crate::ns_control_builder::get_control;

pub fn spawn_block_importer(
    client: Arc<Client>,
    incoming_block_rx: Receiver<(SignedBeaconBlock, SignedShardBlock)>,
    mempool_control_tx: Sender<MemPoolControl>,
) {
    let task = incoming_block_rx.for_each(move |(beacon_block, shard_block)| {
        // TODO: handle other cases.
        if let BlockImportingResult::Success { new_index } =
            client.try_import_blocks(beacon_block, shard_block)
        {
            info!(
                "Successfully imported block(s) up to {}, account_id={:?}",
                new_index, client.account_id
            );
            let next_index = new_index + 1;
            let mempool_control = get_control(&client, next_index);

            // Send mempool control.
            let mempool_reset =
                mempool_control_tx.clone().send(mempool_control).map(|_| ()).map_err(
                    |e| error!(target: "consensus", "Error sending mempool control: {:?}", e),
                );
            tokio::spawn(mempool_reset);
        }
        future::ok(())
    });
    tokio::spawn(task);
}
