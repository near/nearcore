//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use std::sync::Arc;

use futures::{Future, future, Stream, Sink, stream};
use futures::sync::mpsc::{Receiver, Sender};

use beacon::types::SignedBeaconBlock;
use client::Client;

pub fn spawn_block_importer(
    client: Arc<Client>,
    receiver: Receiver<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
) {
    let task = receiver.fold((client, new_block_tx), |(client, new_block_tx), block| {
        let imported_blocks = client.import_beacon_block(block);
        let block_tx = new_block_tx.clone();
        tokio::spawn({
            block_tx
                .send_all(stream::iter_ok(imported_blocks))
                .map(|_| ())
                .map_err(|e| {
                    error!("failed to send new block: {:?}", e);
                })
        });
        future::ok((client, new_block_tx))
    }).and_then(|_| Ok(()));
    tokio::spawn(task);
}
