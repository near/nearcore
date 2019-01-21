//! BeaconBlockImporter consumes blocks that we received from other peers and adds them to the
//! chain.
use std::sync::Arc;

use futures::sync::mpsc::{Receiver, Sender};
use futures::{future, stream, Future, Sink, Stream};

use beacon::types::SignedBeaconBlock;
use client::Client;

pub fn spawn_block_importer(
    client: Arc<Client>,
    receiver: Receiver<SignedBeaconBlock>,
    new_block_tx: Sender<SignedBeaconBlock>,
) {
    let task = receiver
        .fold((client, new_block_tx), |(client, new_block_tx), block| {
            let imported_blocks = client.import_beacon_block(block);
            new_block_tx.clone().send_all(stream::iter_ok(imported_blocks)).then(|res| {
                if let Err(err) = res {
                    error!("failed to send new block: {}", err);
                }
                future::ok((client, new_block_tx))
            })
        })
        .and_then(|_| future::ok(()));
    tokio::spawn(task);
}
