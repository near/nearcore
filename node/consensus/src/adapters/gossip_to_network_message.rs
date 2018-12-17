//! A simple task converting transactions to payloads.
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream};
use primitives::types::{ChainPayload, Gossip};
use beacon::types::{SignedBeaconBlockHeader, SignedBeaconBlock};
use network::message::Message;
use substrate_network_libp2p::NodeIndex;

pub fn spawn_task(
    receiver: Receiver<Gossip<ChainPayload>>,
    sender: Sender<(NodeIndex, Message<SignedBeaconBlock, SignedBeaconBlockHeader, ChainPayload>)>) {
    let task = receiver
        .map(|g| (g.receiver_uid as NodeIndex, Message::Gossip(g)))
        .forward(
            sender.sink_map_err(|err| error!("Error sending gossip down the sink: {:?}", err)),
        )
        .map(|_| ())
        .map_err(|err| error!("Error while converting gossip to message: {:?}", err));
    tokio::spawn(task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{lazy, stream};
    use futures::sync::mpsc::channel;
    use primitives::types::GossipBody;
    use primitives::signature::DEFAULT_SIGNATURE;

    #[test]
    fn pass_through() {
        tokio::run(lazy(|| {
            let (gossip_tx, gossip_rx) = channel(1024);
            let (message_tx, message_rx) = channel(1024);
            spawn_task(gossip_rx, message_tx);
            let mut gossips = vec![];
            for i in 0..10 {
                gossips.push(
                    Gossip {
                        sender_uid: i,
                        receiver_uid: i,
                        sender_sig: DEFAULT_SIGNATURE,
                        body: GossipBody::Fetch::<ChainPayload>(vec![]),
                    }
                );
            }

            let expected: Vec<NodeIndex> = (0..10).collect();

            let assert_task = stream::iter_ok(gossips)
                .forward(
                    gossip_tx.sink_map_err(|err| panic!("Error sending fake gossips {:?}", err))
                )
                .and_then(|_|
                message_rx.map(|m| match m {
                    (uid1, Message::Gossip( Gossip { sender_uid: uid2, .. } )) => {
                        assert_eq!(uid1, uid2 as NodeIndex);
                        uid1
                    },
                    _ => panic!("Unexpected message")
                })
                    .collect().map(move |actual| {
                    assert_eq!(actual, expected);
                }).map_err(|err| error!("Assertion error {:?}", err))
            );
            tokio::spawn(assert_task);
            Ok(())
        }));
    }
}
