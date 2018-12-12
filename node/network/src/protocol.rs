use std::collections::HashMap;
use std::sync::Arc;
use std::time;

use futures::{Future, Sink, stream};
use futures::sync::mpsc::Sender;
use parking_lot::RwLock;
use substrate_network_libp2p::{NodeIndex, ProtocolId, Secret, Severity};

use chain::{SignedBlock, SignedHeader as BlockHeader, BlockChain};
use message::{self, Message};
use primitives::hash::CryptoHash;
use primitives::traits::{Decode, Payload};
use primitives::types::{BlockId, ReceiptTransaction, SignedTransaction};
use test_utils;

/// time to wait (secs) for a request
const REQUEST_WAIT: u64 = 60;

// Maximum allowed entries in `BlockResponse`
const MAX_BLOCK_DATA_RESPONSE: u64 = 128;

/// current version of the protocol
pub(crate) const CURRENT_VERSION: u32 = 1;

#[derive(Clone, Copy)]
pub struct ProtocolConfig {
    // config information goes here
    pub protocol_id: ProtocolId,
    // This is hacky. Ideally we want public key here, but
    // I haven't figured out how to get public key for a node
    // from substrate libp2p
    pub secret: Secret,
}

impl ProtocolConfig {
    pub fn new(protocol_id: ProtocolId, secret: Secret) -> ProtocolConfig {
        ProtocolConfig { protocol_id, secret }
    }

    pub fn new_with_default_id(secret: Secret) -> ProtocolConfig {
        ProtocolConfig { protocol_id: ProtocolId::default(), secret }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        let secret = test_utils::create_secret();
        ProtocolConfig::new(ProtocolId::default(), secret)
    }
}

#[allow(dead_code)]
pub(crate) struct PeerInfo {
    /// Protocol version.
    protocol_version: u32,
    /// best hash from peer.
    best_hash: CryptoHash,
    /// Best block index from peer.
    best_index: u64,
    /// Information about connected peers.
    request_timestamp: Option<time::Instant>,
    /// Pending block request.
    block_request: Option<message::BlockRequest>,
    /// Next request id.
    next_request_id: u64,
}

pub struct Protocol<B: SignedBlock, Header: BlockHeader, P> {
    // TODO: add more fields when we need them
    pub config: ProtocolConfig,
    /// Peers that are in the handshaking process.
    handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    /// Info about peers.
    peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
    /// Chain info, for read-only access.
    chain: Arc<BlockChain<B>>,
    /// Channel into which the protocol sends the new blocks.
    block_sender: Sender<B>,
    /// Channel into which the protocol sends the received transactions.
    transaction_sender: Sender<SignedTransaction>,
    /// Channel into which the protocol sends the received receipts.
    receipt_sender: Sender<ReceiptTransaction>,
    /// Channel into which the protocol sends the messages that should be send back to the network.
    message_sender: Sender<(NodeIndex, Message<B, Header, P>)>,
}

impl<B: SignedBlock, Header: BlockHeader, P: Payload> Protocol<B, Header, P> {
    pub fn new(config: ProtocolConfig,
               chain: Arc<BlockChain<B>>,
               block_sender: Sender<B>,
               transaction_sender: Sender<SignedTransaction>,
               receipt_sender: Sender<ReceiptTransaction>,
               message_sender: Sender<(NodeIndex, Message<B, Header, P>)>,
    ) -> Self {
        Self {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
            chain,
            block_sender,
            transaction_sender,
            receipt_sender,
            message_sender,
        }
    }

    pub fn on_peer_connected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        // use this placeholder for now. Change this when block storage is ready
        let status = message::Status::default();
        let message = Message::Status(status);
        self.send_message(peer, message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().remove(&peer);
        self.peer_info.write().remove(&peer);
    }

    pub fn on_transaction_message(&self, transaction: SignedTransaction) {
        let copied_tx = self.transaction_sender.clone();
        tokio::spawn(
            copied_tx
                .send(transaction)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the transactions {:?}", e)),
        );
    }

    /// Note, we will not actually need this until we have shards.
    fn on_receipt_message(&self, receipt: ReceiptTransaction) {
        let copied_tx = self.receipt_sender.clone();
        tokio::spawn(
            copied_tx
                .send(receipt)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the receipts {:?}", e)),
        );
    }

    fn on_status_message(
        &self, peer: NodeIndex,
        status: &message::Status
    ) -> Result<(), (NodeIndex, Severity)> {
        if status.version != CURRENT_VERSION {
            return Err((peer, Severity::Bad("Peer uses incompatible version.")));
        }
        if status.genesis_hash != self.chain.genesis_hash {
            return Err((peer, Severity::Bad("Peer has different genesis hash.")));
        }

        // request blocks to catch up if necessary
        let best_index = self.chain.best_block().header().index();
        let mut next_request_id = 0;
        if status.best_index > best_index {
            let request = message::BlockRequest {
                id: next_request_id,
                from: BlockId::Number(best_index),
                to: Some(BlockId::Number(status.best_index)),
                max: None,
            };
            next_request_id += 1;
            let message = Message::BlockRequest(request);
            self.send_message(peer, message);
        }

        let peer_info = PeerInfo {
            protocol_version: status.version,
            best_hash: status.best_hash,
            best_index: status.best_index,
            request_timestamp: None,
            block_request: None,
            next_request_id,
        };
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
        Ok(())
    }

    fn on_block_request(
        &self,
        peer: NodeIndex,
        request: message::BlockRequest,
    ) {
        let mut blocks = Vec::new();
        let mut id = request.from;
        let max = std::cmp::min(request.max.unwrap_or(u64::max_value()), MAX_BLOCK_DATA_RESPONSE);
        while let Some(block) = self.chain.get_block(&id) {
            blocks.push(block);
            if blocks.len() as u64 >= max {
                break;
            }
            let header = self.chain.get_header(&id).unwrap();
            let block_index = header.index();
            let block_hash = header.block_hash();
            let reach_end = match request.to {
                Some(BlockId::Number(n)) => block_index == n,
                Some(BlockId::Hash(h)) => block_hash == h,
                None => false,
            };
            if reach_end {
                break;
            }
            id = BlockId::Number(block_index);
        }
        let response = message::BlockResponse { id: request.id, blocks };
        let message = Message::BlockResponse(response);
        self.send_message(peer, message);
    }

    fn on_incoming_block(&self, block: B) {
        let copied_tx = self.block_sender.clone();
        tokio::spawn(
            copied_tx
                .send(block)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    fn on_block_response(
        &self,
        _peer: NodeIndex,
        response: message::BlockResponse<B>,
    ) {
        let copied_tx = self.block_sender.clone();
        tokio::spawn(
            copied_tx
                .send_all(stream::iter_ok(response.blocks))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    pub fn on_message(&self, peer: NodeIndex, data: &[u8]) -> Result<(), (NodeIndex, Severity)> {
        let message: Message<B, Header, P> = Decode::decode(data)
            .ok_or((peer, Severity::Bad("Cannot decode message.")))?;

        match message {
            Message::Transaction(tx) => {
                self.on_transaction_message(*tx);
            },
            Message::Receipt(receipt) => {
                self.on_receipt_message(*receipt);
            },
            Message::Status(status) => {
                self.on_status_message(peer, &status)?;
            },
            Message::BlockRequest(request) => self.on_block_request(peer, request),
            Message::BlockResponse(response) => {
                let request = {
                    let mut peers = self.peer_info.write();
                    let mut peer_info = peers.get_mut(&peer)
                        .ok_or((peer, Severity::Bad("Unexpected packet received from peer")))?;
                    peer_info.block_request.take()
                            .ok_or((peer, Severity::Bad("Unexpected response packet received from peer")))?
                };
                if request.id != response.id {
                    trace!(target: "sync", "Ignoring mismatched response packet from {} (expected {} got {})", peer, request.id, response.id);
                    return Ok(())
                }
                self.on_block_response(peer, response);
            },
            Message::BlockAnnounce(ann) => {
                debug!(target: "sync", "receive block announcement: {:?}", ann);
                // header is actually block for now
                match ann {
                    message::BlockAnnounce::Block(b) => {
                        self.on_incoming_block(b);
                    }
                    _ => unimplemented!(),
                }
            },
            Message::Gossip(_) => {},
        }
        Ok(())
    }

    pub fn send_message(&self, receiver_index: NodeIndex, message: Message<B, Header, P>) {
        let copied_tx = self.message_sender.clone();
        tokio::spawn(
            copied_tx
                .send((receiver_index, message))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    /// Returns the list of peers that have timedout.
    pub fn maintain_peers(&self) -> Vec<NodeIndex> {
        let cur_time = time::Instant::now();
        let mut aborting = Vec::new();
        let peer_info = self.peer_info.read();
        let handshaking_peers = self.handshaking_peers.read();
        for (peer, time_stamp) in peer_info
            .iter()
            .filter_map(|(id, info)| info.request_timestamp.as_ref().map(|x| (id, x)))
            .chain(handshaking_peers.iter()) {
            if (cur_time - *time_stamp).as_secs() > REQUEST_WAIT {
                trace!(target: "sync", "Timeout {}", *peer);
                aborting.push(*peer);
            }
        }
        aborting
    }

//    /// produce blocks
//    /// some super fake logic: if the node has a specific key,
//    /// then it produces and broadcasts the block
//    pub fn prod_block(&self) {
//        let special_secret = test_utils::special_secret();
//        if special_secret == self.config.secret {
//            let block = self.chain.write().prod_block();
//            let block_announce = message::BlockAnnounce::Block(block.clone());
//            let message: Message<_, Header> =
//                Message::new(MessageBody::BlockAnnounce(block_announce));
//            let peer_info = self.peer_info.read();
//            for peer in peer_info.keys() {
//                self.send_message(*peer, &message);
//            }
//            self.on_incoming_block(block);
//        }
//    }
}

//#[cfg(test)]
//mod tests {
//    use super::*;
//    use client::test_utils::*;
//    use primitives::hash::hash;
//    use primitives::types::{SignedTransaction, TransactionBody};
//    use test_utils::*;
//    use primitives::signature::DEFAULT_SIGNATURE;
//    use parking_lot::Mutex;
//
//    impl<B: Block, H: ProtocolHandler> Protocol<B, H> {
//        fn _on_message(&self, data: &[u8]) -> Message<B, B::Header> {
//            match Decode::decode(data) {
//                Some(m) => m,
//                _ => panic!("cannot decode message: {:?}", data),
//            }
//        }
//    }
//
//    #[test]
//    fn test_serialization() {
//        let tx = SignedTransaction::empty();
//        let message: Message<MockBlock, MockBlockHeader> =
//            Message::new(MessageBody::Transaction(Box::new(tx)));
//        let config = ProtocolConfig::default();
//        let mock_client = Arc::new(RwLock::new(MockClient::default()));
//        let protocol = Protocol::new(config, MockProtocolHandler::default(), mock_client);
//        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
//        assert_eq!(message, decoded);
//    }
//
//    #[test]
//    fn test_on_transaction_message() {
//        let config = ProtocolConfig::default();
//        let mock_client = Arc::new(RwLock::new(MockClient::default()));
//        let protocol = Protocol::new(config, MockProtocolHandler::default(), mock_client);
//        let tx = SignedTransaction::empty();
//        protocol.on_transaction_message(tx);
//    }
//
//    #[test]
//    fn test_protocol_and_client() {
//        let client = Arc::new(RwLock::new(generate_test_client()));
//        let handler = MockHandler { client: client.clone() };
//        let config = ProtocolConfig::new_with_default_id(special_secret());
//        let protocol = Protocol::new(config, handler, client.clone());
//        let network_service = Arc::new(Mutex::new(default_network_service()));
//        protocol.on_transaction_message(SignedTransaction::new(
//            DEFAULT_SIGNATURE,
//            TransactionBody::new(1, hash(b"bob"), hash(b"alice"), 10, String::new(), vec![]),
//        ));
//        assert_eq!(client.read().num_transactions(), 1);
//        assert_eq!(client.read().num_blocks_in_queue(), 0);
//        protocol.prod_block::<MockBlockHeader>(&mut net_sync);
//        assert_eq!(client.read().num_transactions(), 0);
//        assert_eq!(client.read().num_blocks_in_queue(), 0);
//        assert_eq!(client.read().best_index(), 1);
//    }
//}
