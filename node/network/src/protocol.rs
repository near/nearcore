use client::chain::Chain;
use io::{NetSyncIo, SyncIo};
use message::{self, Message, MessageBody};
use parking_lot::RwLock;
use primitives::hash::CryptoHash;
use primitives::traits::{Block, Decode, Encode, GenericResult, Header as BlockHeader};
use primitives::types::{BlockId, SignedTransaction};
use rand::{seq, thread_rng};
use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use substrate_network_libp2p::{NodeIndex, ProtocolId, Secret, Severity};
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

#[allow(dead_code)]
pub struct Protocol<B: Block, H: ProtocolHandler> {
    // TODO: add more fields when we need them
    pub config: ProtocolConfig,
    // peers that are in the handshaking process
    handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    // info about peers
    peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
    // backend client
    chain: Arc<Chain<B>>,
    // callbacks
    handler: Option<Box<H>>,
    // phantom data for keep T
}

pub trait ProtocolHandler: 'static {
    fn handle_transaction(&self, transaction: SignedTransaction) -> GenericResult;
}

impl<B: Block, H: ProtocolHandler> Protocol<B, H> {
    pub fn new(config: ProtocolConfig, handler: H, chain: Arc<Chain<B>>) -> Protocol<B, H> {
        Protocol {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
            handler: Some(Box::new(handler)),
            chain,
        }
    }

    pub fn on_peer_connected<Header: BlockHeader>(
        &self,
        net_sync: &mut NetSyncIo,
        peer: NodeIndex,
    ) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        // use this placeholder for now. Change this when block storage is ready
        let status = message::Status::default();
        let message: Message<_, Header> = Message::new(MessageBody::Status(status));
        self.send_message(net_sync, peer, &message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().remove(&peer);
        self.peer_info.write().remove(&peer);
    }

    pub fn sample_peers(&self, num_to_sample: usize) -> Result<Vec<NodeIndex>, ()> {
        let mut rng = thread_rng();
        let peer_info = self.peer_info.read();
        let owned_peers = peer_info.keys().cloned();
        seq::sample_iter(&mut rng, owned_peers, num_to_sample).map_err(|_| ())
    }

    pub fn on_transaction_message(&self, tx: SignedTransaction) {
        //TODO: communicate to consensus
        self.handler.as_ref().unwrap().handle_transaction(tx).unwrap();
    }

    fn on_status_message<Header: BlockHeader>(
        &self,
        net_sync: &mut NetSyncIo,
        peer: NodeIndex,
        status: &message::Status,
    ) {
        if status.version != CURRENT_VERSION {
            debug!(target: "sync", "Version mismatch");
            net_sync.report_peer(
                peer,
                Severity::Bad(&format!("Peer uses incompatible version {}", status.version)),
            );
            return;
        }
        if status.genesis_hash != self.chain.genesis_hash() {
            net_sync.report_peer(
                peer,
                Severity::Bad(&format!(
                    "peer has different genesis hash (ours {:?}, theirs {:?})",
                    self.chain.genesis_hash(),
                    status.genesis_hash
                )),
            );
            return;
        }

        // request blocks to catch up if necessary
        let best_index = self.chain.best_index();
        let mut next_request_id = 0;
        if status.best_index > best_index {
            let request = message::BlockRequest {
                id: next_request_id,
                from: BlockId::Number(best_index),
                to: Some(BlockId::Number(status.best_index)),
                max: None,
            };
            next_request_id += 1;
            let message: Message<_, Header> = Message::new(MessageBody::BlockRequest(request));
            self.send_message(net_sync, peer, &message);
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
    }

    fn on_block_request<Header: BlockHeader>(
        &self,
        net_sync: &mut NetSyncIo,
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
            let block_hash = header.hash();
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
        let message: Message<_, Header> = Message::new(MessageBody::BlockResponse(response));
        self.send_message(net_sync, peer, &message);
    }

    fn on_block_response(
        &self,
        _net_sync: &mut NetSyncIo,
        _peer: NodeIndex,
        response: message::BlockResponse<B>,
    ) {
        // TODO: validate response
        self.chain.import_blocks(response.blocks);
    }

    pub fn on_message<Header: BlockHeader>(
        &self,
        net_sync: &mut NetSyncIo,
        peer: NodeIndex,
        data: &[u8],
    ) {
        let message: Message<_, Header> = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                debug!("cannot decode message: {:?}", data);
                net_sync.report_peer(peer, Severity::Bad("invalid message format"));
                return;
            }
        };
        match message.body {
            MessageBody::Transaction(tx) => self.on_transaction_message(tx),
            MessageBody::Status(status) => {
                self.on_status_message::<Header>(net_sync, peer, &status)
            }
            MessageBody::BlockRequest(request) => {
                self.on_block_request::<Header>(net_sync, peer, request)
            }
            MessageBody::BlockResponse(response) => {
                let request = {
                    let mut peers = self.peer_info.write();
                    if let Some(ref mut peer_info) = peers.get_mut(&peer) {
                        peer_info.request_timestamp = None;
                        match peer_info.block_request.take() {
                            Some(r) => r,
                            None => {
                                net_sync.report_peer(
                                    peer,
                                    Severity::Bad("Unexpected response packet received from peer"),
                                );
                                return;
                            }
                        }
                    } else {
                        net_sync.report_peer(
                            peer,
                            Severity::Bad("Unexpected packet received from peer"),
                        );
                        return;
                    }
                };
                if request.id != response.id {
                    trace!(target: "sync", "Ignoring mismatched response packet from {} (expected {} got {})", peer, request.id, response.id);
                    return;
                }
                self.on_block_response(net_sync, peer, response)
            }
            MessageBody::BlockAnnounce(ann) => {
                debug!(target: "sync", "receive block announcement: {:?}", ann);
                // header is actually block for now
                match ann {
                    message::BlockAnnounce::Block(b) => {
                        self.chain.import_blocks(vec![b]);
                    }
                    _ => unimplemented!(),
                }
            }
        }
    }

    pub fn send_message<Header: BlockHeader>(
        &self,
        net_sync: &mut NetSyncIo,
        node_index: NodeIndex,
        message: &Message<B, Header>,
    ) {
        match Encode::encode(message) {
            Some(data) => {
                net_sync.send(node_index, data);
            }
            _ => {
                // this should never happen
                error!("FATAL: cannot encode message: {:?}", message);
                return;
            }
        };
    }

    pub fn maintain_peers(&self, net_sync: &mut NetSyncIo) {
        let cur_time = time::Instant::now();
        let mut aborting = Vec::new();
        let peer_info = self.peer_info.read();
        let handshaking_peers = self.handshaking_peers.read();
        for (peer, time_stamp) in peer_info
            .iter()
            .filter_map(|(id, info)| info.request_timestamp.as_ref().map(|x| (id, x)))
            .chain(handshaking_peers.iter())
        {
            if (cur_time - *time_stamp).as_secs() > REQUEST_WAIT {
                trace!(target: "sync", "Timeout {}", *peer);
                aborting.push(*peer);
            }
        }
        for peer in aborting {
            net_sync.report_peer(peer, Severity::Timeout);
        }
    }

    /// produce blocks
    /// some super fake logic: if the node has a specific key,
    /// then it produces and broadcasts the block
    pub fn prod_block<Header: BlockHeader>(&self, net_sync: &mut NetSyncIo) {
        let special_secret = test_utils::special_secret();
        if special_secret == self.config.secret {
            let block = self.chain.prod_block();
            let block_announce = message::BlockAnnounce::Block(block.clone());
            let message: Message<_, Header> =
                Message::new(MessageBody::BlockAnnounce(block_announce));
            let peer_info = self.peer_info.read();
            for peer in peer_info.keys() {
                self.send_message(net_sync, *peer, &message);
            }
            self.chain.import_blocks(vec![block]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use client::test_utils::*;
    use primitives::hash::hash;
    use primitives::types;
    use std::cell::RefCell;
    use std::rc::Rc;
    use test_utils::*;

    impl<B: Block, H: ProtocolHandler> Protocol<B, H> {
        fn _on_message(&self, data: &[u8]) -> Message<B, B::Header> {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data),
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::empty();
        let message: Message<MockBlock, MockBlockHeader> =
            Message::new(MessageBody::Transaction(tx));
        let config = ProtocolConfig::default();
        let mock_client = Arc::new(MockClient::default());
        let protocol = Protocol::new(config, MockProtocolHandler::default(), mock_client);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

    #[test]
    fn test_on_transaction_message() {
        let config = ProtocolConfig::default();
        let mock_client = Arc::new(MockClient::default());
        let protocol = Protocol::new(config, MockProtocolHandler::default(), mock_client);
        let tx = types::SignedTransaction::empty();
        protocol.on_transaction_message(tx);
    }

    #[test]
    fn test_protocol_and_client() {
        let client = Arc::new(generate_test_client());
        let handler = MockHandler { client: client.clone() };
        let config = ProtocolConfig::new_with_default_id(special_secret());
        let protocol = Protocol::new(config, handler, client.clone());
        let network_service = Rc::new(RefCell::new(default_network_service()));
        let mut net_sync = NetSyncIo::new(network_service, protocol.config.protocol_id);
        protocol.on_transaction_message(SignedTransaction::new(
            123,
            types::TransactionBody::new(1, hash(b"bob"), hash(b"alice"), 10, String::new(), vec![]),
        ));
        assert_eq!(client.num_transactions(), 1);
        assert_eq!(client.num_blocks_in_queue(), 0);
        protocol.prod_block::<MockBlockHeader>(&mut net_sync);
        assert_eq!(client.num_transactions(), 0);
        assert_eq!(client.num_blocks_in_queue(), 0);
        assert_eq!(client.best_index(), 1);
    }
}
