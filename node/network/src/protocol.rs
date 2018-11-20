use parking_lot::RwLock;
use rand::{seq, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time;
use substrate_network_libp2p::{NodeIndex, ProtocolId, Severity};

use client::Client;
use io::{NetSyncIo, SyncIo};
use message::{self, Message, MessageBody};
use primitives::hash::CryptoHash;
use primitives::traits::{Block, Decode, Encode, GenericResult, Header};
use primitives::types::BlockId;

/// time to wait (secs) for a request
const REQUEST_WAIT: u64 = 60;

// Maximum allowed entries in `BlockResponse`
const MAX_BLOCK_DATA_RESPONSE: u64 = 128;

/// current version of the protocol
pub(crate) const CURRENT_VERSION: u32 = 1;

pub struct ProtocolConfig {
    // config information goes here
    pub protocol_id: ProtocolId,
}

impl ProtocolConfig {
    pub fn new(protocol_id: ProtocolId) -> ProtocolConfig {
        ProtocolConfig { protocol_id }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig::new(ProtocolId::default())
    }
}

#[allow(dead_code)]
pub(crate) struct PeerInfo {
    // protocol version
    protocol_version: u32,
    // best hash from peer
    best_hash: CryptoHash,
    // best block number from peer
    best_number: u64,
    // information about connected peers
    request_timestamp: Option<time::Instant>,
    // pending block request
    block_request: Option<message::BlockRequest>,
    // next request id
    next_request_id: u64,
}

pub trait Transaction: Send + Sync + Serialize + DeserializeOwned + Debug + 'static {}
impl<T> Transaction for T where T: Send + Sync + Serialize + DeserializeOwned + Debug + 'static {}

#[allow(dead_code)]
pub struct Protocol<T, B> {
    // TODO: add more fields when we need them
    config: ProtocolConfig,
    // peers that are in the handshaking process
    handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    // info about peers
    peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
    // backend client
    client: Arc<Client<B>>,
    // callbacks
    tx_callback: fn(T) -> GenericResult,
}

impl<T: Transaction, B: Block> Protocol<T, B> {
    pub fn new(
        config: ProtocolConfig,
        client: Arc<Client<B>>,
        tx_callback: fn(T) -> GenericResult,
    ) -> Protocol<T, B> {
        Protocol {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
            client,
            tx_callback,
        }
    }

    pub fn on_peer_connected(&self, net_sync: &mut NetSyncIo, peer: NodeIndex) {
        self.handshaking_peers
            .write()
            .insert(peer, time::Instant::now());
        // use this placeholder for now. Change this when block storage is ready
        let status = message::Status::default();
        let message: Message<T, B> = Message::new(MessageBody::Status(status));
        self.send_message(net_sync, peer, &message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().remove(&peer);
        self.peer_info.write().remove(&peer);
    }

    pub fn sample_peers(&self, num_to_sample: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        let peer_info = self.peer_info.read();
        let owned_peers = peer_info.keys().cloned();
        seq::sample_iter(&mut rng, owned_peers, num_to_sample).unwrap()
    }

    fn on_transaction_message(&self, tx: T) {
        //TODO: communicate to consensus
        let _ = (self.tx_callback)(tx);
    }

    fn on_status_message(
        &self,
        net_sync: &mut NetSyncIo,
        peer: NodeIndex,
        status: &message::Status,
    ) {
        if status.version != CURRENT_VERSION {
            debug!(target: "sync", "Version mismatch");
            net_sync.report_peer(
                peer,
                Severity::Bad(&format!(
                    "Peer uses incompatible version {}",
                    status.version
                )),
            );
            return;
        }
        if status.genesis_hash != self.client.genesis_hash() {
            net_sync.report_peer(
                peer,
                Severity::Bad(&format!(
                    "peer has different genesis hash (ours {:?}, theirs {:?})",
                    self.client.genesis_hash(),
                    status.genesis_hash
                )),
            );
            return;
        }

        // request blocks to catch up if necessary
        let best_number = self.client.best_number();
        let mut next_request_id = 0;
        if status.best_number > best_number {
            let request = message::BlockRequest {
                id: next_request_id,
                from: BlockId::Number(best_number),
                to: Some(BlockId::Number(status.best_number)),
                max: None,
            };
            next_request_id += 1;
            let message = Message::new(MessageBody::BlockRequest(request));
            self.send_message(net_sync, peer, &message);
        }

        let peer_info = PeerInfo {
            protocol_version: status.version,
            best_hash: status.best_hash,
            best_number: status.best_number,
            request_timestamp: None,
            block_request: None,
            next_request_id,
        };
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
    }

    fn on_block_request(
        &self,
        net_sync: &mut NetSyncIo,
        peer: NodeIndex,
        request: message::BlockRequest,
    ) {
        let mut blocks = Vec::new();
        let mut id = request.from;
        let max = std::cmp::min(
            request.max.unwrap_or(u64::max_value()),
            MAX_BLOCK_DATA_RESPONSE,
        );
        while let Some(block) = self.client.get_block(&id) {
            blocks.push(block);
            if blocks.len() as u64 >= max {
                break;
            }
            let header = self.client.get_header(&id).unwrap();
            let block_number = header.number();
            let block_hash = header.hash();
            let reach_end = match request.to {
                Some(BlockId::Number(n)) => block_number == n,
                Some(BlockId::Hash(h)) => block_hash == h,
                None => false,
            };
            if reach_end {
                break;
            }
            id = BlockId::Number(block_number);
        }
        let response = message::BlockResponse {
            id: request.id,
            blocks,
        };
        let message = Message::new(MessageBody::BlockResponse(response));
        self.send_message(net_sync, peer, &message);
    }

    fn on_block_response(
        &self,
        _net_sync: &mut NetSyncIo,
        _peer: NodeIndex,
        response: message::BlockResponse<B>,
    ) {
        // TODO: validate response
        self.client.import_blocks(response.blocks);
    }

    pub fn on_message(&self, net_sync: &mut NetSyncIo, peer: NodeIndex, data: &[u8]) {
        let message: Message<T, B> = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                debug!("cannot decode message: {:?}", data);
                net_sync.report_peer(peer, Severity::Bad("invalid message format"));
                return;
            }
        };
        match message.body {
            MessageBody::Transaction(tx) => self.on_transaction_message(tx),
            MessageBody::Status(status) => self.on_status_message(net_sync, peer, &status),
            MessageBody::BlockRequest(request) => self.on_block_request(net_sync, peer, request),
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
        }
    }

    pub fn send_message(
        &self,
        net_sync: &mut NetSyncIo,
        node_index: NodeIndex,
        message: &Message<T, B>,
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
}

#[cfg(test)]
mod tests {

    use super::*;
    use primitives::types;
    use test_utils::*;

    impl<T: Transaction, B: Block> Protocol<T, B> {
        fn _on_message(&self, data: &[u8]) -> Message<T, B> {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data),
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::new(0, types::TransactionBody::new(0, 0, 0, 0));
        let message: Message<_, MockBlock> = Message::new(MessageBody::Transaction(tx));
        let config = ProtocolConfig::default();
        let callback = |_| Ok(());
        let mock_client = Arc::new(MockClient {
            block: MockBlock {},
        });
        let protocol = Protocol::new(config, mock_client, callback);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

}
