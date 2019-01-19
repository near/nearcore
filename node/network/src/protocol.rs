use std::collections::HashMap;
use std::sync::Arc;
use std::time;

use futures::{Future, Sink, stream};
use futures::sync::mpsc::Sender;
use parking_lot::RwLock;
use substrate_network_libp2p::{NodeIndex, ProtocolId, Severity};

use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use client::Client;
use primitives::hash::CryptoHash;
use primitives::traits::Decode;
use primitives::types::{
    AccountId, BlockId, Gossip, UID, AuthorityStake
};
use transaction::{ChainPayload, Transaction};

use crate::message::{self, Message};

/// time to wait (secs) for a request
const REQUEST_WAIT: u64 = 60;

// Maximum allowed entries in `BlockResponse`
const MAX_BLOCK_DATA_RESPONSE: u64 = 128;

/// current version of the protocol
pub(crate) const CURRENT_VERSION: u32 = 1;

#[derive(Clone)]
pub struct ProtocolConfig {
    /// Account id that runs on given machine.
    pub account_id: Option<AccountId>,
    /// Config information goes here.
    pub protocol_id: ProtocolId,
}

impl ProtocolConfig {
    pub fn new(account_id: Option<AccountId>, protocol_id: ProtocolId) -> ProtocolConfig {
        ProtocolConfig { account_id, protocol_id }
    }

    pub fn new_with_default_id(account_id: Option<AccountId>) -> ProtocolConfig {
        ProtocolConfig { account_id, protocol_id: ProtocolId::default() }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig::new(None, ProtocolId::default())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
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
    /// Optionally, Account id.
    account_id: Option<AccountId>,
}

pub struct Protocol {
    // TODO: add more fields when we need them
    pub config: ProtocolConfig,
    /// Peers that are in the handshaking process.
    handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    /// Info about peers.
    peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
    /// Info for authority peers.
    peer_account_info: RwLock<HashMap<AccountId, NodeIndex>>,
    /// Client, for read-only access.
    client: Arc<Client>,
    /// Channel into which the protocol sends the new blocks.
    block_sender: Sender<SignedBeaconBlock>,
    /// Channel into which the protocol sends the received transactions and receipts.
    transaction_sender: Sender<Transaction>,
    /// Channel into which the protocol sends the messages that should be send back to the network.
    message_sender: Sender<(NodeIndex, Message)>,
    /// Channel into which the protocol sends the gossips that should be processed by TxFlow.
    gossip_sender: Sender<Gossip<ChainPayload>>,
    /// map between authority uid and account id + public key.
    authority_map: RwLock<HashMap<UID, AuthorityStake>>,
}

impl Protocol {
    pub fn new(
        config: ProtocolConfig,
        client: Arc<Client>,
        block_sender: Sender<SignedBeaconBlock>,
        transaction_sender: Sender<Transaction>,
        message_sender: Sender<(NodeIndex, Message)>,
        gossip_sender: Sender<Gossip<ChainPayload>>,
    ) -> Self {
        Self {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
            peer_account_info: RwLock::new(HashMap::new()),
            client,
            block_sender,
            transaction_sender,
            message_sender,
            gossip_sender,
            authority_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_node_by_account_id(&self, account_id: &AccountId) -> Option<NodeIndex> {
        let peer_account_info = self.peer_account_info.read();
        peer_account_info.get(account_id).cloned()
    }

    pub fn on_peer_connected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        let best_block_header = self.client.beacon_chain.best_block().header();
        let status = message::Status {
            version: CURRENT_VERSION,
            best_index: best_block_header.index(),
            best_hash: best_block_header.block_hash(),
            genesis_hash: self.client.beacon_chain.genesis_hash,
            account_id: self.config.account_id.clone(),
        };
        debug!(target: "network", "Sending status message to {:?}: {:?}", peer, status);
        let message = Message::Status(status);
        self.send_message(peer, message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        if let Some(peer_info) = self.peer_info.read().get(&peer) {
            if let Some(account_id) = peer_info.account_id.clone() {
                self.peer_account_info.write().remove(&account_id);
            }
        }
        self.handshaking_peers.write().remove(&peer);
        self.peer_info.write().remove(&peer);
    }

    pub fn on_transaction_message(&self, transaction: Transaction) {
        let copied_tx = self.transaction_sender.clone();
        tokio::spawn(
            copied_tx
                .send(transaction)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the transactions {:?}", e)),
        );
    }

    pub fn on_gossip_message(&self, gossip: Gossip<ChainPayload>) {
        let copied_tx = self.gossip_sender.clone();
        tokio::spawn(
            copied_tx
                .send(gossip)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the gossip {:?}", e)),
        );
    }

    fn on_status_message(
        &self,
        peer: NodeIndex,
        status: &message::Status,
    ) -> Result<(), (NodeIndex, Severity)> {
        debug!(target: "network", "Status message received from {:?}: {:?}", peer, status);
        if status.version != CURRENT_VERSION {
            return Err((peer, Severity::Bad("Peer uses incompatible version.")));
        }
        if status.genesis_hash != self.client.beacon_chain.genesis_hash {
            return Err((peer, Severity::Bad("Peer has different genesis hash.")));
        }

        // request blocks to catch up if necessary
        let best_index = self.client.beacon_chain.best_index();
        let mut next_request_id = 0;
        let mut block_request = None;
        let mut request_timestamp = None;
        if status.best_index > best_index {
            let request = message::BlockRequest {
                id: next_request_id,
                from: BlockId::Number(best_index + 1),
                to: Some(BlockId::Number(status.best_index)),
                max: Some(MAX_BLOCK_DATA_RESPONSE),
            };
            block_request = Some(request.clone());
            next_request_id += 1;
            request_timestamp = Some(time::Instant::now());
            let message = Message::BlockRequest(request);
            self.send_message(peer, message);
        }

        let peer_info = PeerInfo {
            protocol_version: status.version,
            best_hash: status.best_hash,
            best_index: status.best_index,
            request_timestamp,
            block_request,
            next_request_id,
            account_id: status.account_id.clone(),
        };
        if let Some(account_id) = status.account_id.clone() {
            self.peer_account_info.write().insert(account_id, peer);
        }
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
        Ok(())
    }

    fn on_block_request(&self, peer: NodeIndex, request: message::BlockRequest) {
        let mut blocks = Vec::new();
        let mut id = request.from;
        let max = std::cmp::min(request.max.unwrap_or(u64::max_value()), MAX_BLOCK_DATA_RESPONSE);
        while let Some(block) = self.client.beacon_chain.get_block(&id) {
            blocks.push(block);
            if blocks.len() as u64 >= max {
                break;
            }
            let header = self.client.beacon_chain.get_header(&id).unwrap();
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
            id = BlockId::Number(block_index + 1);
        }
        let response = message::BlockResponse { id: request.id, blocks };
        let message = Message::BlockResponse(response);
        self.send_message(peer, message);
    }

    fn on_incoming_block(&self, block: SignedBeaconBlock) {
        let copied_tx = self.block_sender.clone();
        tokio::spawn(
            copied_tx
                .send(block)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    pub fn on_outgoing_block(&self, block: &SignedBeaconBlock) {
        let peers = self.peer_info.read();
        for peer in peers.keys() {
            let message = Message::BlockAnnounce(message::BlockAnnounce::Block(block.clone()));
            self.send_message(*peer, message);
        }
    }

    fn on_block_response(
        &self,
        peer_id: NodeIndex,
        response: message::BlockResponse<SignedBeaconBlock>,
    ) {
        let copied_tx = self.block_sender.clone();
        self.peer_info.write().entry(peer_id).and_modify(|e| e.request_timestamp = None);
        tokio::spawn(
            copied_tx
                .send_all(stream::iter_ok(response.blocks))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    pub fn on_message(&self, peer: NodeIndex, data: &[u8]) -> Result<(), (NodeIndex, Severity)> {
        let message: Message =
            Decode::decode(data).map_err(|_| (peer, Severity::Bad("Cannot decode message.")))?;

        debug!(target: "network", "message received: {:?}", message);

        match message {
            Message::Transaction(tx) => {
                self.on_transaction_message(Transaction::SignedTransaction(*tx));
            }
            Message::Receipt(receipt) => {
                self.on_transaction_message(Transaction::Receipt(*receipt));
            }
            Message::Status(status) => {
                self.on_status_message(peer, &status)?;
            }
            Message::BlockRequest(request) => self.on_block_request(peer, request),
            Message::BlockResponse(response) => {
                let request = {
                    let mut peers = self.peer_info.write();
                    let peer_info = peers
                        .get_mut(&peer)
                        .ok_or((peer, Severity::Bad("Unexpected packet received from peer")))?;
                    peer_info.block_request.take().ok_or((
                        peer,
                        Severity::Bad("Unexpected response packet received from peer"),
                    ))?
                };
                if request.id != response.id {
                    trace!(
                        target: "network",
                        "Ignoring mismatched response packet from {} (expected {} got {})",
                        peer,
                        request.id,
                        response.id
                    );
                    return Ok(());
                }
                self.on_block_response(peer, response);
            }
            Message::BlockAnnounce(ann) => {
                debug!(target: "network", "receive block announcement: {:?}", ann);
                match ann {
                    message::BlockAnnounce::Block(b) => {
                        self.on_incoming_block(b);
                    }
                    _ => unimplemented!(),
                }
            }
            Message::Gossip(gossip) => self.on_gossip_message(*gossip),
        }
        Ok(())
    }

    pub fn send_message(&self, receiver_index: NodeIndex, message: Message) {
        let copied_tx = self.message_sender.clone();
        tokio::spawn(
            copied_tx
                .send((receiver_index, message))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the message {:?}", e)),
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
            .chain(handshaking_peers.iter())
        {
            if (cur_time - *time_stamp).as_secs() > REQUEST_WAIT {
                trace!(target: "sync", "Timeout {}", *peer);
                aborting.push(*peer);
            }
        }
        aborting
    }

    pub fn set_authority_map(&self, authority_map: HashMap<UID, AuthorityStake>) {
        *self.authority_map.write() = authority_map;
    }

    pub fn get_node_index_by_uid(&self, uid: UID) -> Option<NodeIndex> {
        let auth_map = &*self.authority_map.read();
        auth_map
            .iter()
            .find_map(
                |(uid_, auth)| if uid_ == &uid { Some(auth.account_id.clone()) } else { None },
            )
            .and_then(|account_id| self.peer_account_info.read().get(&account_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    extern crate storage;

    use std::thread;
    use std::time::Duration;

    use futures::{Sink, Stream};
    use futures::sync::mpsc::channel;

    use beacon::authority::Authority;
    use beacon::types::{BeaconBlockChain, SignedBeaconBlock};
    use primitives::traits::Encode;
    use transaction::SignedTransaction;

    use crate::test_utils::{get_test_authority_config, get_test_protocol};

    use super::*;

    use self::storage::test_utils::create_memory_db;

    #[test]
    fn test_serialization() {
        let tx = SignedTransaction::empty();
        let message: Message = Message::Transaction(Box::new(tx));
        let encoded = Encode::encode(&message).unwrap();
        let decoded = Decode::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn test_authority_map() {
        let storage = Arc::new(create_memory_db());
        let genesis_block =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        // chain1
        let beacon_chain = Arc::new(BeaconBlockChain::new(genesis_block.clone(), storage.clone()));
        let authority_config = get_test_authority_config(1, 1, 1);
        let authority = Authority::new(authority_config, &beacon_chain);
        let (authority_tx, authority_rx) = channel(1024);
        let protocol = Arc::new(get_test_protocol());

        // get authorities for block 1
        let authorities = authority.get_authorities(1).unwrap();
        let authority_map: HashMap<UID, AuthorityStake> =
            authorities.into_iter().enumerate().map(|(k, v)| (k as UID, v)).collect();
        let authority_map1 = authority_map.clone();
        let protocol1 = protocol.clone();
        let task = futures::lazy(move || {
            tokio::spawn(authority_rx.for_each(move |map| {
                protocol1.set_authority_map(map);
                Ok(())
            }));
            tokio::spawn(authority_tx.send(authority_map).map(|_| ()).map_err(|_| ()));
            Ok(())
        });

        let handle = thread::spawn(move || {
            tokio::run(task);
        });
        thread::sleep(Duration::from_secs(1));
        let map = protocol.authority_map.read();
        assert_eq!(*map, authority_map1);
        std::mem::drop(handle);
    }
}
