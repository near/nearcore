use std::collections::HashMap;
use std::sync::Arc;
use std::time;

use futures::{Future, Sink};
use futures::sync::mpsc::Sender;
use parking_lot::RwLock;
use substrate_network_libp2p::{NodeIndex, ProtocolId, Severity};

use beacon::types::SignedBeaconBlock;
use chain::{SignedBlock, SignedHeader};
use client::Client;
use primitives::hash::CryptoHash;
use primitives::traits::Decode;
use primitives::types::{AccountId, Gossip, UID};
use shard::SignedShardBlock;
use transaction::{ChainPayload, Transaction};

use crate::message::{self, Message};

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
    incoming_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    /// Channel into which the protocol sends the received transactions and receipts.
    transaction_sender: Sender<Transaction>,
    /// Channel into which the protocol sends the messages that should be send back to the network.
    message_sender: Sender<(NodeIndex, Message)>,
    /// Channel into which the protocol sends the gossips that should be processed by TxFlow.
    gossip_sender: Sender<Gossip<ChainPayload>>,
}

impl Protocol {
    pub fn new(
        config: ProtocolConfig,
        client: Arc<Client>,
        incoming_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
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
            incoming_block_tx,
            transaction_sender,
            message_sender,
            gossip_sender,
        }
    }

    pub fn get_node_by_account_id(&self, account_id: &AccountId) -> Option<NodeIndex> {
        let peer_account_info = self.peer_account_info.read();
        peer_account_info.get(account_id).cloned()
    }

    pub fn on_peer_connected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        let best_block_header = self.client.beacon_chain.chain.best_block().header();
        let status = message::Status {
            version: CURRENT_VERSION,
            best_index: best_block_header.index(),
            best_hash: best_block_header.block_hash(),
            genesis_hash: self.client.beacon_chain.chain.genesis_hash,
            account_id: self.config.account_id.clone(),
        };
        debug!(target: "network", "Sending status message to {:?}: {:?}", peer, status);
        let message = Message::Status(status);
        self.send_message(peer, message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        if let Some(peer_info) = self.peer_info.read().get(&peer) {
            if let Some(account_id) = peer_info.account_id.clone() {
                println!("Forgetting account_id {}", account_id);
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
        if status.genesis_hash != self.client.beacon_chain.chain.genesis_hash {
            return Err((peer, Severity::Bad("Peer has different genesis hash.")));
        }

        // request blocks to catch up if necessary
        let best_index = self.client.beacon_chain.chain.best_index();
        if status.best_index > best_index {
            unimplemented!("Block catch-up is not implemented, yet.");
        }

        let peer_info = PeerInfo {
            protocol_version: status.version,
            best_hash: status.best_hash,
            best_index: status.best_index,
            account_id: status.account_id.clone(),
        };
        if let Some(account_id) = status.account_id.clone() {
            println!("Recording account_id, peer: {}, {}", account_id, peer);
            self.peer_account_info.write().insert(account_id, peer);
        }
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
        Ok(())
    }

    fn on_incoming_blocks(&self, block: (SignedBeaconBlock, SignedShardBlock)) {
        let copied_tx = self.incoming_block_tx.clone();
        tokio::spawn(
            copied_tx
                .send(block)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    pub fn on_outgoing_blocks(&self, blocks: (SignedBeaconBlock, SignedShardBlock)) {
        let peers = self.peer_info.read();
        for peer in peers.keys() {
            let message = Message::BlockAnnounce(Box::new((blocks.0.clone(), blocks.1.clone())));
            self.send_message(*peer, message);
        }
    }

    pub fn on_message(&self, peer: NodeIndex, data: &[u8]) -> Result<(), (NodeIndex, Severity)> {
        let message: Message =
            Decode::decode(data).map_err(|_| (peer, Severity::Bad("Cannot decode message.")))?;

        debug!(target: "network", "message received: {:?}", message);
//        println!("Message received");

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
            Message::BlockAnnounce(blocks) => {
                self.on_incoming_blocks(*blocks);
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

    pub fn get_node_index_by_uid(&self, uid: UID) -> Option<NodeIndex> {
        let auth_map = self.client.get_recent_uid_to_authority_map();
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

    use primitives::traits::Encode;
    use transaction::SignedTransaction;

    use super::*;

    #[test]
    fn test_serialization() {
        let tx = SignedTransaction::empty();
        let message: Message = Message::Transaction(Box::new(tx));
        let encoded = Encode::encode(&message).unwrap();
        let decoded = Decode::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }
}
