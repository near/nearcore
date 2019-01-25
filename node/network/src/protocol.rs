use std::collections::HashMap;
use std::sync::Arc;
use std::time;

use ::futures::{Future, Sink, stream};
use ::futures::sync::mpsc::Sender;
use ::parking_lot::RwLock;
use ::log::{debug, error, trace, info};

use ::beacon::types::SignedBeaconBlock;
use ::chain::{SignedBlock, SignedHeader};
use ::shard::SignedShardBlock;
use ::client::Client;
use ::primitives::hash::CryptoHash;
use ::primitives::traits::Decode;
use ::primitives::types::{
    AccountId, BlockId, Gossip, UID, AuthorityStake, PeerId
};
use ::transaction::{ChainPayload, Transaction};

use crate::message::{self, Message, BlockAnnounce};
use crate::service::Severity;

/// time to wait (secs) for a request
const REQUEST_WAIT: u64 = 60;

/// Maximum allowed entries in `BlockResponse`
const MAX_BLOCK_DATA_RESPONSE: u64 = 128;

/// current version of the protocol
pub(crate) const CURRENT_VERSION: u32 = 1;

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
    /// Information about connected peers.
    request_timestamp: Option<time::Instant>,
    /// Pending block request.
    block_request: Option<message::BlockRequest>,
    /// Next request id.
    next_request_id: u64,
}

pub struct Protocol {
    /// account id of the node running protocol
    account_id: Option<AccountId>,
    /// Peers that are in the handshaking process.
    handshaking_peers: RwLock<HashMap<PeerId, time::Instant>>,
    /// Info about peers.
    peer_info: RwLock<HashMap<PeerId, PeerInfo>>,
    /// Info for authority peers.
    peer_account_info: RwLock<HashMap<AccountId, PeerId>>,
    /// Client, for read-only access.
    client: Arc<Client>,
    /// Channel into which the protocol sends the new blocks.
    incoming_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
    /// Channel into which the protocol sends the received transactions and receipts.
    transaction_sender: Sender<Transaction>,
    /// Channel into which the protocol sends the messages that should be send back to the network.
    message_sender: Sender<Result<(PeerId, Message), (PeerId, Severity)>>,
    /// Channel into which the protocol sends the gossips that should be processed by TxFlow.
    gossip_sender: Sender<Gossip<ChainPayload>>,
}

impl Protocol {
    pub fn new(
        account_id: Option<AccountId>,
        client: Arc<Client>,
        incoming_block_tx: Sender<(SignedBeaconBlock, SignedShardBlock)>,
        transaction_sender: Sender<Transaction>,
        message_sender: Sender<Result<(PeerId, Message), (PeerId, Severity)>>,
        gossip_sender: Sender<Gossip<ChainPayload>>,
    ) -> Self {
        Self {
            account_id,
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

    pub fn get_node_by_account_id(&self, account_id: &AccountId) -> Option<PeerId> {
        let peer_account_info = self.peer_account_info.read();
        peer_account_info.get(account_id).cloned()
    }

    pub fn on_peer_connected(&self, peer: PeerId) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        let best_block_header = self.client.beacon_chain.best_block().header();
        let status = message::Status {
            version: CURRENT_VERSION,
            best_index: best_block_header.index(),
            best_hash: best_block_header.block_hash(),
            genesis_hash: self.client.beacon_chain.genesis_hash,
            account_id: self.account_id.clone(),
        };
        debug!(target: "network", "Sending status message to {:?}: {:?}", peer, status);
        let message = Message::Status(status);
        self.send_message(peer, message);
    }

    pub fn on_peer_disconnected(&self, peer: PeerId) {
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
        peer: PeerId,
        status: &message::Status,
    ) -> Result<(), (PeerId, Severity)> {
        debug!(target: "network", "Status message received from {:?}: {:?}", peer, status);
        if status.version != CURRENT_VERSION {
            return Err((peer, Severity::Bad("Peer uses incompatible version.".to_string())));
        }
        if status.genesis_hash != self.client.beacon_chain.genesis_hash {
            return Err((peer, Severity::Bad("Peer has different genesis hash.".to_string())));
        }

        // request blocks to catch up if necessary
        let best_index = self.client.beacon_chain.best_index();
        let mut next_request_id = 0;
        let mut block_request = None;
        let mut request_timestamp = None;
        if status.best_index > best_index {
            unimplemented!("Block catch-up is not implemented, yet.");
        }

        let peer_info = PeerInfo {
            protocol_version: status.version,
            best_hash: status.best_hash,
            best_index: status.best_index,
            account_id: status.account_id.clone(),
            request_timestamp,
            block_request,
            next_request_id,
        };
        if let Some(account_id) = status.account_id.clone() {
            println!("Recording account_id, peer: {}, {}", account_id, peer);
            self.peer_account_info.write().insert(account_id, peer);
        }
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
        Ok(())
    }

    fn on_block_request(&self, peer: PeerId, request: message::BlockRequest) {
//        let mut blocks = Vec::new();
//        let mut id = request.from;
//        let max = std::cmp::min(request.max.unwrap_or(u64::max_value()), MAX_BLOCK_DATA_RESPONSE);
//        while let Some(block) = self.client.beacon_chain.get_block(&id) {
//            blocks.push(block);
//            if blocks.len() as u64 >= max {
//                break;
//            }
//            let header = self.client.beacon_chain.get_header(&id).unwrap();
//            let block_index = header.index();
//            let block_hash = header.block_hash();
//            let reach_end = match request.to {
//                Some(BlockId::Number(n)) => block_index == n,
//                Some(BlockId::Hash(h)) => block_hash == h,
//                None => false,
//            };
//            if reach_end {
//                break;
//            }
//            id = BlockId::Number(block_index + 1);
//        }
//        let response = message::BlockResponse { id: request.id, blocks };
//        let message = Message::BlockResponse(response);
//        self.send_message(peer, message);
    }

    fn on_incoming_blocks(&self, blocks: BlockAnnounce) {
        let copied_tx = self.incoming_block_tx.clone();
        tokio::spawn(
            copied_tx
                .send((blocks.0, blocks.1))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
        );
    }

    pub fn on_outgoing_blocks(&self, blocks: (SignedBeaconBlock, SignedShardBlock)) {
        let peers = self.peer_info.read();
        for peer in peers.keys() {
            let message = Message::BlockAnnounce(Box::new(message::BlockAnnounce(blocks.0.clone(), blocks.1.clone())));
            self.send_message(*peer, message);
        }
    }

    fn on_block_response(
        &self,
        peer_id: PeerId,
        response: message::BlockResponse,
    ) {
//        let copied_tx = self.block_sender.clone();
//        self.peer_info.write().entry(peer_id).and_modify(|e| e.request_timestamp = None);
//        tokio::spawn(
//            copied_tx
//                .send_all(stream::iter_ok(response.blocks))
//                .map(|_| ())
//                .map_err(|e| error!("Failure to send the blocks {:?}", e)),
//        );
    }

    pub fn on_message(&self, peer: PeerId, data: &[u8]) -> Result<(), (PeerId, Severity)> {
        let message: Message = Decode::decode(data)
            .map_err(|_| (peer, Severity::Bad("Cannot decode message.".to_string())))?;
        info!(target: "network", "message received: {:?}", message);

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
            Message::BlockRequest(request) => self.on_block_request(peer, *request),
            Message::BlockResponse(response) => {
//                let request = {
//                    let mut peers = self.peer_info.write();
//                    let peer_info = peers
//                        .get_mut(&peer)
//                        .ok_or((peer, Severity::Bad("Unexpected packet received from peer".to_string())))?;
//                    peer_info.block_request.take().ok_or((
//                        peer,
//                        Severity::Bad("Unexpected response packet received from peer".to_string()),
//                    ))?
//                };
//                if request.id != response.id {
//                    trace!(
//                        target: "network",
//                        "Ignoring mismatched response packet from {} (expected {} got {})",
//                        peer,
//                        request.id,
//                        response.id
//                    );
//                    return Ok(());
//                }
//                self.on_block_response(peer, *response);
            }
            Message::BlockAnnounce(announce) => {
                debug!(target: "network", "receive block announcement: {:?}", announce);
                self.on_incoming_blocks(*announce);
            },
            Message::Gossip(gossip) => self.on_gossip_message(*gossip),
        }
        Ok(())
    }

    pub fn send_message(&self, peer: PeerId, message: Message) {
        let copied_tx = self.message_sender.clone();
        tokio::spawn(
            copied_tx
                .send(Ok((peer, message)))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the message {}", e)),
        );
    }

    pub fn report_peer(&self, peer: PeerId, severity: Severity) {
        let copied_tx = self.message_sender.clone();
        tokio::spawn(
            copied_tx
                .send(Err((peer, severity)))
                .map(|_| ())
                .map_err(|e| error!("Failure to send the message {:?}", e)),
        );
    }

    /// Returns the list of peers that have timed out.
    pub fn maintain_peers(&self) -> Vec<PeerId> {
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
                trace!(target: "network", "Timeout {}", *peer);
                aborting.push(*peer);
            }
        }
        aborting
    }

    pub fn get_peer_id_by_uid(&self, uid: UID) -> Option<PeerId> {
        let auth_map = self.client.get_recent_uid_to_authority_map();
        println!("Auth map: {:?}", auth_map);
        println!("Peer accounts: {:?}", self.peer_account_info);
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
