use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::future;
use futures::sink::Sink;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use futures::Future;
use futures::{stream, stream::Stream};
use log::{debug, error, info, warn};
use tokio::timer::Interval;

use client::Client;
use configs::network::ProxyHandlerType;
use configs::{ClientConfig, NetworkConfig};
use mempool::payload_gossip::PayloadGossip;
use nightshade::nightshade_task::Gossip;
use primitives::block_traits::SignedBlock;
use primitives::chain::{
    ChainState, MissingPayloadResponse, PayloadRequest, PayloadResponse, Snapshot,
};
use primitives::consensus::JointBlockBLS;
use primitives::crypto::signer::{AccountSigner, BLSSigner, EDSigner};
use primitives::hash::CryptoHash;
use primitives::network::{ConnectedInfo, PeerInfo, PeerMessage};
use primitives::types::{AccountId, AuthorityId, BlockIndex, PeerId};

use crate::message::{decode_message, encode_message, CoupledBlock, Message, RequestId};
use crate::peer::ChainStateRetriever;
use crate::peer_manager::PeerManager;
use crate::proxy::debug::DebugHandler;
use crate::proxy::dropout::DropoutHandler;
use crate::proxy::{Proxy, ProxyHandler};

// Default ban period for malicious peers.
pub(crate) const PEER_BAN_PERIOD: Duration = Duration::from_secs(3600);

/// Tuple containing one single message (pointer) and one channel to send the message through.
/// Used for Proxy, they implement a stream of `SimplePackedMessage`.
pub type SimplePackedMessage = (Arc<Message>, Sender<PeerMessage>);

/// Package containing messages and channels to send results after going through proxy handlers.
pub enum PackedMessage {
    SingleMessage(Box<Message>, Sender<PeerMessage>),
    BroadcastMessage(Box<Message>, Vec<Sender<PeerMessage>>),
    MultipleMessages(Vec<Message>, Sender<PeerMessage>),
}

impl PackedMessage {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_stream(self) -> Box<Stream<Item = SimplePackedMessage, Error = ()> + Send + Sync> {
        match self {
            PackedMessage::SingleMessage(message, channel) => {
                Box::new(stream::once(Ok((Arc::new(*message), channel))))
            }

            PackedMessage::BroadcastMessage(message, channels) => {
                let pnt_message = Arc::new(*message);
                Box::new(stream::iter_ok(
                    channels.into_iter().map(move |c| (pnt_message.clone(), c)),
                ))
            }

            PackedMessage::MultipleMessages(messages, channel) => Box::new(stream::iter_ok(
                messages.into_iter().map(move |m| (Arc::new(m), channel.clone())),
            )),
        }
    }
}

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Time interval between printing connected peers.
const CONNECTED_PEERS_INT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ClientChainStateRetriever<T> {
    client: Arc<Client<T>>,
}

impl<T> ClientChainStateRetriever<T> {
    pub fn new(client: Arc<Client<T>>) -> Self {
        ClientChainStateRetriever { client }
    }
}

impl<T: Sized + Sync + Send + Clone + 'static> ChainStateRetriever
    for ClientChainStateRetriever<T>
{
    #[inline]
    fn get_chain_state(&self) -> ChainState {
        ChainState {
            genesis_hash: self.client.beacon_client.chain.genesis_hash(),
            last_index: self.client.beacon_client.chain.best_index(),
        }
    }
}

enum RequestType {
    /// Start and end index
    Block(u64, u64),
    /// Hash of the snapshot from which we request payload.
    Payload(CryptoHash),
    /// Hash of snapshot we request
    PayloadSnapshot(CryptoHash),
}

/// Protocol responsible for actual message processing from network and sending messages.
struct Protocol<T> {
    client: Arc<Client<T>>,
    peer_manager: Arc<PeerManager<ClientChainStateRetriever<T>>>,
    inc_gossip_tx: Sender<Gossip>,
    inc_payload_gossip_tx: Sender<PayloadGossip>,
    inc_block_tx: Sender<(PeerId, Vec<CoupledBlock>, BlockIndex)>,
    payload_response_tx: Sender<PayloadResponse>,
    inc_final_signatures_tx: Sender<JointBlockBLS>,
    inc_chain_state_tx: Sender<(PeerId, ChainState)>,
    requests: RwLock<HashMap<RequestId, RequestType>>,
    next_request_id: RwLock<u64>,
    proxy_messages_tx: Sender<PackedMessage>,
}

impl<T: AccountSigner + EDSigner + BLSSigner + Sized + Clone + 'static> Protocol<T> {
    fn update_requests(&self, request: RequestType) -> RequestId {
        let mut request_id_guard = self.next_request_id.write().expect(POISONED_LOCK_ERR);
        let mut guard = self.requests.write().expect(POISONED_LOCK_ERR);
        let next_request_id = *request_id_guard + 1;
        *request_id_guard += 1;
        guard.insert(next_request_id, request);
        next_request_id
    }

    #[allow(clippy::cyclomatic_complexity)]
    fn receive_message(&self, peer_id: PeerId, data: Vec<u8>) {
        let message = match decode_message(&data) {
            Ok(m) => m,
            Err(e) => {
                warn!(target: "network", "{}", e);
                return;
            }
        };
        match message {
            Message::Connected(connected_info) => {
                info!(
                    "[{:?}] Peer {} connected to {} with {:?}",
                    self.client.account_id(),
                    peer_id,
                    self.peer_manager.node_info.id,
                    connected_info
                );
                self.on_new_peer(peer_id, connected_info);
            }
            Message::Transaction(tx) => {
                if let Err(e) = self
                    .client
                    .shard_client
                    .pool
                    .clone()
                    .expect("Must have pool")
                    .write()
                    .expect(POISONED_LOCK_ERR)
                    .add_transaction(*tx)
                {
                    error!(target: "network", "{}", e);
                }
            }
            Message::Receipt(receipt) => {
                if let Err(e) = self
                    .client
                    .shard_client
                    .pool
                    .clone()
                    .expect("Must have pool")
                    .write()
                    .expect(POISONED_LOCK_ERR)
                    .add_receipt(*receipt)
                {
                    error!(target: "network", "{}", e);
                }
            }
            Message::Gossip(gossip) => forward_msg(self.inc_gossip_tx.clone(), *gossip),
            Message::PayloadGossip(gossip) => {
                forward_msg(self.inc_payload_gossip_tx.clone(), *gossip)
            }
            Message::BlockAnnounce(block) => {
                let index = block.0.index();
                forward_msg(self.inc_block_tx.clone(), (peer_id, vec![*block], index));
            }
            Message::BlockFetchRequest(request_id, from_index, til_index) => {
                match self.client.fetch_blocks_range(from_index, til_index) {
                    Ok(blocks) => self.send_block_response(&peer_id, request_id, blocks),
                    Err(err) => {
                        self.peer_manager.suspect_malicious(&peer_id);
                        warn!(target: "network", "Failed to fetch blocks from {} with {}. Possible grinding attack.", peer_id, err);
                    }
                }
            }
            Message::BlockResponse(request_id, blocks, best_index) => {
                match self.requests.read().expect(POISONED_LOCK_ERR).get(&request_id) {
                    Some(RequestType::Block(start, end)) => {
                        let from_index = blocks[0].0.index();
                        let til_index = blocks[blocks.len() - 1].0.index();
                        if *start != from_index || *end != til_index {
                            self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                            return;
                        }
                    }
                    _ => {
                        self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                        return;
                    }
                }
                self.requests.write().expect(POISONED_LOCK_ERR).remove(&request_id);
                forward_msg(self.inc_block_tx.clone(), (peer_id, blocks, best_index));
            }
            Message::PayloadRequest(request_id, missing_payload_request) => {
                match self.client.fetch_payload(missing_payload_request) {
                    Ok(response) => self.send_payload_response(&peer_id, request_id, response),
                    Err(err) => {
                        self.peer_manager.suspect_malicious(&peer_id);
                        warn!(target: "network", "Failed to retrieve payload for {}: {}", peer_id, err);
                    }
                }
            }
            Message::PayloadSnapshotRequest(request_id, hash) => {
                let block_index = self.client.shard_client.chain.best_index() + 1;
                if let Some(authority_id) =
                    self.get_authority_id_from_peer_id(block_index, &peer_id)
                {
                    info!(
                        "[{:?}], Payload snapshot request from {} for {} (block index = {})",
                        self.client.account_id(),
                        authority_id,
                        hash,
                        block_index
                    );
                    match self
                        .client
                        .shard_client
                        .pool
                        .clone()
                        .expect("Must have a pool")
                        .write()
                        .expect(POISONED_LOCK_ERR)
                        .on_snapshot_request(hash)
                    {
                        Ok(snapshot) => {
                            self.send_snapshot_response(&peer_id, request_id, snapshot);
                        }
                        Err(err) => {
                            self.peer_manager.suspect_malicious(&peer_id);
                            warn!(target: "network", "Failed to fetch payload snapshot for {} with: {}. Possible grinding attack.", peer_id, err);
                        }
                    }
                } else {
                    self.peer_manager.suspect_malicious(&peer_id);
                    let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
                    warn!(target: "network", "Requesting snapshot from peer {} who is not an authority. {:?}", peer_id, auth_map);
                }
            }
            Message::PayloadResponse(request_id, missing_payload) => {
                match self.requests.read().expect(POISONED_LOCK_ERR).get(&request_id) {
                    Some(RequestType::Payload(hash)) => {
                        // Only check the snapshot hash match here. Mempool will
                        // check whether the content match.
                        if *hash != missing_payload.snapshot_hash {
                            self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                            return;
                        }
                    }
                    _ => {
                        self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                        return;
                    }
                }
                self.requests.write().expect(POISONED_LOCK_ERR).remove(&request_id);
                let block_index = self.client.beacon_client.chain.best_index() + 1;
                if let Some(authority_id) =
                    self.get_authority_id_from_peer_id(block_index, &peer_id)
                {
                    info!(
                        "[{:?}] Payload response from {} / {}",
                        self.client.account_id(),
                        peer_id,
                        authority_id
                    );
                    forward_msg(
                        self.payload_response_tx.clone(),
                        PayloadResponse::General(authority_id, missing_payload),
                    );
                } else {
                    self.peer_manager.suspect_malicious(&peer_id);
                    let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
                    warn!(target: "network", "Requesting snapshot from peer {} who is not an authority. {:?}", peer_id, auth_map);
                }
            }
            Message::PayloadSnapshotResponse(request_id, snapshot) => {
                match self.requests.read().expect(POISONED_LOCK_ERR).get(&request_id) {
                    Some(RequestType::PayloadSnapshot(hash)) => {
                        if *hash != snapshot.get_hash() {
                            self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                            return;
                        }
                    }
                    _ => {
                        self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
                        return;
                    }
                }
                self.requests.write().expect(POISONED_LOCK_ERR).remove(&request_id);
                let block_index = self.client.beacon_client.chain.best_index() + 1;
                if let Some(authority_id) =
                    self.get_authority_id_from_peer_id(block_index, &peer_id)
                {
                    info!(
                        "[{:?}] Snapshot response from {} / {}",
                        self.client.account_id(),
                        peer_id,
                        authority_id
                    );
                    forward_msg(
                        self.payload_response_tx.clone(),
                        PayloadResponse::BlockProposal(authority_id, snapshot),
                    );
                } else {
                    self.peer_manager.suspect_malicious(&peer_id);
                    let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
                    warn!(target: "network", "Requesting snapshot from peer {} who is not an authority. {:?}", peer_id, auth_map);
                }
            }
            Message::JointBlockBLS(b) => {
                forward_msg(self.inc_final_signatures_tx.clone(), b);
            }
        }
    }

    fn on_new_peer(&self, peer_id: PeerId, connected_info: ConnectedInfo) {
        if connected_info.chain_state.genesis_hash != self.client.beacon_client.chain.genesis_hash()
        {
            self.peer_manager.ban_peer(&peer_id, PEER_BAN_PERIOD);
        }
        forward_msg(self.inc_chain_state_tx.clone(), (peer_id, connected_info.chain_state));
    }

    fn get_authority_id_from_peer_id(
        &self,
        block_index: BlockIndex,
        peer_id: &PeerId,
    ) -> Option<AuthorityId> {
        self.peer_manager.get_peer_info(peer_id).and_then(|peer_info| {
            peer_info.account_id.and_then(|account_id| {
                let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
                auth_map.iter().find_map(|(authority_id, authority_stake)| {
                    if authority_stake.account_id == account_id {
                        Some(*authority_id as AuthorityId)
                    } else {
                        None
                    }
                })
            })
        })
    }

    fn get_authority_channel(
        &self,
        block_index: BlockIndex,
        authority_id: AuthorityId,
    ) -> Option<Sender<PeerMessage>> {
        let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
        auth_map
            .get(&authority_id)
            .map(|auth| auth.account_id.clone())
            .and_then(|account_id| self.peer_manager.get_account_channel(account_id))
    }

    fn send_gossip(&self, g: Gossip) {
        if let Some(ch) = self.get_authority_channel(g.block_index, g.receiver_id) {
            let message = Message::Gossip(Box::new(g));
            self.send_single(message, ch);
        } else {
            debug!(target: "network", "[SND GSP] Channel for receiver_id={} not found, where account_id={:?}, sender_id={}, peer_manager={:?}",
                  g.receiver_id,
                  self.peer_manager.node_info.account_id,
                  g.sender_id, self.peer_manager);
        }
    }

    fn send_payload_gossip(&self, g: PayloadGossip) {
        if let Some(ch) = self.get_authority_channel(g.block_index, g.receiver_id) {
            let message = Message::PayloadGossip(Box::new(g));
            self.send_single(message, ch);
        } else {
            debug!(target: "network", "[SND TX GSP] Channel for {} not found.", g.receiver_id);
        }
    }

    fn send_block_announce(&self, peer_ids: Vec<PeerId>, b: CoupledBlock) {
        let message = Message::BlockAnnounce(Box::new(b));
        let mut channels = vec![];
        for peer_id in peer_ids.iter() {
            if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
                channels.push(ch);
            }
        }
        self.send_broadcast(message, channels);
    }

    fn send_block_fetch_request(
        &self,
        peer_id: &PeerId,
        from_index: BlockIndex,
        til_index: BlockIndex,
        max_request_num: u64,
    ) {
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            let end_index = min(from_index + max_request_num - 1, til_index);
            let request_id = self.update_requests(RequestType::Block(from_index, end_index));
            let message = Message::BlockFetchRequest(request_id, from_index, end_index);
            self.send_single(message, ch);
        } else {
            debug!(target: "network", "[SND BLK FTCH] Channel for peer_id={} not found, where account_id={:?}.", peer_id, self.peer_manager.node_info.account_id);
        }
    }

    fn send_block_response(
        &self,
        peer_id: &PeerId,
        request_id: RequestId,
        blocks: Vec<CoupledBlock>,
    ) {
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            let best_index = self.client.beacon_client.chain.best_index();
            let message = Message::BlockResponse(request_id, blocks, best_index);
            self.send_single(message, ch);
        } else {
            debug!(target: "network", "[SND BLOCK RQ] Channel for {} not found, where account_id={:?}.", peer_id, self.peer_manager.node_info.account_id);
        }
    }

    fn send_payload_request(&self, block_index: BlockIndex, request: PayloadRequest) {
        match request {
            PayloadRequest::General(authority_id, payload_request) => {
                let request_id =
                    self.update_requests(RequestType::Payload(payload_request.snapshot_hash));
                if let Some(ch) = self.get_authority_channel(block_index, authority_id) {
                    let data = encode_message(Message::PayloadRequest(request_id, payload_request))
                        .unwrap();
                    forward_msg(ch, PeerMessage::Message(data));
                } else {
                    debug!(target: "network", "[SND PAYLOAD RQ] Channel for {} not found, account_id={:?}", authority_id, self.peer_manager.node_info.account_id);
                }
            }
            PayloadRequest::BlockProposal(authority_id, hash) => {
                let request_id = self.update_requests(RequestType::PayloadSnapshot(hash));
                if let Some(ch) = self.get_authority_channel(block_index, authority_id) {
                    let message = Message::PayloadSnapshotRequest(request_id, hash);
                    self.send_single(message, ch);
                } else {
                    debug!(target: "network", "[SND PAYLOAD RQ] Channel for {} not found, account_id={:?}", authority_id, self.peer_manager.node_info.account_id);
                }
            }
        }
    }

    fn send_snapshot_response(&self, peer_id: &PeerId, request_id: RequestId, snapshot: Snapshot) {
        info!("[{:?}] Send snapshot to {}", self.client.account_id(), peer_id);
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            let data =
                encode_message(Message::PayloadSnapshotResponse(request_id, snapshot)).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            debug!(
                target: "network",
                "[SND SNAPSHOT RSP] Channel for {} not found, account_id={:?}",
                peer_id,
                self.peer_manager.node_info.account_id
            );
        }
    }

    fn send_payload_response(
        &self,
        peer_id: &PeerId,
        request_id: RequestId,
        payload: MissingPayloadResponse,
    ) {
        info!("[{:?}] Send payload to {}", self.client.account_id(), peer_id);
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            let message = Message::PayloadResponse(request_id, payload);
            self.send_single(message, ch);
        } else {
            debug!(target: "network", "[SND PAYLOAD RSP] Channel for {} not found, account_id={:?}", peer_id, self.peer_manager.node_info.account_id);
        }
    }

    fn send_broadcast(&self, message: Message, channels: Vec<Sender<PeerMessage>>) {
        let packed_message = PackedMessage::BroadcastMessage(Box::new(message), channels);
        self.send_message(packed_message);
    }

    fn send_single(&self, message: Message, channel: Sender<PeerMessage>) {
        let packed_message = PackedMessage::SingleMessage(Box::new(message), channel);
        self.send_message(packed_message);
    }

    /// Pass message through active proxies handlers and result messages are sent over channel `ch`.
    /// Take owner of `message`.
    fn send_message(&self, packed_message: PackedMessage) {
        let task = self
            .proxy_messages_tx
            .clone()
            .send(packed_message)
            .map(|_| ())
            .map_err(|e| warn!("Error sending message to proxy. {:?}", e));

        tokio_utils::spawn(task);
    }

    fn send_joint_block_bls_announce(&self, block_index: BlockIndex, b: JointBlockBLS) {
        let receiver_id = match b {
            JointBlockBLS::Request { receiver_id, .. } => receiver_id,
            JointBlockBLS::General { receiver_id, .. } => receiver_id,
        };
        if let Some(ch) = self.get_authority_channel(block_index, receiver_id) {
            let data = encode_message(Message::JointBlockBLS(b)).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            let (_, auth_map) = self.client.get_uid_to_authority_map(block_index);
            debug!(target: "network", "[SND BLS] Channel for {} not found, where account_id={:?} map={:?}", receiver_id, self.peer_manager.node_info.account_id,
                   auth_map
            );
        }
    }
}

fn get_proxy_handler(proxy_handler_type: &ProxyHandlerType) -> Arc<ProxyHandler> {
    match proxy_handler_type {
        ProxyHandlerType::Dropout(dropout_rate) => Arc::new(DropoutHandler::new(*dropout_rate)),
        ProxyHandlerType::Debug => Arc::new(DebugHandler::default()),
    }
}

/// Build Proxy and spawn using specified ProxyHandlers from NetworkConfig
/// or custom ProxyHandler for testing particular features.
///
/// At least one between `network_cfg.proxy_handlers` and `proxy_handlers` should be empty.
/// * `network_cfg.proxy_handlers` is not empty when running nodes with given proxy handlers from config.
/// * `proxy_handlers` is not empty when running particular tests.
fn spawn_proxy(
    network_cfg: NetworkConfig,
    mut handlers: Vec<Arc<ProxyHandler>>,
) -> Sender<PackedMessage> {
    // Combine passed proxies with the proxies from the config.
    for proxy_type in &network_cfg.proxy_handlers {
        handlers.push(get_proxy_handler(proxy_type));
    }

    let proxy = Proxy::new(handlers);
    let (proxy_messages_tx, proxy_messages_rx) = channel(1024);
    proxy.spawn(proxy_messages_rx);

    proxy_messages_tx
}

/// Spawn network tasks that process incoming and outgoing messages of various kind.
/// Args:
/// * `account_id`: Optional account id of the node;
/// * `network_cfg`: `NetworkConfig` object;
/// * `client`: Shared Client object which we use to get the list of authorities, and use for
///   exporting, importing blocks;
/// * `inc_gossip_tx`: Channel where protocol places incoming Nightshade gossip;
/// * `out_gossip_rx`: Channel where from protocol reads gossip that should be sent to other peers;
/// * `inc_block_tx`: Channel where protocol places incoming blocks;
/// * `out_blocks_rx`: Channel where from protocol reads blocks that should be sent for
///   announcements.
/// * `inc_final_signatures_tx`: Channel where protocol places incoming joint block BLS signatures;
/// * `out_final_signatures_rx`: Channel where from protocol reads outgoing joint block BLS signatures.
/// * `proxy_handlers`: Message are sent through proxy handlers before being sent to the network,
///   Handlers can see/drop/modify/replicate each message.
///   Note: Use empty vector when no proxy handler will be used.
pub fn spawn_network<T: AccountSigner + BLSSigner + EDSigner + Send + Sync + Clone + 'static>(
    client: Arc<Client<T>>,
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    client_cfg: ClientConfig,
    inc_gossip_tx: Sender<Gossip>,
    out_gossip_rx: Receiver<Gossip>,
    inc_block_tx: Sender<(PeerId, Vec<CoupledBlock>, BlockIndex)>,
    out_block_rx: Receiver<(Vec<PeerId>, CoupledBlock)>,
    payload_request_rx: Receiver<(BlockIndex, PayloadRequest)>,
    payload_response_tx: Sender<PayloadResponse>,
    inc_final_signatures_tx: Sender<JointBlockBLS>,
    out_final_signatures_rx: Receiver<(BlockIndex, JointBlockBLS)>,
    inc_payload_gossip_tx: Sender<PayloadGossip>,
    out_payload_gossip_rx: Receiver<PayloadGossip>,
    inc_chain_state_tx: Sender<(PeerId, ChainState)>,
    out_block_fetch_rx: Receiver<(PeerId, BlockIndex, BlockIndex)>,
    proxy_handlers: Vec<Arc<ProxyHandler>>,
) {
    let (inc_msg_tx, inc_msg_rx) = channel(1024);
    let (_, out_msg_rx) = channel(1024);

    let client_chain_state_retriever = ClientChainStateRetriever::new(client.clone());
    let peer_manager = Arc::new(PeerManager::new(
        network_cfg.reconnect_delay,
        network_cfg.gossip_interval,
        network_cfg.gossip_sample_size,
        PeerInfo { id: network_cfg.peer_id, addr: network_cfg.listen_addr, account_id },
        &network_cfg.boot_nodes,
        inc_msg_tx,
        out_msg_rx,
        client_chain_state_retriever,
    ));

    // Create proxy
    let proxy_messages_tx = spawn_proxy(network_cfg, proxy_handlers);

    let protocol = Arc::new(Protocol {
        client: client.clone(),
        peer_manager: peer_manager.clone(),
        inc_gossip_tx,
        inc_block_tx,
        payload_response_tx,
        inc_final_signatures_tx,
        inc_payload_gossip_tx,
        inc_chain_state_tx,
        requests: Default::default(),
        next_request_id: Default::default(),
        proxy_messages_tx,
    });

    // Spawn a task that decodes incoming messages and places them in the corresponding channels.
    let task = {
        let protocol = protocol.clone();
        inc_msg_rx.for_each(move |(peer_id, data)| {
            protocol.receive_message(peer_id, data);
            future::ok(())
        })
    };
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing gossips.
    let task = {
        let protocol = protocol.clone();
        out_gossip_rx.for_each(move |g| {
            protocol.send_gossip(g);
            future::ok(())
        })
    };
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing gossips.
    let task = {
        let protocol = protocol.clone();
        out_payload_gossip_rx.for_each(move |g| {
            protocol.send_payload_gossip(g);
            future::ok(())
        })
    };
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing block announcements.
    let task = {
        let protocol = protocol.clone();
        out_block_rx.for_each(move |(peer_ids, b)| {
            protocol.send_block_announce(peer_ids, b);
            future::ok(())
        })
    };
    tokio::spawn(task);

    // Spawn a task that send payload requests.
    let task = {
        let protocol = protocol.clone();
        payload_request_rx.for_each(move |(block_index, r)| {
            protocol.send_payload_request(block_index, r);
            future::ok(())
        })
    };
    tokio::spawn(task);

    // Spawn a task that send block fetch requests.
    let task = {
        let protocol = protocol.clone();
        out_block_fetch_rx.for_each(move |(peer_id, from_index, til_index)| {
            protocol.send_block_fetch_request(
                &peer_id,
                from_index,
                til_index,
                client_cfg.block_fetch_limit,
            );
            future::ok(())
        })
    };
    tokio::spawn(task);

    let task = {
        let protocol = protocol.clone();
        out_final_signatures_rx.for_each(move |(block_index, b)| {
            protocol.send_joint_block_bls_announce(block_index, b);
            future::ok(())
        })
    };
    tokio::spawn(task);

    let task = Interval::new_interval(CONNECTED_PEERS_INT)
        .for_each(move |_| {
            let (active_peers, known_peers) = peer_manager.get_peer_stats();
            info!(target: "network", "[{:?}] Peers: active = {}, known = {}", client.account_id(), active_peers, known_peers);
            future::ok(())
        })
        .map_err(|e| error!("Timer error: {}", e));

    tokio::spawn(task);
}

pub fn forward_msg<T>(ch: Sender<T>, el: T)
where
    T: Send + 'static,
{
    let task = ch
        .send(el)
        .map(|_| ())
        .map_err(|e| warn!(target: "network", "Error forwarding message: {}", e));
    tokio_utils::spawn(task);
}
