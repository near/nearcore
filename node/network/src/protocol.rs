use std::sync::Arc;

use futures::{stream, stream::Stream};
use futures::future;
use futures::Future;
use futures::sink::Sink;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::{error, info, warn, debug};

use client::Client;
use configs::NetworkConfig;
use nightshade::nightshade_task::Gossip;
use mempool::payload_gossip::PayloadGossip;
use primitives::chain::{ChainPayload, PayloadRequest, PayloadResponse, ChainState};
use primitives::network::{PeerInfo, PeerMessage, ConnectedInfo};
use primitives::serialize::{Decode, Encode};
use primitives::types::{AccountId, AuthorityId, PeerId, BlockIndex};

use crate::message::{CoupledBlock, Message, RequestId};
use crate::peer::ChainStateRetriever;
use crate::peer_manager::PeerManager;

#[derive(Clone)]
pub struct ClientChainStateRetriever {
    client: Arc<Client>,
}

impl ClientChainStateRetriever {
    pub fn new(client: Arc<Client>) -> Self {
        ClientChainStateRetriever { client }
    }
}

impl ChainStateRetriever for ClientChainStateRetriever {
    #[inline]
    fn get_chain_state(&self) -> ChainState {
        ChainState {
            genesis_hash: self.client.beacon_client.chain.genesis_hash(),
            last_index: self.client.beacon_client.chain.best_index(),
        }
    }
}

/// Protocol responsible for actual message processing from network and sending messages.
struct Protocol {
    client: Arc<Client>,
    peer_manager: Arc<PeerManager<ClientChainStateRetriever>>,
    inc_gossip_tx: Sender<Gossip>,
    inc_payload_gossip_tx: Sender<PayloadGossip>,
    inc_block_tx: Sender<(PeerId, CoupledBlock)>,
    payload_response_tx: Sender<PayloadResponse>,
    inc_chain_state_tx: Sender<(PeerId, ChainState)>,
}

impl Protocol {
    fn receive_message(&self, peer_id: PeerId, data: Vec<u8>) {
        match Decode::decode(&data) {
            Ok(m) => match m {
                Message::Connected(connected_info) => {
                    info!("Peer {} connected to {} with {:?}", peer_id, self.peer_manager.node_info.id, connected_info);
                    self.on_new_peer(peer_id, connected_info);
                }
                Message::Transaction(tx) => {
                    if let Err(e) = self.client.shard_client.pool.add_transaction(*tx) {
                        error!(target: "network", "{}", e);
                    }
                }
                Message::Receipt(receipt) => {
                    if let Err(e) = self.client.shard_client.pool.add_receipt(*receipt) {
                        error!(target: "network", "{}", e);
                    }
                }
                Message::Gossip(gossip) => forward_msg(self.inc_gossip_tx.clone(), *gossip),
                Message::PayloadGossip(gossip) => forward_msg(self.inc_payload_gossip_tx.clone(), *gossip),
                Message::BlockAnnounce(block) => {
                    forward_msg(self.inc_block_tx.clone(), (peer_id, *block));
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
                Message::BlockResponse(_request_id, mut blocks) => {
                    forward_msgs(self.inc_block_tx.clone(), blocks.drain(..).map(|b| (peer_id, b)).collect());
                }
                Message::PayloadRequest(request_id, transaction_hashes, receipt_hashes) => {
                    match self.client.fetch_payload(transaction_hashes, receipt_hashes) {
                        Ok(payload) => self.send_payload_response(&peer_id, request_id, payload),
                        Err(err) => {
                            self.peer_manager.suspect_malicious(&peer_id);
                            warn!(target: "network", "Failed to fetch payload for {} with: {}. Possible grinding attack.", peer_id, err);
                        }
                    }
                },
                Message::PayloadSnapshotRequest(request_id, hash) => {
                    if let Some(authority_id) = self.get_authority_id_from_peer_id(&peer_id) {
                        info!("Payload snapshot request from {} for {}", authority_id, hash);
                        match self.client.shard_client.pool.snapshot_request(authority_id, hash) {
                            Ok(payload) => self.send_payload_response(&peer_id, request_id, payload),
                            Err(err) => {
                                self.peer_manager.suspect_malicious(&peer_id);
                                warn!(target: "network", "Failed to fetch payload snapshot for {} with: {}. Possible grinding attack.", peer_id, err);
                            }
                        }
                    } else {
                        self.peer_manager.suspect_malicious(&peer_id);
                        warn!(target: "network", "Requesting snapshot from peer {} who is not an authority.", peer_id);
                    }
                },
                Message::PayloadResponse(_request_id, payload) => {
                    // TODO: check request id.
                    if let Some(authority_id) = self.get_authority_id_from_peer_id(&peer_id) {
                        info!("Payload response from {} / {}", peer_id, authority_id);
                        forward_msg(self.payload_response_tx.clone(), PayloadResponse::BlockProposal(authority_id, payload));
                    } else {
                        self.peer_manager.suspect_malicious(&peer_id);
                        warn!(target: "network", "Requesting snapshot from peer {} who is not an authority.", peer_id);
                    }
                }
            },
            Err(e) => warn!(target: "network", "{}", e),
        };
    }

    fn on_new_peer(&self, peer_id: PeerId, connected_info: ConnectedInfo) {
        if connected_info.chain_state.genesis_hash != self.client.beacon_client.chain.genesis_hash()
        {
            self.peer_manager.ban_peer(&peer_id);
        }
        forward_msg(self.inc_chain_state_tx.clone(), (peer_id, connected_info.chain_state));
    }

    fn get_authority_id_from_peer_id(&self, peer_id: &PeerId) -> Option<AuthorityId> {
        self.peer_manager.get_peer_info(peer_id).and_then(|peer_info| {
            peer_info.account_id.and_then(|account_id| {
                let auth_map = self.client.get_recent_uid_to_authority_map();
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

    fn get_authority_channel(&self, authority_id: AuthorityId) -> Option<Sender<PeerMessage>> {
        let auth_map = self.client.get_recent_uid_to_authority_map();
        auth_map
            .get(&authority_id)
            .map(|auth| auth.account_id.clone())
            .and_then(|account_id| self.peer_manager.get_account_channel(account_id))
    }

    fn send_gossip(&self, g: Gossip) {
        if let Some(ch) = self.get_authority_channel(g.receiver_id) {
            let data = Encode::encode(&Message::Gossip(Box::new(g))).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            debug!(target: "network", "[SND GSP] Channel for receiver_id={} not found, where account_id={:?}, sender_id={}",
                  g.receiver_id,
                  self.peer_manager.node_info.account_id,
                  g.sender_id);
        }
    }

    fn send_payload_gossip(&self, g: PayloadGossip) {
        if let Some(ch) = self.get_authority_channel(g.receiver_id) {
            let data = Encode::encode(&Message::PayloadGossip(Box::new(g))).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            debug!(target: "network", "[SND TX GSP] Channel for {} not found.", g.receiver_id);
        }
    }

    fn send_block_announce(&self, peer_ids: Vec<PeerId>, b: CoupledBlock) {
        let data = Encode::encode(&Message::BlockAnnounce(Box::new(b))).unwrap();
        for peer_id in peer_ids.iter() {
            if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
                forward_msg(ch, PeerMessage::Message(data.to_vec()));
            }
        }
    }

    fn send_block_fetch_request(&self, peer_id: &PeerId, from_index: BlockIndex, til_index: BlockIndex) {
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            // TODO: make proper request ids.
            let request_id = 1;
            let data =
                Encode::encode(&Message::BlockFetchRequest(request_id, from_index, til_index))
                    .unwrap();
            forward_msg(ch, PeerMessage::Message(data));
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
            let data = Encode::encode(&Message::BlockResponse(request_id, blocks)).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            debug!(target: "network", "[SND BLOCK RQ] Channel for {} not found, where account_id={:?}.", peer_id, self.peer_manager.node_info.account_id);
        }
    }

    fn send_payload_request(&self, request: PayloadRequest) {
        match request {
            PayloadRequest::General(_transactions, _receipts) => panic!("Not implemented"),
            PayloadRequest::BlockProposal(authority_id, hash) => {
                // TODO: make proper request ids.
                let request_id = 1;
                if let Some(ch) = self.get_authority_channel(authority_id) {
                    let data = Encode::encode(&Message::PayloadSnapshotRequest(request_id, hash)).unwrap();
                    forward_msg(ch, PeerMessage::Message(data));
                } else {
                    debug!(target: "network", "[SND PAYLOAD RQ] Channel for {} not found, account_id={:?}", authority_id, self.peer_manager.node_info.account_id);
                }
            }
        }
    }

    fn send_payload_response(
        &self,
        peer_id: &PeerId,
        request_id: RequestId,
        payload: ChainPayload,
    ) {
        info!("Send payload to {}", peer_id);
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            let data = Encode::encode(&Message::PayloadResponse(request_id, payload)).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            debug!(target: "network", "[SND PAYLOAD RSP] Channel for {} not found, account_id={:?}", peer_id, self.peer_manager.node_info.account_id);
        }
    }
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
pub fn spawn_network(
    client: Arc<Client>,
    account_id: Option<AccountId>,
    network_cfg: NetworkConfig,
    inc_gossip_tx: Sender<Gossip>,
    out_gossip_rx: Receiver<Gossip>,
    inc_block_tx: Sender<(PeerId, CoupledBlock)>,
    out_block_rx: Receiver<(Vec<PeerId>, CoupledBlock)>,
    payload_request_rx: Receiver<PayloadRequest>,
    payload_response_tx: Sender<PayloadResponse>,
    inc_payload_gossip_tx: Sender<PayloadGossip>,
    out_payload_gossip_rx: Receiver<PayloadGossip>,
    inc_chain_state_tx: Sender<(PeerId, ChainState)>,
    out_block_fetch_rx: Receiver<(PeerId, BlockIndex, BlockIndex)>,
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

    let protocol = Arc::new(Protocol { client, peer_manager, inc_gossip_tx, inc_payload_gossip_tx, inc_block_tx, payload_response_tx, inc_chain_state_tx });

    // Spawn a task that decodes incoming messages and places them in the corresponding channels.
    let protocol1 = protocol.clone();
    let task = inc_msg_rx.for_each(move |(peer_id, data)| {
        protocol1.receive_message(peer_id, data);
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing gossips.
    let protocol2 = protocol.clone();
    let task = out_gossip_rx.for_each(move |g| {
        protocol2.send_gossip(g);
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing gossips.
    let protocol_payload = protocol.clone();
    let task = out_payload_gossip_rx.for_each(move |g| {
        protocol_payload.send_payload_gossip(g);
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that encodes and sends outgoing block announcements.
    let protocol3 = protocol.clone();
    let task = out_block_rx.for_each(move |(peer_ids, b)| {
        protocol3.send_block_announce(peer_ids, b);
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that send payload requests.
    let protocol4 = protocol.clone();
    let task = payload_request_rx.for_each(move |r| {
        protocol4.send_payload_request(r);
        future::ok(())
    });
    tokio::spawn(task);

    // Spawn a task that send block fetch requests.
    let protocol5 = protocol.clone();
    let task = out_block_fetch_rx.for_each(move |(peer_id, from_index, til_index)| {
       protocol5.send_block_fetch_request(&peer_id, from_index, til_index);
        future::ok(())
    });
    tokio::spawn(task);
}

fn forward_msg<T>(ch: Sender<T>, el: T)
where
    T: Send + 'static,
{
    let task = ch
        .send(el)
        .map(|_| ())
        .map_err(|e| warn!(target: "network", "Error forwarding message: {}", e));
    tokio::spawn(task);
}

fn forward_msgs<T>(ch: Sender<T>, els: Vec<T>)
where
    T: Send + 'static,
{
    let task = ch
        .send_all(stream::iter_ok(els))
        .map(|_| ())
        .map_err(|e| warn!(target: "network", "Error forwarding messages: {}", e));
    tokio::spawn(task);
}
