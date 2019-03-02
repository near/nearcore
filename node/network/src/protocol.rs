use std::sync::Arc;

use futures::{stream, stream::Stream};
use futures::future;
use futures::Future;
use futures::sink::Sink;
use futures::sync::mpsc::channel;
use futures::sync::mpsc::Receiver;
use futures::sync::mpsc::Sender;
use log::{error, info, warn};

use client::Client;
use configs::NetworkConfig;
use nightshade::nightshade_task::Gossip;
use primitives::block_traits::SignedBlock;
use primitives::chain::{ChainPayload, PayloadRequest, PayloadResponse};
use primitives::network::PeerInfo;
use primitives::serialize::{Decode, Encode};
use primitives::types::{AccountId, AuthorityId, PeerId};

use crate::message::{ChainState, ConnectedInfo, CoupledBlock, Message, RequestId};
use crate::peer::{ChainStateRetriever, PeerMessage};
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
            genesis_hash: self.client.beacon_chain.chain.genesis_hash(),
            last_index: self.client.beacon_chain.chain.best_block().index(),
        }
    }
}

/// Protocol responsible for actual message processing from network and sending messages.
struct Protocol {
    client: Arc<Client>,
    peer_manager: Arc<PeerManager<ClientChainStateRetriever>>,
    inc_gossip_tx: Sender<Gossip>,
    inc_block_tx: Sender<CoupledBlock>,
    payload_response_tx: Sender<PayloadResponse>,
}

impl Protocol {
    fn receive_message(&self, peer_id: PeerId, data: Vec<u8>) {
        match Decode::decode(&data) {
            Ok(m) => match m {
                Message::Connected(connected_info) => {
                    info!("Peer {} connected.", peer_id);
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
                Message::BlockAnnounce(block) => {
                    forward_msg(self.inc_block_tx.clone(), *block);
                }
                Message::BlockRequest(request_id, hashes) => {
                    match self.client.fetch_blocks(hashes) {
                        Ok(blocks) => self.send_block_response(&peer_id, request_id, blocks),
                        Err(err) => {
                            self.peer_manager.suspect_malicious(&peer_id);
                            warn!(target: "network", "Failed to fetch blocks from {} with {}. Possible grinding attack.", peer_id, err);
                        }
                    }
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
                Message::BlockResponse(_request_id, blocks) => {
                    forward_msgs(self.inc_block_tx.clone(), blocks);
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
                Message::PayloadAnnounce(payload) => {
                    forward_msg(self.payload_response_tx.clone(), PayloadResponse::General(payload));
                }
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
        if connected_info.chain_state.genesis_hash != self.client.beacon_chain.chain.genesis_hash()
        {
            self.peer_manager.ban_peer(&peer_id);
        }
        // Make a separate Sync agent that constantly runs and is being called from here?
        // as we really need one per shard we keeping track + beacon chain.
        if connected_info.chain_state.last_index > self.client.beacon_chain.chain.best_index() {
            info!(target: "network", "Fetching blocks {}..{} from {}",
                  self.client.beacon_chain.chain.best_index(),
                  connected_info.chain_state.last_index, peer_id);
            self.send_block_fetch_request(
                &peer_id,
                self.client.beacon_chain.chain.best_index() + 1,
                connected_info.chain_state.last_index,
            );
        }
    }

    fn get_authority_id_from_peer_id(&self, peer_id: &PeerId) -> Option<AuthorityId> {
        if let Some(peer_info) = self.peer_manager.get_peer_info(peer_id) {
            if let Some(account_id) = peer_info.account_id {
                let (_, auth_map) = self.client.get_uid_to_authority_map(1);
                auth_map.iter().find_map(|(authority_id, authority_stake)| {
                    if authority_stake.account_id == account_id {
                        Some(*authority_id as AuthorityId)
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_authority_channel(&self, authority_id: AuthorityId) -> Option<Sender<PeerMessage>> {
        // TODO: Currently it gets the same authority map for all block indices.
        // Update authority map, once block production and block importing is in place.
        //        let auth_map = self.client.get_recent_uid_to_authority_map();
        let (_, auth_map) = self.client.get_uid_to_authority_map(1);
        auth_map
            .get(&(authority_id as u64))
            .map(|auth| auth.account_id.clone())
            .and_then(|account_id| self.peer_manager.get_account_channel(account_id))
    }

    fn send_gossip(&self, g: Gossip) {
        if let Some(ch) = self.get_authority_channel(g.receiver_id) {
            let data = Encode::encode(&Message::Gossip(Box::new(g))).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            warn!(target: "network", "[SND GSP] Channel for {} not found.", g.receiver_id);
        }
    }

    fn send_block_announce(&self, b: CoupledBlock) {
        let data = Encode::encode(&Message::BlockAnnounce(Box::new(b))).unwrap();
        for ch in self.peer_manager.get_ready_channels() {
            forward_msg(ch, PeerMessage::Message(data.to_vec()));
        }
    }

    fn send_block_fetch_request(&self, peer_id: &PeerId, from_index: u64, til_index: u64) {
        if let Some(ch) = self.peer_manager.get_peer_channel(peer_id) {
            // TODO: make proper request ids.
            let request_id = 1;
            let data =
                Encode::encode(&Message::BlockFetchRequest(request_id, from_index, til_index))
                    .unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            warn!(target: "network", "[SND BLK FTCH] Channel for {} not found.", peer_id);
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
            warn!(target: "network", "[SND BLOCK RQ] Channel for {} not found.", peer_id);
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
                    warn!(target: "network", "[SND PAYLOAD RQ] Channel for {} not found", authority_id);
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
            warn!(target: "network", "[SND PAYLOAD RSP] Channel for {} not found.", peer_id);
        }
    }

    fn send_payload_announce(
        &self,
        authority_id: AuthorityId,
        payload: ChainPayload
    ) {
        if let Some(ch) = self.get_authority_channel(authority_id) {
            let data = Encode::encode(&Message::PayloadAnnounce(payload)).unwrap();
            forward_msg(ch, PeerMessage::Message(data));
        } else {
            warn!(target: "network", "[SND PLD ANC] Channel for {} not found.", authority_id);
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
    inc_block_tx: Sender<CoupledBlock>,
    out_block_rx: Receiver<CoupledBlock>,
    payload_announce_rx: Receiver<(AuthorityId, ChainPayload)>,
    payload_request_rx: Receiver<PayloadRequest>,
    payload_response_tx: Sender<PayloadResponse>,
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

    let protocol = Arc::new(Protocol { client, peer_manager, inc_gossip_tx, inc_block_tx, payload_response_tx });

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

    // Spawn a task that encodes and sends outgoing block announcements.
    let protocol3 = protocol.clone();
    let task = out_block_rx.for_each(move |b| {
        protocol3.send_block_announce(b);
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

    // Spawn a task that sends payload announces.
    let protocol5 = protocol.clone();
    let task = payload_announce_rx.for_each(move |(authority_id, payload)| {
        protocol5.send_payload_announce(authority_id, payload);
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
