//! Implements `MockPeer`, which is the main component of the mock network.

use anyhow::{anyhow, Context as AnyhowContext};
use near_chain::{Block, Chain, ChainStoreAccess, Error};
use near_client::sync;
use near_crypto::{KeyType, SecretKey};
use near_network::types::{Handshake, ParsePeerMessageError, PeerMessage};
use near_network_primitives::types::{
    PartialEdgeInfo, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    PeerChainInfoV2, PeerInfo, RoutedMessageBody,
};
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{BlockHeight, ShardId};
use nearcore::config::NearConfig;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

pub mod net;
pub mod setup;
pub mod utils;

use crate::net::{MockTcpStream, MockTcpStreamHandle};

// For now this is a simple struct with one field just to leave the door
// open for adding stuff and/or having different configs for different message types later.
#[derive(Clone, Debug, Deserialize)]
pub struct MockIncomingRequestConfig {
    // How long we wait between sending each incoming request
    interval: Duration,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MockIncomingRequestsConfig {
    // Options for sending unrequested blocks
    block: Option<MockIncomingRequestConfig>,
    // Options for sending chunk part requests
    chunk_request: Option<MockIncomingRequestConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MockNetworkConfig {
    #[serde(default = "default_delay")]
    // How long we'll wait until sending replies to the client
    pub response_delay: Duration,
    pub incoming_requests: Option<MockIncomingRequestsConfig>,
}

impl MockNetworkConfig {
    pub fn with_delay(response_delay: Duration) -> Self {
        let mut ret = Self::default();
        ret.response_delay = response_delay;
        ret
    }

    pub fn from_file<P: AsRef<Path>>(path: &P) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&s)?)
    }
}

pub const MOCK_DEFAULT_NETWORK_DELAY: Duration = Duration::from_millis(100);

fn default_delay() -> Duration {
    MOCK_DEFAULT_NETWORK_DELAY
}

impl Default for MockNetworkConfig {
    fn default() -> Self {
        Self { response_delay: default_delay(), incoming_requests: None }
    }
}

#[derive(Debug)]
// Info related to unrequested messages we'll send to the client
struct IncomingRequests {
    block: Option<(Duration, Block)>,
    block_task: Option<JoinHandle<()>>,
    chunk_request: Option<(Duration, PartialEncodedChunkRequestMsg)>,
    chunk_request_task: Option<JoinHandle<()>>,
}

// get some chunk hash to serve as the source of unrequested incoming chunks.
// For now we find the first chunk hash we know about starting from the height the client will start at.
// The lower the height, the better, so that the client will actually do some work on these
// requests instead of just seeing that the chunk hash is unknown.
fn retrieve_starting_chunk_hash(
    chain: &mut Chain,
    client_start_height: BlockHeight,
    target_height: BlockHeight,
) -> anyhow::Result<ChunkHash> {
    let mut last_err = None;
    for height in client_start_height..target_height + 1 {
        match chain.store().get_any_chunk_hash_by_height_shard(height, 0) {
            Ok(hash) => return Ok(hash),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    match last_err {
        Some(e) => Err(e)
            .with_context(|| format!("Last error (retrieving chunk hash @ #{})", target_height)),
        None => Err(anyhow!("given target_height is not after the client start height?")),
    }
}

// get some block to serve as the source of unrequested incoming blocks.
fn retrieve_incoming_block(
    chain: &mut Chain,
    client_start_height: BlockHeight,
    target_height: BlockHeight,
) -> anyhow::Result<Block> {
    let mut last_err = None;
    for height in client_start_height..target_height + 1 {
        match chain.get_block_by_height(height) {
            Ok(b) => return Ok(b),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    match last_err {
        Some(e) => {
            Err(e).with_context(|| format!("Last error (retrieving block #{})", target_height))
        }
        None => Err(anyhow!("given target_height is not after the client start height?")),
    }
}

impl IncomingRequests {
    fn new(
        config: &Option<MockIncomingRequestsConfig>,
        chain: &mut Chain,
        client_start_height: BlockHeight,
        target_height: BlockHeight,
    ) -> Self {
        let mut block = None;
        let mut chunk_request = None;

        if let Some(config) = config {
            if let Some(block_config) = &config.block {
                match retrieve_incoming_block(chain, client_start_height, target_height) {
                    Ok(b) => {
                        block = Some((block_config.interval, b));
                    }
                    Err(e) => {
                        tracing::error!("Can't retrieve block suitable for mock messages: {:?}", e);
                    }
                };
            }
            if let Some(chunk_request_config) = &config.chunk_request {
                match retrieve_starting_chunk_hash(chain, client_start_height, target_height) {
                    Ok(chunk_hash) => {
                        chunk_request = Some((
                            chunk_request_config.interval,
                            PartialEncodedChunkRequestMsg {
                                chunk_hash,
                                part_ords: vec![0],
                                tracking_shards: std::iter::once(0).collect::<HashSet<_>>(),
                            },
                        ));
                    }
                    Err(e) => {
                        tracing::error!(
                            "Can't construct chunk part request suitable for mock messages: {:?}",
                            e
                        );
                    }
                };
            }
        }

        Self { block, block_task: None, chunk_request, chunk_request_task: None }
    }
}

fn retrieve_partial_encoded_chunk(
    chain: &mut Chain,
    request: &PartialEncodedChunkRequestMsg,
) -> Result<PartialEncodedChunkResponseMsg, Error> {
    let num_total_parts = chain.runtime_adapter.num_total_parts();
    let partial_chunk = chain.mut_store().get_partial_chunk(&request.chunk_hash)?;
    let present_parts: HashMap<u64, _> =
        partial_chunk.parts().iter().map(|part| (part.part_ord, part)).collect();
    assert_eq!(
        present_parts.len(),
        num_total_parts,
        "chunk {:?} doesn't have all parts",
        request.chunk_hash
    );
    let parts: Vec<_> = request
        .part_ords
        .iter()
        .map(|ord| present_parts.get(ord).cloned().cloned().unwrap())
        .collect();

    // Same process for receipts as above for parts.
    let present_receipts: HashMap<ShardId, _> =
        partial_chunk.receipts().iter().map(|receipt| (receipt.1.to_shard_id, receipt)).collect();
    let receipts: Vec<_> = request
        .tracking_shards
        .iter()
        .map(|shard_id| present_receipts.get(shard_id).cloned().cloned().unwrap())
        .collect();

    Ok(PartialEncodedChunkResponseMsg { chunk_hash: request.chunk_hash.clone(), parts, receipts })
}

struct MockConn {
    stream: MockTcpStreamHandle,
    got_proto_handshake: bool,
    got_borsh_handshake: bool,
    other_peer: Option<PeerId>,
}

/// MockPeer mocks a NEAR network peer and responds to messages from the client.
/// Instead of sending these messages out to other peers, it simulates a network and reads
/// the needed block and chunk content from storage.
/// MockPeer has the following responsibilities
/// - Responds to the requests sent from the client, including
///     BlockRequest, BlockHeadersRequest and PartialEncodedChunkRequest
/// - Simulates block production and sends the most "recent" block to the client
struct MockPeer {
    addr: SocketAddr,
    conn: Option<MockConn>,
    response_delay: Duration,
    chain_id: String,
    chain: Arc<Mutex<Chain>>,
    key: SecretKey,
    block_production_delay: Duration,
    tracked_shards: Vec<ShardId>,
    /// The simulated peers will stop producing new blocks at this height
    target_height: BlockHeight,
    network_start_height: BlockHeight,
    incoming_requests: IncomingRequests,
    send_blocks: Option<JoinHandle<()>>,
}

impl MockPeer {
    fn new(
        config: &mut NearConfig,
        mock_config: &MockNetworkConfig,
        addr: SocketAddr,
        chain: Arc<Mutex<Chain>>,
        block_production_delay: Duration,
        client_start_height: BlockHeight,
        network_start_height: BlockHeight,
        target_height: BlockHeight,
    ) -> Self {
        let key = SecretKey::from_random(KeyType::ED25519);
        config.network_config.boot_nodes.push(PeerInfo::new(PeerId::new(key.public_key()), addr));
        let incoming_requests = IncomingRequests::new(
            &mock_config.incoming_requests,
            &mut chain.lock().unwrap(),
            client_start_height,
            target_height,
        );
        Self {
            addr,
            conn: None,
            response_delay: mock_config.response_delay,
            chain: chain,
            chain_id: config.genesis.config.chain_id.clone(),
            key,
            block_production_delay,
            tracked_shards: (0..config.genesis.config.shard_layout.num_shards()).collect(),
            target_height,
            network_start_height,
            incoming_requests,
            send_blocks: None,
        }
    }

    fn is_connected(&self) -> bool {
        match &self.conn {
            // if alive() says true, it's possible that it'll become false after if neard drops the TCP stream,
            // and then we return an error when trying to connect, saying we're already connected.
            // but if it returns false, then it's definitely not possible that it'll become true after
            // since we have the lock on this MockPeer. so this is best effort kinda
            Some(c) => c.stream.alive(),
            None => false,
        }
    }

    fn connected(&mut self, stream: MockTcpStreamHandle) {
        self.conn = Some(MockConn {
            stream: stream,
            got_proto_handshake: false,
            got_borsh_handshake: false,
            other_peer: None,
        });
    }

    fn send_new_blocks(&self) -> JoinHandle<()> {
        let block_production_delay = self.block_production_delay;
        let target_height = self.target_height;
        let chain = self.chain.clone();
        let mut current_height = self.network_start_height;
        let stream = self.conn.as_ref().unwrap().stream.clone();

        actix::spawn(async move {
            let mut interval = tokio::time::interval(block_production_delay);

            loop {
                interval.tick().await;

                let s = match stream.get() {
                    Some(s) => s,
                    None => break,
                };
                if current_height <= target_height {
                    if let Ok(b) = chain.lock().unwrap().get_block_by_height(current_height) {
                        s.send_message(&PeerMessage::Block(b));
                    }
                    current_height += 1;
                } else {
                    break;
                }
            }
        })
    }

    fn send_unrequested_block(&self) -> Option<JoinHandle<()>> {
        match self.incoming_requests.block.clone() {
            Some((interval, block)) => {
                let stream = self.conn.as_ref().unwrap().stream.clone();

                Some(actix::spawn(async move {
                    let mut interval = tokio::time::interval(interval);

                    loop {
                        interval.tick().await;

                        let s = match stream.get() {
                            Some(s) => s,
                            None => break,
                        };
                        s.send_message(&PeerMessage::Block(block.clone()));
                    }
                }))
            }
            None => None,
        }
    }

    fn send_chunk_request(&self) -> Option<JoinHandle<()>> {
        match self.incoming_requests.chunk_request.clone() {
            Some((interval, req)) => {
                let conn = self.conn.as_ref().unwrap();
                let stream = conn.stream.clone();
                let key = self.key.clone();
                let target = conn.other_peer.clone().unwrap();

                Some(actix::spawn(async move {
                    let mut interval = tokio::time::interval(interval);

                    loop {
                        interval.tick().await;
                        let s = match stream.get() {
                            Some(s) => s,
                            None => break,
                        };
                        s.send_routed_message(
                            RoutedMessageBody::PartialEncodedChunkRequest(req.clone()),
                            target.clone(),
                            &key,
                        );
                    }
                }))
            }
            None => None,
        }
    }

    fn start_timers(&mut self) {
        // this doesn't seem to be documented really, but it appears this will cancel
        // the existing tasks if they're Some() and haven't already seen stream.get().is_none()
        self.send_blocks = Some(self.send_new_blocks());
        self.incoming_requests.block_task = self.send_unrequested_block();
        self.incoming_requests.chunk_request_task = self.send_chunk_request();
    }

    fn do_handshake(&mut self, stream: &MockTcpStream, other: &Handshake) {
        let mut conn = self.conn.as_mut().unwrap();
        conn.got_proto_handshake = true;
        conn.other_peer = Some(other.sender_peer_id.clone());

        let hash = self.chain.lock().unwrap().genesis().hash().clone();
        let me = PeerId::new(self.key.public_key());
        let handshake = PeerMessage::Handshake(Handshake::new(
            near_primitives::version::PROTOCOL_VERSION,
            me.clone(),
            other.sender_peer_id.clone(),
            Some(self.addr.port()),
            PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: self.chain_id.clone(), hash },
                height: self.network_start_height,
                tracked_shards: self.tracked_shards.clone(),
                archival: false,
            },
            PartialEdgeInfo::new(
                &me,
                &other.sender_peer_id,
                other.partial_edge_info.nonce,
                &self.key,
            ),
        ));
        stream.send_message(&handshake);
        self.start_timers();
    }

    fn recv(&mut self, stream: &MockTcpStream, msg: Result<PeerMessage, ParsePeerMessageError>) {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => {
                let conn = self.conn.as_mut().unwrap();
                if !conn.got_borsh_handshake {
                    conn.got_borsh_handshake = true;
                } else {
                    tracing::error!(target: "mock-node", "node has unexpectedly sent more than one borsh encoded message. Dropping it...");
                }
                return;
            }
        };

        if !self.conn.as_ref().unwrap().got_proto_handshake {
            if let PeerMessage::Handshake(h) = &msg {
                self.do_handshake(stream, h);
            } else {
                tracing::error!(target: "mock-node", "node sent something other than a handshake before sending the handshake. Dropping...: {:?}", &msg);
            }
            return;
        }

        tracing::debug!(target: "mock-node", "recv {}", &msg);
        match &msg {
            PeerMessage::Handshake(_) => {
                tracing::error!(target: "mock-node", "node unexpectedly sent more than one handshake");
                return;
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                match self.chain.lock().unwrap().retrieve_headers(
                    hashes,
                    sync::MAX_BLOCK_HEADERS,
                    Some(self.target_height),
                ) {
                    Ok(headers) => {
                        stream.send_message(&PeerMessage::BlockHeaders(headers));
                    }
                    Err(e) => {
                        tracing::error!(target: "mock-node", "Can't retrieve block headers: {:?}: {:?}", hashes, e);
                    }
                };
            }
            PeerMessage::BlockRequest(hash) => match self.chain.lock().unwrap().get_block(hash) {
                Ok(b) => {
                    if b.header().height() <= self.target_height {
                        stream.send_message(&PeerMessage::Block(b));
                    }
                }
                Err(e) => {
                    tracing::error!(target: "mock-node", "Can't retrieve block: {:?}: {:?}", hash, e);
                }
            },
            PeerMessage::Routed(r) => match &r.body {
                RoutedMessageBody::PartialEncodedChunkRequest(req) => {
                    match retrieve_partial_encoded_chunk(&mut self.chain.lock().unwrap(), req) {
                        Ok(response) => {
                            stream.send_routed_message(
                                RoutedMessageBody::PartialEncodedChunkResponse(response),
                                self.conn.as_ref().unwrap().other_peer.clone().unwrap(),
                                &self.key,
                            );
                        }
                        Err(e) => {
                            tracing::error!(target: "mock-node", "Can't construct partial encoded chunk response for: {:?}: {:?}", req, e);
                        }
                    };
                }
                RoutedMessageBody::PartialEncodedChunkResponse(_) => {}
                RoutedMessageBody::StateRequestHeader(shard, hash) => {
                    tracing::warn!(target: "mock-node",
                    "received state sync request ({:?}, {:?}) \
                    It doesn't support state sync now. Try setting start_height \
                    and target_height to be at the same epoch to avoid state sync", shard, hash);
                }
                _ => {
                    tracing::warn!(target: "mock-node", "Don't know how to handle {:?}", msg);
                }
            },
            PeerMessage::SyncRoutingTable(_) | PeerMessage::PeersRequest => {
                // ignore for now
            }
            // TODO, although not a big deal:
            // PeerMessage::Disconnect => { set things up so that read() on the socket gives EOF }
            _ => {
                tracing::warn!(target: "mock-node", "Don't know how to handle {:?}", msg);
            }
        }
    }
}

#[derive(Clone)]
struct MockNet {
    peers: Vec<Arc<Mutex<MockPeer>>>,
    used_ports: Arc<Mutex<HashSet<u16>>>,
}

impl MockNet {
    fn new(
        config: &mut NearConfig,
        chain: Chain,
        client_start_height: BlockHeight,
        network_start_height: BlockHeight,
        target_height: BlockHeight,
        block_production_delay: Duration,
        mock_config: &MockNetworkConfig,
    ) -> Self {
        let chain = Arc::new(Mutex::new(chain));
        // for now, we only simulate one peer
        // we will add more complicated network config in the future
        Self {
            peers: vec![Arc::new(Mutex::new(MockPeer::new(
                config,
                mock_config,
                "34.150.242.72:24567".parse().unwrap(),
                chain,
                block_production_delay,
                client_start_height,
                network_start_height,
                target_height,
            )))],
            used_ports: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn get_port(&self) -> io::Result<u16> {
        let mut ports = self.used_ports.lock().unwrap();
        // silly implementation but it's fine for now... Don't want to start too low
        // because 127.0.0.1:1 looks weird...
        for p in 60..=u16::MAX {
            if !ports.contains(&p) {
                ports.insert(p);
                return Ok(p);
            }
        }
        tracing::error!(target:"mock-node", "too many ports allocated!");
        Err(io::Error::new(io::ErrorKind::AddrInUse, "no ports available"))
    }

    fn release_port(&self, port: u16) {
        let mut ports = self.used_ports.lock().unwrap();
        if !ports.remove(&port) {
            tracing::warn!(target: "mock-node", "tried to release port {} which was not in use", port);
        }
    }
}
