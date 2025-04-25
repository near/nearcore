//! Implements `ChainHistoryAccess` and `MockPeerManagerActor`, which is the main
//! components of the mock network.

use anyhow::{Context as AnyhowContext, anyhow};
use near_chain::{Block, Error, retrieve_headers};
use near_client::sync::header::MAX_BLOCK_HEADERS;
use near_crypto::SecretKey;
use near_epoch_manager::EpochManagerAdapter;
use near_network::raw::{
    ConnectError, Connection, DirectMessage, Listener, Message, RoutedMessage,
};
use near_network::tcp;
use near_network::types::{PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolVersion;
use near_store::adapter::chain_store::ChainStoreAdapter;

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

pub mod setup;

// For now this is a simple struct with one field just to leave the door
// open for adding stuff and/or having different configs for different message types later.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct MockIncomingRequestConfig {
    // How long we wait between sending each incoming request
    interval: Duration,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct MockIncomingRequestsConfig {
    // Options for sending unrequested blocks
    block: Option<MockIncomingRequestConfig>,
    // Options for sending chunk part requests
    chunk_request: Option<MockIncomingRequestConfig>,
}

#[derive(Clone, Debug, serde::Deserialize)]
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

// A request we want to spam the node under test with over and over
#[derive(Debug)]
struct PeriodicRequest {
    interval: tokio::time::Interval,
    message: Message,
}

async fn next_request(r: Option<&mut PeriodicRequest>) -> Message {
    match r {
        Some(r) => {
            r.interval.tick().await;
            r.message.clone()
        }
        None => futures::future::pending().await,
    }
}

#[derive(Debug)]
// Info related to unrequested messages we'll send to the client
struct IncomingRequests {
    block: Option<PeriodicRequest>,
    chunk_request: Option<PeriodicRequest>,
}

// get some chunk hash to serve as the source of unrequested incoming chunks.
// For now we find the first chunk hash we know about starting from the height the client will start at.
// The lower the height, the better, so that the client will actually do some work on these
// requests instead of just seeing that the chunk hash is unknown.
fn retrieve_starting_chunk_hash(
    chain: &ChainStoreAdapter,
    head_height: BlockHeight,
) -> anyhow::Result<ChunkHash> {
    let mut last_err = None;
    for height in (chain.tail().context("failed fetching chain tail")? + 1..=head_height).rev() {
        match chain
            .get_block_hash_by_height(height)
            .and_then(|hash| chain.get_block(&hash))
            .map(|block| block.chunks().iter_deprecated().next().unwrap().chunk_hash())
        {
            Ok(hash) => return Ok(hash),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    match last_err {
        Some(e) => {
            Err(e).with_context(|| format!("Last error (retrieving chunk hash @ #{})", head_height))
        }
        None => Err(anyhow!("given head_height is not after the chain tail?")),
    }
}

// get the block with highest height at most `max_height`
// we make sure we start at a height that actually exists, because we want self.produce_block()
// to give the first block immediately. Otherwise the node won't even try asking us for block headers
// until we give it a block.
fn get_head_block(chain: &ChainStoreAdapter, max_height: BlockHeight) -> anyhow::Result<Block> {
    let tail = chain.tail().context("failed fetching chain tail")?;
    for height in (tail + 1..=max_height).rev() {
        let hash = match chain.get_block_hash_by_height(height) {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("get_block_hash_by_height #{} failed", height));
            }
        };
        return chain.get_block(&hash).with_context(|| format!("get_block {} failed", &hash));
    }
    anyhow::bail!("No blocks between tail #{} and head #{}", tail, max_height);
}

impl IncomingRequests {
    fn new(
        config: &Option<MockIncomingRequestsConfig>,
        chain: &ChainStoreAdapter,
        max_height_block: Block,
    ) -> Self {
        let max_height = max_height_block.header().height();
        let now = std::time::Instant::now();
        let mut block = None;
        let mut chunk_request = None;

        if let Some(config) = config {
            if let Some(block_config) = &config.block {
                block = Some(PeriodicRequest {
                    interval: tokio::time::interval_at(
                        (now + block_config.interval).into(),
                        block_config.interval,
                    ),
                    message: Message::Direct(DirectMessage::Block(max_height_block)),
                });
            }
            if let Some(chunk_request_config) = &config.chunk_request {
                match retrieve_starting_chunk_hash(chain, max_height) {
                    Ok(chunk_hash) => {
                        chunk_request = Some(PeriodicRequest {
                            interval: tokio::time::interval_at(
                                (now + chunk_request_config.interval).into(),
                                chunk_request_config.interval,
                            ),
                            message: Message::Routed(RoutedMessage::PartialEncodedChunkRequest(
                                PartialEncodedChunkRequestMsg {
                                    chunk_hash,
                                    part_ords: vec![0],
                                    tracking_shards: [ShardId::new(0)]
                                        .into_iter()
                                        .collect::<HashSet<_>>(),
                                },
                            )),
                        });
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

        Self { block, chunk_request }
    }

    // If the user told us to spam the node with incoming messages via the mock.json
    // config file, this function will produce them at the rate specified there.
    async fn next(&mut self) -> Message {
        tokio::select! {
            msg = next_request(self.block.as_mut()) => {
                msg
            }
            msg = next_request(self.chunk_request.as_mut()) => {
                msg
            }
        }
    }
}

struct InFlightMessage {
    message: Message,
    sent_at: tokio::time::Instant,
}

// type that simulates network latency by waiting for `response_delay`
// before delivering queued up messages
#[pin_project::pin_project]
struct InFlightMessages {
    #[pin]
    next_delivery: tokio::time::Sleep,
    messages: VecDeque<InFlightMessage>,
    response_delay: Duration,
}

impl InFlightMessages {
    fn new(response_delay: Duration) -> Self {
        Self {
            next_delivery: tokio::time::sleep(Duration::ZERO),
            messages: VecDeque::new(),
            response_delay,
        }
    }

    fn queue_message(self: Pin<&mut Self>, message: Message) {
        let me = self.project();
        let now = tokio::time::Instant::now();
        if me.messages.is_empty() {
            me.next_delivery.reset(now + *me.response_delay);
        }
        tracing::debug!(
            "mock peer queueing up message {} to be delivered in {:?}",
            &message,
            me.response_delay
        );
        me.messages.push_back(InFlightMessage { message, sent_at: now });
    }
}

impl Future for InFlightMessages {
    type Output = Message;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.messages.is_empty() {
            Poll::Pending
        } else {
            let mut me = self.project();
            match me.next_delivery.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    let msg = me.messages.pop_front().unwrap();
                    if let Some(m) = me.messages.front() {
                        // if there's another message after the one we're returning here, reset
                        // the time til the next message gets delivered accordingly.
                        me.next_delivery.as_mut().reset(m.sent_at + *me.response_delay);
                    }
                    Poll::Ready(msg.message)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

struct MockPeer {
    chain: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    current_height: BlockHeight,
    network_config: MockNetworkConfig,
    block_production: tokio::time::Interval,
    incoming_requests: IncomingRequests,
}

impl MockPeer {
    fn new(
        chain: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_config: MockNetworkConfig,
        block_production_delay: Duration,
        head_block: Block,
    ) -> Self {
        let current_height = head_block.header().height();
        let incoming_requests =
            IncomingRequests::new(&network_config.incoming_requests, &chain, head_block);
        Self {
            chain,
            epoch_manager,
            current_height,
            network_config,
            block_production: tokio::time::interval(block_production_delay),
            incoming_requests,
        }
    }

    // Handle the message and return a bool that tells whether we should continue
    fn handle_message(
        &self,
        conn: &Connection,
        message: std::io::Result<Message>,
        outbound: Pin<&mut InFlightMessages>,
    ) -> anyhow::Result<bool> {
        let message = match message {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    tracing::info!("{:?} disconnected", &conn);
                    return Ok(false);
                }
                return Err(e)
                    .with_context(|| format!("failed receiving message from {:?}", &conn));
            }
        };
        tracing::debug!("mock peer received message: {}", &message);
        match message {
            Message::Direct(msg) => {
                match msg {
                    DirectMessage::BlockHeadersRequest(hashes) => {
                        let headers = retrieve_headers(
                            &self.chain,
                            hashes,
                            MAX_BLOCK_HEADERS,
                            Some(self.current_height),
                        )
                        .with_context(|| {
                            format!("failed retrieving block headers up to {}", self.current_height)
                        })?;
                        outbound
                            .queue_message(Message::Direct(DirectMessage::BlockHeaders(headers)));
                    }
                    DirectMessage::BlockRequest(hash) => {
                        let block = self
                            .chain
                            .get_block(&hash)
                            .with_context(|| format!("failed getting block {}", &hash))?;
                        outbound.queue_message(Message::Direct(DirectMessage::Block(block)));
                    }
                    _ => {}
                };
            }
            Message::Routed(r) => {
                match r {
                    RoutedMessage::PartialEncodedChunkRequest(request) => {
                        let response = retrieve_partial_encoded_chunk(
                            &self.chain,
                            self.epoch_manager.as_ref(),
                            &request,
                        )
                        .with_context(|| {
                            format!(
                                "failed getting partial encoded chunk response for {:?}",
                                &request
                            )
                        })?;
                        outbound.queue_message(Message::Routed(
                            RoutedMessage::PartialEncodedChunkResponse(response),
                        ));
                    }
                    // TODO: add state sync requests to possible request types so we can either
                    // respond or just exit, saying we don't know how to do that
                    _ => {}
                }
            }
        };
        Ok(true)
    }

    // simulate the normal block production of the network by sending out a
    // "new" block at an interval set by the config's block_production_delay field
    fn produce_block(&mut self) -> anyhow::Result<Option<Block>> {
        let height = self.current_height;
        self.current_height += 1;
        let hash = match self.chain.get_block_hash_by_height(height) {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => return Ok(None),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("get_block_hash_by_height #{} failed", height));
            }
        };
        self.chain.get_block(&hash).with_context(|| format!("get_block {} failed", &hash)).map(Some)
    }

    // returns a message produced by this mock peer. Right now this includes a new block
    // at a rate given by block_production_delay in the config, and extra chunk part requests
    // and blocks as specified by the mock.json config
    async fn incoming_message(&mut self, target_height: BlockHeight) -> anyhow::Result<Message> {
        loop {
            tokio::select! {
                msg = self.incoming_requests.next() => {
                    return Ok(msg);
                }
                _ = self.block_production.tick(), if self.current_height <= target_height => {
                    if let Some(block) = self.produce_block()? {
                        return Ok(Message::Direct(DirectMessage::Block(block)));
                    }
                }
            }
        }
    }

    async fn serve_peer(
        mut self,
        mut conn: Connection,
        target_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let messages = InFlightMessages::new(self.network_config.response_delay);
        tokio::pin!(messages);

        loop {
            tokio::select! {
                res = conn.recv() => {
                    if !self.handle_message(&conn, res.map(|m| m.0), messages.as_mut())? {
                        return Ok(());
                    }
                }
                msg = &mut messages => {
                    tracing::debug!("mock peer sending message {}", &msg);
                    match msg {
                        Message::Direct(msg) => conn.send_message(msg).await?,
                        Message::Routed(msg) => conn.send_routed_message(msg, conn.peer_id().clone(), 100).await?,
                    };
                }
                msg = self.incoming_message(target_height) => {
                    let msg = msg?;
                    messages.as_mut().queue_message(msg);
                }
            }
        }
    }
}

// The mock node accepts from `listener` and starts a new MockPeer to handle
// each incoming connection
struct MockNode {
    listener: Listener,
    chain: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_start_height: BlockHeight,
    network_config: MockNetworkConfig,
    block_production_delay: Duration,
}

impl MockNode {
    fn new(
        chain: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        genesis_hash: CryptoHash,
        secret_key: SecretKey,
        listen_addr: tcp::ListenerAddr,
        chain_id: String,
        archival: bool,
        block_production_delay: Duration,
        shard_layout: ShardLayout,
        network_start_height: BlockHeight,
        network_config: MockNetworkConfig,
        handshake_protocol_version: Option<ProtocolVersion>,
    ) -> anyhow::Result<Self> {
        let listener = Listener::bind(
            listen_addr,
            secret_key,
            &chain_id,
            genesis_hash,
            network_start_height,
            shard_layout.shard_ids().collect(),
            archival,
            None,
            handshake_protocol_version,
        )?;

        Ok(Self {
            listener,
            chain,
            epoch_manager,
            network_start_height,
            network_config,
            block_production_delay,
        })
    }

    // listen on the addr passed to MockPeer::new() and wait until someone connects.
    // Then respond to messages indefinitely until an error occurs
    async fn run(mut self, target_height: BlockHeight) -> anyhow::Result<()> {
        let head_block = get_head_block(&self.chain, self.network_start_height)?;

        loop {
            let conn = match self.listener.accept().await {
                Ok(conn) => conn,
                Err(ConnectError::Accept(e)) => {
                    return Err(e).context("error accepting from TCP socket");
                }
                Err(e) => {
                    tracing::warn!("Error accepting incoming connection: {:?}", &e);
                    continue;
                }
            };

            let peer = MockPeer::new(
                self.chain.clone(),
                self.epoch_manager.clone(),
                self.network_config.clone(),
                self.block_production_delay,
                head_block.clone(),
            );

            tokio::spawn(async move {
                if let Err(e) = peer.serve_peer(conn, target_height).await {
                    tracing::error!("error serving requests: {:?}", e);
                }
            });
        }
    }
}

// TODO: this is not currently correct if we're an archival node and we get
// asked about an old chunk. In that case it needs to be reconstructed like
// in ShardsManager::prepare_partial_encoded_chunk_response()
fn retrieve_partial_encoded_chunk(
    chain: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    request: &PartialEncodedChunkRequestMsg,
) -> Result<PartialEncodedChunkResponseMsg, Error> {
    let num_total_parts = epoch_manager.num_total_parts();
    let partial_chunk = chain.get_partial_chunk(&request.chunk_hash)?;
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
    let present_receipts: HashMap<ShardId, _> = partial_chunk
        .prev_outgoing_receipts()
        .iter()
        .map(|receipt| (receipt.1.to_shard_id, receipt))
        .collect();
    let receipts: Vec<_> = request
        .tracking_shards
        .iter()
        .map(|shard_id| present_receipts.get(shard_id).cloned().cloned().unwrap())
        .collect();

    Ok(PartialEncodedChunkResponseMsg { chunk_hash: request.chunk_hash.clone(), parts, receipts })
}
