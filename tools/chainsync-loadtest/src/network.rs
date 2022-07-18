use std::sync::atomic::{AtomicU64, Ordering};

use crate::concurrency::{Ctx, Once, RateLimiter, Scope, WeakMap};

use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, NetworkViewClientMessages, NetworkViewClientResponses,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};

use actix::{Actor, Context, Handler};
use log::info;
use near_network::types::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkInfo, NetworkRequests,
    PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::time::Clock;
use nearcore::config::NearConfig;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::time;

#[derive(Default, Debug)]
pub struct Stats {
    pub msgs_sent: AtomicU64,
    pub msgs_recv: AtomicU64,

    pub header_start: AtomicU64,
    pub header_done: AtomicU64,
    pub block_start: AtomicU64,
    pub block_done: AtomicU64,
    pub chunk_start: AtomicU64,
    pub chunk_done: AtomicU64,
}

// NetworkData contains the mutable private data of the Network struct.
// TODO: consider replacing the vector of oneshot Senders with a single
// Notify/Once.
struct NetworkData {
    info_futures: Vec<oneshot::Sender<Arc<NetworkInfo>>>,
    info_: Arc<NetworkInfo>,
}

// Network encapsulates PeerManager and exposes an async API for sending RPCs.
pub struct Network {
    pub stats: Stats,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    pub block_headers: Arc<WeakMap<CryptoHash, Once<Vec<BlockHeader>>>>,
    pub blocks: Arc<WeakMap<CryptoHash, Once<Block>>>,
    pub chunks: Arc<WeakMap<ChunkHash, Once<PartialEncodedChunkResponseMsg>>>,
    data: Mutex<NetworkData>,

    // client_config.min_num_peers
    min_peers: usize,
    // Currently it is equivalent to genesis_config.num_block_producer_seats,
    // (see https://cs.github.com/near/nearcore/blob/dae9553670de13c279d3ebd55f17da13d94fa691/nearcore/src/runtime/mod.rs#L1114).
    // AFAICT eventually it will change dynamically (I guess it will be provided in the Block).
    parts_per_chunk: u64,

    request_timeout: tokio::time::Duration,
    rate_limiter: RateLimiter,
}

impl Network {
    pub fn new(
        config: &NearConfig,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        qps_limit: u32,
    ) -> Arc<Network> {
        Arc::new(Network {
            stats: Default::default(),
            network_adapter,
            data: Mutex::new(NetworkData {
                info_: Arc::new(NetworkInfo {
                    connected_peers: vec![],
                    num_connected_peers: 0,
                    peer_max_count: 0,
                    highest_height_peers: vec![],
                    sent_bytes_per_sec: 0,
                    received_bytes_per_sec: 0,
                    known_producers: vec![],
                    peer_counter: 0,
                }),
                info_futures: Default::default(),
            }),
            blocks: WeakMap::new(),
            block_headers: WeakMap::new(),
            chunks: WeakMap::new(),

            min_peers: config.client_config.min_num_peers,
            parts_per_chunk: config.genesis.config.num_block_producer_seats,
            rate_limiter: RateLimiter::new(
                time::Duration::from_secs(1) / qps_limit,
                qps_limit as u64,
            ),
            request_timeout: time::Duration::from_secs(2),
        })
    }

    // keep_sending() sends periodically (every self.request_timeout)
    // a NetworkRequest produced by <new_req> in an infinite loop.
    // The requests are distributed uniformly among all the available peers.
    // - keep_sending() completes as soon as ctx expires.
    // - keep_sending() respects the global rate limits, so the actual frequency
    //   of the sends may be lower than expected.
    // - keep_sending() may pause if the number of connected peers is too small.
    fn keep_sending(
        self: &Arc<Self>,
        ctx: &Ctx,
        new_req: impl Fn(FullPeerInfo) -> NetworkRequests + Send,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        let self_ = self.clone();
        let ctx = ctx.with_label("keep_sending");
        async move {
            loop {
                let mut peers = self_.info(&ctx).await?.connected_peers.clone();
                peers.shuffle(&mut thread_rng());
                for peer in peers {
                    // TODO: rate limit per peer.
                    self_.rate_limiter.allow(&ctx).await?;
                    self_
                        .network_adapter
                        .do_send(PeerManagerMessageRequest::NetworkRequests(new_req(peer.clone())));
                    self_.stats.msgs_sent.fetch_add(1, Ordering::Relaxed);
                    ctx.wait(self_.request_timeout).await?;
                }
            }
        }
    }

    // info() fetches the state of the newest available NetworkInfo.
    // It blocks if the number of connected peers is too small.
    pub async fn info(self: &Arc<Self>, ctx: &Ctx) -> anyhow::Result<Arc<NetworkInfo>> {
        let ctx = ctx.clone();
        let (send, recv) = oneshot::channel();
        {
            let mut n = self.data.lock().unwrap();
            if n.info_.num_connected_peers >= self.min_peers {
                let _ = send.send(n.info_.clone());
            } else {
                n.info_futures.push(send);
            }
        }
        anyhow::Ok(ctx.wrap(recv).await??)
    }

    // fetch_block_headers fetches a batch of headers, starting with the header
    // AFTER the header with the given <hash>. The batch size is bounded by
    // sync::MAX_BLOCK_HEADERS = (currently) 512
    // https://github.com/near/nearcore/blob/ad896e767b0efbce53060dd145836bbeda3d656b/chain/client/src/sync.rs#L38
    pub async fn fetch_block_headers(
        self: &Arc<Self>,
        ctx: &Ctx,
        hash: &CryptoHash,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        Scope::run(ctx, {
            let self_ = self.clone();
            let hash = hash.clone();
            move |ctx, s| async move {
                self_.stats.header_start.fetch_add(1, Ordering::Relaxed);
                let recv = self_.block_headers.get_or_insert(&hash, || Once::new());
                s.spawn_weak(|ctx| {
                    self_.keep_sending(&ctx, move |peer| NetworkRequests::BlockHeadersRequest {
                        hashes: vec![hash.clone()],
                        peer_id: peer.peer_info.id,
                    })
                });
                let res = ctx.wrap(recv.wait()).await;
                self_.stats.header_done.fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(res?)
            }
        })
        .await
    }

    // fetch_block() fetches a block with a given hash.
    pub async fn fetch_block(
        self: &Arc<Self>,
        ctx: &Ctx,
        hash: &CryptoHash,
    ) -> anyhow::Result<Block> {
        Scope::run(ctx, {
            let self_ = self.clone();
            let hash = hash.clone();
            move |ctx, s| async move {
                self_.stats.block_start.fetch_add(1, Ordering::Relaxed);
                let recv = self_.blocks.get_or_insert(&hash, || Once::new());
                s.spawn_weak(|ctx| {
                    self_.keep_sending(&ctx, move |peer| NetworkRequests::BlockRequest {
                        hash: hash.clone(),
                        peer_id: peer.peer_info.id,
                    })
                });
                let res = ctx.wrap(recv.wait()).await;
                self_.stats.block_done.fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(res?)
            }
        })
        .await
    }

    // fetch_chunk fetches a chunk for the given chunk header.
    pub async fn fetch_chunk(
        self: &Arc<Self>,
        ctx: &Ctx,
        ch: &ShardChunkHeader,
    ) -> anyhow::Result<PartialEncodedChunkResponseMsg> {
        Scope::run(ctx, {
            let self_ = self.clone();
            let ch = ch.clone();
            move |ctx, s| async move {
                let recv = self_.chunks.get_or_insert(&ch.chunk_hash(), || Once::new());
                // TODO: consider converting wrapping these atomic counters into sth like a Span.
                self_.stats.chunk_start.fetch_add(1, Ordering::Relaxed);
                s.spawn_weak(|ctx| {
                    self_.keep_sending(&ctx, {
                        let ppc = self_.parts_per_chunk;
                        move |peer| NetworkRequests::PartialEncodedChunkRequest {
                            target: AccountIdOrPeerTrackingShard {
                                account_id: peer.peer_info.account_id,
                                prefer_peer: true,
                                shard_id: ch.shard_id(),
                                only_archival: false,
                                min_height: ch.height_included(),
                            },
                            request: PartialEncodedChunkRequestMsg {
                                chunk_hash: ch.chunk_hash(),
                                part_ords: (0..ppc).collect(),
                                tracking_shards: Default::default(),
                            },
                            create_time: Clock::instant().into(),
                        }
                    })
                });
                let res = ctx.wrap(recv.wait()).await;
                self_.stats.chunk_done.fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(res?)
            }
        })
        .await
    }

    fn notify(&self, msg: NetworkClientMessages) {
        self.stats.msgs_recv.fetch_add(1, Ordering::Relaxed);
        match msg {
            NetworkClientMessages::NetworkInfo(info) => {
                let mut n = self.data.lock().unwrap();
                n.info_ = Arc::new(info);
                if n.info_.num_connected_peers < self.min_peers {
                    info!("connected = {}/{}", n.info_.num_connected_peers, self.min_peers);
                    return;
                }
                for s in n.info_futures.split_off(0) {
                    s.send(n.info_.clone()).unwrap();
                }
            }
            NetworkClientMessages::Block(block, _, _) => {
                self.blocks.get(&block.hash().clone()).map(|p| p.set(block));
            }
            NetworkClientMessages::BlockHeaders(headers, _) => {
                if let Some(h) = headers.iter().min_by_key(|h| h.height()) {
                    let hash = h.prev_hash().clone();
                    self.block_headers.get(&hash).map(|p| p.set(headers));
                }
            }
            NetworkClientMessages::PartialEncodedChunkResponse(resp, _) => {
                self.chunks.get(&resp.chunk_hash.clone()).map(|p| p.set(resp));
            }
            _ => {}
        }
    }
}

pub struct FakeClientActor {
    network: Arc<Network>,
}

impl FakeClientActor {
    pub fn new(network: Arc<Network>) -> Self {
        FakeClientActor { network }
    }
}

impl Actor for FakeClientActor {
    type Context = Context<Self>;
}

impl Handler<NetworkViewClientMessages> for FakeClientActor {
    type Result = NetworkViewClientResponses;
    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Self::Context) -> Self::Result {
        let name = match msg {
            NetworkViewClientMessages::TxStatus { .. } => "TxStatus",
            NetworkViewClientMessages::TxStatusResponse(_) => "TxStatusResponse",
            NetworkViewClientMessages::ReceiptOutcomeRequest(_) => "ReceiptOutcomeRequest",
            NetworkViewClientMessages::ReceiptOutcomeResponse(_) => "ReceiptOutputResponse",
            NetworkViewClientMessages::BlockRequest(_) => "BlockRequest",
            NetworkViewClientMessages::BlockHeadersRequest(_) => "BlockHeadersRequest",
            NetworkViewClientMessages::StateRequestHeader { .. } => "StateRequestHeader",
            NetworkViewClientMessages::StateRequestPart { .. } => "StateRequestPart",
            NetworkViewClientMessages::EpochSyncRequest { .. } => "EpochSyncRequest",
            NetworkViewClientMessages::EpochSyncFinalizationRequest { .. } => {
                "EpochSyncFinalizationRequest"
            }
            NetworkViewClientMessages::AnnounceAccount(_) => {
                return NetworkViewClientResponses::NoResponse;
            }
            #[allow(unreachable_patterns)]
            _ => "unknown",
        };
        info!("view_request: {}", name);
        return NetworkViewClientResponses::NoResponse;
    }
}

impl Handler<NetworkClientMessages> for FakeClientActor {
    type Result = NetworkClientResponses;
    fn handle(&mut self, msg: NetworkClientMessages, _ctx: &mut Context<Self>) -> Self::Result {
        self.network.notify(msg);
        return NetworkClientResponses::NoResponse;
    }
}
