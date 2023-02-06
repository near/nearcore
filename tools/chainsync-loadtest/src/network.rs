use crate::concurrency::{Ctx, Once, RateLimiter, Scope, WeakMap};
use log::info;

use near_async::messaging::CanSend;
use near_network::types::{
    AccountIdOrPeerTrackingShard, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, ReasonForBan, StateResponseInfo,
};
use near_network::types::{
    FullPeerInfo, NetworkInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};

use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::sharding::ChunkHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::time::Clock;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::views::FinalExecutionOutcomeView;
use nearcore::config::NearConfig;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

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
    network_adapter: PeerManagerAdapter,
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
        network_adapter: PeerManagerAdapter,
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
                    tier1_connections: vec![],
                    tier1_accounts_keys: vec![],
                    tier1_accounts_data: vec![],
                }),
                info_futures: Default::default(),
            }),
            blocks: WeakMap::new(),
            block_headers: WeakMap::new(),
            chunks: WeakMap::new(),

            min_peers: config.client_config.min_num_peers,
            parts_per_chunk: config.genesis.config.num_block_producer_seats,
            rate_limiter: RateLimiter::new(
                tokio::time::Duration::from_secs(1) / qps_limit,
                qps_limit as u64,
            ),
            request_timeout: tokio::time::Duration::from_secs(2),
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
                    self_.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                        new_req(peer.full_peer_info.clone()),
                    ));
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
}

#[async_trait::async_trait]
impl near_network::client::Client for Network {
    async fn tx_status_request(
        &self,
        _account_id: AccountId,
        _tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>> {
        None
    }

    async fn tx_status_response(&self, _tx_result: FinalExecutionOutcomeView) {}

    async fn state_request_header(
        &self,
        _shard_id: ShardId,
        _sync_hash: CryptoHash,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        Ok(None)
    }

    async fn state_request_part(
        &self,
        _shard_id: ShardId,
        _sync_hash: CryptoHash,
        _part_id: u64,
    ) -> Result<Option<StateResponseInfo>, ReasonForBan> {
        Ok(None)
    }

    async fn state_response(&self, _info: StateResponseInfo) {}

    async fn block_approval(&self, _approval: Approval, _peer_id: PeerId) {}

    async fn transaction(&self, _transaction: SignedTransaction, _is_forwarded: bool) {}

    async fn block_request(&self, _hash: CryptoHash) -> Option<Box<Block>> {
        None
    }

    async fn block_headers_request(&self, _hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        None
    }

    async fn block(&self, block: Block, _peer_id: PeerId, _was_requested: bool) {
        self.blocks.get(&block.hash().clone()).map(|p| p.set(block));
    }

    async fn block_headers(
        &self,
        headers: Vec<BlockHeader>,
        _peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        if let Some(h) = headers.iter().min_by_key(|h| h.height()) {
            let hash = h.prev_hash().clone();
            self.block_headers.get(&hash).map(|p| p.set(headers));
        }
        Ok(())
    }

    async fn challenge(&self, _challenge: Challenge) {}

    async fn network_info(&self, info: NetworkInfo) {
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

    async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        Ok(accounts.into_iter().map(|a| a.0).collect())
    }
}

impl near_network::shards_manager::ShardsManagerAdapterForNetwork for Network {
    fn process_partial_encoded_chunk(
        &self,
        _partial_encoded_chunk: near_primitives::sharding::PartialEncodedChunk,
    ) {
    }

    fn process_partial_encoded_chunk_forward(
        &self,
        _partial_encoded_chunk_forward: PartialEncodedChunkForwardMsg,
    ) {
    }

    fn process_partial_encoded_chunk_response(
        &self,
        _partial_encoded_chunk_response: PartialEncodedChunkResponseMsg,
        _received_time: std::time::Instant,
    ) {
    }

    fn process_partial_encoded_chunk_request(
        &self,
        _partial_encoded_chunk_request: PartialEncodedChunkRequestMsg,
        _route_back: CryptoHash,
    ) {
    }
}
