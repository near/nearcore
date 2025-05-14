use crate::concurrency::{Once, RateLimiter, WeakMap};
use log::info;
use near_async::messaging::CanSend;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_async::messaging::noop;
use near_async::time;
use near_network::client::{
    AnnounceAccountRequest, BlockHeadersResponse, BlockResponse, ClientSenderForNetwork,
    SetNetworkInfo,
};
use near_network::concurrency::ctx;
use near_network::concurrency::scope;
use near_network::types::{
    AccountIdOrPeerTrackingShard, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};
use near_network::types::{
    FullPeerInfo, NetworkInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::sharding::ShardChunkHeader;
use nearcore::config::NearConfig;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    data: Arc<Mutex<NetworkData>>,

    // client_config.min_num_peers
    min_peers: usize,
    // Currently it is equivalent to genesis_config.num_block_producer_seats,
    // (see https://cs.github.com/near/nearcore/blob/dae9553670de13c279d3ebd55f17da13d94fa691/nearcore/src/runtime/mod.rs#L1114).
    // AFAICT eventually it will change dynamically (I guess it will be provided in the Block).
    parts_per_chunk: u64,

    request_timeout: time::Duration,
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
            data: Arc::new(Mutex::new(NetworkData {
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
            })),
            blocks: WeakMap::new(),
            block_headers: WeakMap::new(),
            chunks: WeakMap::new(),

            min_peers: config.client_config.min_num_peers,
            parts_per_chunk: config.genesis.config.num_block_producer_seats,
            rate_limiter: RateLimiter::new(
                time::Duration::seconds(1) / qps_limit,
                qps_limit as u64,
            ),
            request_timeout: time::Duration::seconds(2),
        })
    }

    // keep_sending() sends periodically (every self.request_timeout)
    // a NetworkRequest produced by <new_req> in an infinite loop.
    // The requests are distributed uniformly among all the available peers.
    // - keep_sending() completes as soon as ctx expires.
    // - keep_sending() respects the global rate limits, so the actual frequency
    //   of the sends may be lower than expected.
    // - keep_sending() may pause if the number of connected peers is too small.
    async fn keep_sending<'a>(
        self: &'a Arc<Self>,
        new_req: impl 'a + Fn(FullPeerInfo) -> NetworkRequests,
    ) -> anyhow::Result<()> {
        loop {
            let mut peers = self.info().await?.connected_peers.clone();
            peers.shuffle(&mut thread_rng());
            for peer in peers {
                // TODO: rate limit per peer.
                self.rate_limiter.allow().await?;
                self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(new_req(
                    peer.full_peer_info.clone(),
                )));
                self.stats.msgs_sent.fetch_add(1, Ordering::Relaxed);
                ctx::time::sleep(self.request_timeout).await?;
            }
        }
    }

    // info() fetches the state of the newest available NetworkInfo.
    // It blocks if the number of connected peers is too small.
    pub async fn info(self: &Arc<Self>) -> anyhow::Result<Arc<NetworkInfo>> {
        let (send, recv) = oneshot::channel();
        {
            let mut n = self.data.lock();
            if n.info_.num_connected_peers >= self.min_peers {
                let _ = send.send(n.info_.clone());
            } else {
                n.info_futures.push(send);
            }
        }
        anyhow::Ok(ctx::wait(recv).await??)
    }

    // fetch_block_headers fetches a batch of headers, starting with the header
    // AFTER the header with the given <hash>. The batch size is bounded by
    // sync::MAX_BLOCK_HEADERS = (currently) 512
    // https://github.com/near/nearcore/blob/ad896e767b0efbce53060dd145836bbeda3d656b/chain/client/src/sync.rs#L38
    pub async fn fetch_block_headers(
        self: &Arc<Self>,
        hash: CryptoHash,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        scope::run!(|s| async {
            self.stats.header_start.fetch_add(1, Ordering::Relaxed);
            let recv = self.block_headers.get_or_insert(&hash, || Once::new());
            s.spawn_bg(async {
                self.keep_sending(|peer| NetworkRequests::BlockHeadersRequest {
                    hashes: vec![hash],
                    peer_id: peer.peer_info.id,
                })
                .await
            });
            let res = ctx::wait(recv.wait()).await;
            self.stats.header_done.fetch_add(1, Ordering::Relaxed);
            anyhow::Ok(res?)
        })
    }

    // fetch_block() fetches a block with a given hash.
    pub async fn fetch_block(self: &Arc<Self>, hash: CryptoHash) -> anyhow::Result<Block> {
        scope::run!(|s| async {
            self.stats.block_start.fetch_add(1, Ordering::Relaxed);
            let recv = self.blocks.get_or_insert(&hash, || Once::new());
            s.spawn_bg(async {
                self.keep_sending(|peer| NetworkRequests::BlockRequest {
                    hash,
                    peer_id: peer.peer_info.id,
                })
                .await
            });
            let res = ctx::wait(recv.wait()).await;
            self.stats.block_done.fetch_add(1, Ordering::Relaxed);
            anyhow::Ok(res?)
        })
    }

    // fetch_chunk fetches a chunk for the given chunk header.
    pub async fn fetch_chunk(
        self: &Arc<Self>,
        ch: ShardChunkHeader,
    ) -> anyhow::Result<PartialEncodedChunkResponseMsg> {
        scope::run!(|s| async {
            let recv = self.chunks.get_or_insert(&ch.chunk_hash(), || Once::new());
            // TODO: consider converting wrapping these atomic counters into sth like a Span.
            self.stats.chunk_start.fetch_add(1, Ordering::Relaxed);
            s.spawn_bg(async {
                self.keep_sending(|peer| NetworkRequests::PartialEncodedChunkRequest {
                    target: AccountIdOrPeerTrackingShard {
                        account_id: peer.peer_info.account_id,
                        prefer_peer: true,
                        shard_id: ch.shard_id(),
                        only_archival: false,
                        min_height: ch.height_included(),
                    },
                    request: PartialEncodedChunkRequestMsg {
                        chunk_hash: ch.chunk_hash(),
                        part_ords: (0..self.parts_per_chunk).collect(),
                        tracking_shards: Default::default(),
                    },
                    create_time: ctx::time::now(),
                })
                .await
            });
            let res = ctx::wait(recv.wait()).await;
            self.stats.chunk_done.fetch_add(1, Ordering::Relaxed);
            anyhow::Ok(res?)
        })
    }

    pub fn as_client_adapter(&self) -> ClientSenderForNetwork {
        let blocks = self.blocks.clone();
        let block_headers = self.block_headers.clone();
        let data = self.data.clone();
        let min_peers = self.min_peers;
        ClientSenderForNetwork {
            tx_status_request: Sender::from_async_fn(|_| None),
            tx_status_response: noop().into_sender(),
            transaction: noop().into_sender(),
            state_request_header: Sender::from_async_fn(|_| None),
            state_request_part: Sender::from_async_fn(|_| None),
            state_response: noop().into_sender(),
            block_approval: noop().into_sender(),
            block_request: Sender::from_async_fn(|_| None),
            block_headers_request: Sender::from_async_fn(|_| None),
            block: Sender::from_async_fn(move |block: BlockResponse| {
                blocks.get(&block.block.hash().clone()).map(|p| p.set(block.block));
            }),
            block_headers: Sender::from_async_fn(move |headers: BlockHeadersResponse| {
                if let Some(h) = headers.0.iter().min_by_key(|h| h.height()) {
                    let hash = *h.prev_hash();
                    block_headers.get(&hash).map(|p| p.set(headers.0));
                }
                Ok(())
            }),
            network_info: Sender::from_async_fn(move |info: SetNetworkInfo| {
                let mut n = data.lock();
                n.info_ = Arc::new(info.0);
                if n.info_.num_connected_peers < min_peers {
                    info!("connected = {}/{}", n.info_.num_connected_peers, min_peers);
                    return;
                }
                for s in n.info_futures.split_off(0) {
                    s.send(n.info_.clone()).unwrap();
                }
            }),
            announce_account: Sender::from_async_fn(|accounts: AnnounceAccountRequest| {
                Ok(accounts.0.into_iter().map(|a| a.0).collect::<Vec<_>>())
            }),
            chunk_endorsement: noop().into_sender(),
            epoch_sync_request: noop().into_sender(),
            epoch_sync_response: noop().into_sender(),
            optimistic_block_receiver: noop().into_sender(),
        }
    }
}
