use crate::{NeardSubCommand, StressTestCmd};
use actix::{Actor, Addr, Arbiter, Context, Handler};
use actix_rt::ArbiterHandle;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode, RuntimeAdapter};
use near_chain_configs::GenesisConfig;
use near_network::routing::start_routing_table_actor;
use near_network::test_utils::NetworkRecipient;
use near_network::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkInfo, NetworkRequests,
    PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_network::PeerManagerActor;
use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, NetworkViewClientMessages, NetworkViewClientResponses,
    PartialEncodedChunkRequestMsg,
};
use near_primitives::block::{Block, GenesisId};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use nearcore::{init_and_migrate_store, NearConfig, NightshadeRuntime};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use strum::AsStaticRef;
use tracing::{debug, info, trace};

fn start_client(
    network_adapter: Arc<dyn PeerManagerAdapter>,
    cmd: StressTestCmd,
) -> (Addr<MockClientActor>, ArbiterHandle) {
    let client_arbiter_handle = Arbiter::current();
    let client_addr = MockClientActor::start_in_arbiter(&client_arbiter_handle, move |_ctx| {
        MockClientActor::new(network_adapter, cmd)
    });
    (client_addr, client_arbiter_handle)
}

struct MockClientActor {
    height2hash: HashMap<u64, CryptoHash>,
    network_info: NetworkInfo,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    last_x_hashes: VecDeque<CryptoHash>,
    blocks_received_since_trigger: usize,
    msg_received: usize,
    unique_hashes_since_trigger: HashSet<CryptoHash>,
    blocks_since_trigger: VecDeque<Block>,
    cmd: StressTestCmd,
}

impl MockClientActor {
    fn new(network_adapter: Arc<dyn PeerManagerAdapter>, cmd: StressTestCmd) -> Self {
        Self {
            height2hash: HashMap::new(),
            network_info: NetworkInfo {
                connected_peers: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                peer_counter: 0,
            },
            network_adapter,
            last_x_hashes: VecDeque::new(),
            blocks_received_since_trigger: 0,
            msg_received: 0,
            unique_hashes_since_trigger: HashSet::new(),
            blocks_since_trigger: VecDeque::new(),
            cmd,
        }
    }

    fn trigger_request_blocks(&mut self, ctx: &mut Context<Self>, interval: Duration) {
        match &self.cmd.subcmd {
            NeardSubCommand::Blocks(_blocks_cmd) => {
                info!(
                    msg_received = self.msg_received,
                    unique_blocks = self.unique_hashes_since_trigger.len(),
                    blocks_received = self.blocks_received_since_trigger,
                    "running trigger"
                );
                for hash in self.last_x_hashes.iter() {
                    for peer in &self.network_info.highest_height_peers {
                        self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::BlockRequest {
                                hash: hash.clone(),
                                peer_id: peer.peer_info.id.clone(),
                            },
                        ));
                    }
                }
            }
            NeardSubCommand::Chunks(_chunks_cmd) => {
                info!(
                    msg_received = self.msg_received,
                    unique_blocks = self.blocks_since_trigger.len(),
                    "running trigger"
                );
                if self.network_info.highest_height_peers.len() > 0 {
                    for b in self.blocks_since_trigger.iter() {
                        for chunk in b.chunks().iter() {
                            let request = PartialEncodedChunkRequestMsg {
                                chunk_hash: chunk.chunk_hash(),
                                part_ords: vec![0],
                                tracking_shards: HashSet::new(),
                            };
                            let target = AccountIdOrPeerTrackingShard {
                                account_id: None,
                                prefer_peer: false,
                                shard_id: chunk.shard_id(),
                                only_archival: false,
                                min_height: chunk.height_created(),
                            };

                            self.network_adapter.do_send(
                                PeerManagerMessageRequest::NetworkRequests(
                                    NetworkRequests::PartialEncodedChunkRequest { target, request },
                                ),
                            );
                        }
                    }
                }
            }
        }
        self.msg_received = 0;
        self.blocks_received_since_trigger = 0;
        self.unique_hashes_since_trigger.clear();

        near_performance_metrics::actix::run_later(ctx, interval, move |act, ctx| {
            act.trigger_request_blocks(ctx, interval);
        });
    }
}

impl Actor for MockClientActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.trigger_request_blocks(ctx, Duration::from_secs(1))
    }
}

struct MockViewClientActor {
    config: GenesisConfig,
    chain: Chain,
}

impl MockViewClientActor {
    fn new(
        config: GenesisConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
    ) -> Self {
        let chain = Chain::new_for_view_client(
            runtime_adapter.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
        )
        .unwrap();
        Self { config, chain }
    }
}

impl Actor for MockViewClientActor {
    type Context = actix::Context<Self>;
}

fn start_view_client(
    config: GenesisConfig,

    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chain_genesis: &ChainGenesis,
) -> Addr<MockViewClientActor> {
    MockViewClientActor::new(config, runtime_adapter, chain_genesis).start()
}

impl Handler<NetworkClientMessages> for MockClientActor {
    type Result = NetworkClientResponses;

    fn handle(&mut self, msg: NetworkClientMessages, _ctx: &mut Context<Self>) -> Self::Result {
        trace!(st = msg.as_static(), len = self.height2hash.len(), "MockClientActor msg");
        self.msg_received += 1;

        match msg {
            NetworkClientMessages::Block(b, _, _) => {
                let height = b.header().height();
                let hash = b.hash();
                if !self.height2hash.contains_key(&height) {
                    debug!(hash = ?b.hash(), len = self.height2hash.len() ,"MockClientActor msg block");
                    self.height2hash.insert(height, hash.clone());
                    match &self.cmd.subcmd {
                        NeardSubCommand::Blocks(blocks_cmd) => {
                            self.last_x_hashes.push_back(hash.clone());
                            if self.last_x_hashes.len() > blocks_cmd.requested_blocks_per_sec {
                                self.last_x_hashes.pop_front();
                            }
                        }
                        NeardSubCommand::Chunks(chunks_cmd) => {
                            self.blocks_since_trigger.push_back(b.clone());
                            if self.blocks_since_trigger.len() > chunks_cmd.requested_chunks_per_sec
                            {
                                self.blocks_since_trigger.pop_front();
                            }
                        }
                    }
                }
                self.blocks_received_since_trigger += 1;
                self.unique_hashes_since_trigger.insert(hash.clone());
            }
            NetworkClientMessages::BlockHeaders(_bh, _) => {}
            NetworkClientMessages::NetworkInfo(network_info) => self.network_info = network_info,
            _ => {}
        }
        NetworkClientResponses::NoResponse
    }
}

impl Handler<NetworkViewClientMessages> for MockViewClientActor {
    type Result = NetworkViewClientResponses;

    fn handle(&mut self, msg: NetworkViewClientMessages, _ctx: &mut Context<Self>) -> Self::Result {
        trace!(st = msg.as_static(), "MockViewClientActor message");
        match msg {
            NetworkViewClientMessages::GetChainInfo => NetworkViewClientResponses::ChainInfo {
                genesis_id: GenesisId {
                    chain_id: self.config.chain_id.clone(),

                    hash: *self.chain.genesis().hash(),
                },
                height: self.config.genesis_height,
                tracked_shards: Vec::new(),
                archival: false,
            },
            _ => NetworkViewClientResponses::NoResponse,
        }
    }
}

pub(crate) fn start_with_config(
    home_dir: &Path,
    config: NearConfig,
    cmd: StressTestCmd,
) -> anyhow::Result<()> {
    info!(?home_dir, "start_with_config");
    let store = init_and_migrate_store(home_dir, &config);

    let runtime = Arc::new(NightshadeRuntime::with_config(
        home_dir,
        store.clone(),
        &config,
        config.client_config.trie_viewer_state_size_limit,
        config.client_config.max_gas_burnt_view,
    ));
    let chain_genesis = ChainGenesis::from(&config.genesis);
    let network_adapter = Arc::new(NetworkRecipient::default());
    let routing_table_addr = start_routing_table_actor(
        PeerId::new(config.network_config.public_key.clone()),
        store.clone(),
    );
    let view_client = start_view_client(config.genesis.config, runtime, &chain_genesis);
    let view_client1 = view_client.clone().recipient();
    let (client_actor, _client_arbiter_handle) = start_client(network_adapter.clone(), cmd);

    let client_actor1 = client_actor.clone().recipient();
    let network_config = config.network_config;
    let arbiter = Arbiter::new();
    let network_actor = PeerManagerActor::start_in_arbiter(&arbiter.handle(), move |_ctx| {
        PeerManagerActor::new(
            store,
            network_config,
            client_actor1,
            view_client1,
            routing_table_addr,
        )
        .unwrap()
    });
    network_adapter.set_recipient(network_actor.recipient());
    Ok(())
}
