use actix::{Actor, Addr, AsyncContext, Context};
use chrono::DateTime;
use futures::{future, FutureExt};
use near_chunks::client::{ClientAdapterForShardsManager, ShardsManagerResponse};
use near_chunks::test_utils::MockClientAdapterForShardsManager;
use near_o11y::testonly::TracingCapture;
use near_primitives::time::Utc;
use num_rational::Ratio;
use once_cell::sync::OnceCell;
use rand::{thread_rng, Rng};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tracing::info;

use near_chain::test_utils::{
    wait_for_all_blocks_in_processing, wait_for_block_in_processing, KeyValueRuntime,
    ValidatorSchedule,
};
use near_chain::{
    Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Provenance, RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{
    ConnectedPeerInfo, FullPeerInfo, NetworkClientMessages, NetworkClientResponses,
    NetworkRecipient, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_network_primitives::types::PartialEdgeInfo;
use near_primitives::block::{ApprovalInner, Block, GenesisId};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{EncodedShardChunk, ReedSolomonWrapper};
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, NumBlocks, NumSeats, ShardId,
};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, QueryRequest, QueryResponseKind, StateItem,
};
use near_store::test_utils::create_test_store;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::{start_view_client, Client, ClientActor, SyncStatus, ViewClientActor};
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest, StateSplitRequest};
use near_client_primitives::types::Error;
use near_network::types::{
    NetworkInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, NetworkViewClientMessages, NetworkViewClientResponses,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerChainInfoV2, PeerInfo,
    PeerType,
};
use near_primitives::epoch_manager::RngSeed;
use near_primitives::network::PeerId;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::time::{Clock, Instant};
use near_primitives::utils::MaybeValidated;

pub struct PeerManagerMock {
    handle: Box<
        dyn FnMut(
            PeerManagerMessageRequest,
            &mut actix::Context<Self>,
        ) -> PeerManagerMessageResponse,
    >,
}

impl PeerManagerMock {
    fn new(
        f: impl 'static
            + FnMut(
                PeerManagerMessageRequest,
                &mut actix::Context<Self>,
            ) -> PeerManagerMessageResponse,
    ) -> Self {
        Self { handle: Box::new(f) }
    }
}

impl actix::Actor for PeerManagerMock {
    type Context = actix::Context<Self>;
}

impl actix::Handler<PeerManagerMessageRequest> for PeerManagerMock {
    type Result = PeerManagerMessageResponse;
    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        (self.handle)(msg, ctx)
    }
}

impl actix::Handler<SetChainInfo> for PeerManagerMock {
    type Result = ();
    fn handle(&mut self, _msg: SetChainInfo, _ctx: &mut Self::Context) {}
}

/// min block production time in milliseconds
pub const MIN_BLOCK_PROD_TIME: Duration = Duration::from_millis(100);
/// max block production time in milliseconds
pub const MAX_BLOCK_PROD_TIME: Duration = Duration::from_millis(200);

const TEST_SEED: RngSeed = [3; 32];

impl Client {
    /// Unlike Client::start_process_block, which returns before the block finishes processing
    /// This function waits until the block is processed.
    /// `should_produce_chunk`: Normally, if a block is accepted, client will try to produce
    ///                         chunks for the next block if it is the chunk producer.
    ///                         If `should_produce_chunk` is set to false, client will skip the
    ///                         chunk production. This is useful in tests that need to tweak
    ///                         the produced chunk content.
    fn process_block_sync_with_produce_chunk_options(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        should_produce_chunk: bool,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.start_process_block(block, provenance, Arc::new(|_| {}))?;
        wait_for_all_blocks_in_processing(&mut self.chain);
        let (accepted_blocks, errors) =
            self.postprocess_ready_blocks(Arc::new(|_| {}), should_produce_chunk);
        assert!(errors.is_empty(), "unexpected errors when processing blocks: {errors:#?}");
        Ok(accepted_blocks)
    }

    pub fn process_block_test(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, true)
    }

    pub fn process_block_test_no_produce_chunk(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false)
    }

    /// This function finishes processing all blocks that started being processed.
    pub fn finish_blocks_in_processing(&mut self) -> Vec<CryptoHash> {
        let mut accepted_blocks = vec![];
        while wait_for_all_blocks_in_processing(&mut self.chain) {
            accepted_blocks.extend(self.postprocess_ready_blocks(Arc::new(|_| {}), true).0);
        }
        accepted_blocks
    }

    /// This function finishes processing block with hash `hash`, if the procesing of that block
    /// has started.
    pub fn finish_block_in_processing(&mut self, hash: &CryptoHash) -> Vec<CryptoHash> {
        if let Ok(()) = wait_for_block_in_processing(&mut self.chain, hash) {
            let (accepted_blocks, errors) = self.postprocess_ready_blocks(Arc::new(|_| {}), true);
            assert!(errors.is_empty());
            return accepted_blocks;
        }
        vec![]
    }
}

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
pub fn setup(
    vs: ValidatorSchedule,
    epoch_length: BlockHeightDelta,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    epoch_sync_enabled: bool,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    transaction_validity_period: NumBlocks,
    genesis_time: DateTime<Utc>,
    ctx: &Context<ClientActor>,
) -> (Block, ClientActor, Addr<ViewClientActor>) {
    let store = create_test_store();
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let runtime =
        Arc::new(KeyValueRuntime::new_with_validators_and_no_gc(store, vs, epoch_length, archive));
    let chain_genesis = ChainGenesis {
        time: genesis_time,
        height: 0,
        gas_limit: 1_000_000,
        min_gas_price: 100,
        max_gas_price: 1_000_000_000,
        total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
        gas_price_adjustment_rate: Ratio::from_integer(0),
        transaction_validity_period,
        epoch_length,
        protocol_version: PROTOCOL_VERSION,
    };
    let doomslug_threshold_mode = if enable_doomslug {
        DoomslugThresholdMode::TwoThirds
    } else {
        DoomslugThresholdMode::NoApprovals
    };
    let chain =
        Chain::new(runtime.clone(), &chain_genesis, doomslug_threshold_mode, !archive).unwrap();
    let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();

    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));
    let telemetry = TelemetryActor::default().start();
    let config = ClientConfig::test(
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        epoch_sync_enabled,
    );

    let adv = crate::adversarial::Controls::default();

    let view_client_addr = start_view_client(
        Some(signer.validator_id().clone()),
        chain_genesis.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.clone(),
        adv.clone(),
    );

    let client = ClientActor::new(
        ctx.address(),
        config,
        chain_genesis,
        runtime,
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        network_adapter,
        Some(signer),
        telemetry,
        enable_doomslug,
        TEST_SEED,
        ctx,
        None,
        adv,
    )
    .unwrap();
    (genesis_block, client, view_client_addr)
}

pub fn setup_only_view(
    vs: ValidatorSchedule,
    epoch_length: BlockHeightDelta,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    epoch_sync_enabled: bool,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    transaction_validity_period: NumBlocks,
    genesis_time: DateTime<Utc>,
) -> Addr<ViewClientActor> {
    let store = create_test_store();
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let runtime =
        Arc::new(KeyValueRuntime::new_with_validators_and_no_gc(store, vs, epoch_length, archive));
    let chain_genesis = ChainGenesis {
        time: genesis_time,
        height: 0,
        gas_limit: 1_000_000,
        min_gas_price: 100,
        max_gas_price: 1_000_000_000,
        total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
        gas_price_adjustment_rate: Ratio::from_integer(0),
        transaction_validity_period,
        epoch_length,
        protocol_version: PROTOCOL_VERSION,
    };

    let doomslug_threshold_mode = if enable_doomslug {
        DoomslugThresholdMode::TwoThirds
    } else {
        DoomslugThresholdMode::NoApprovals
    };
    Chain::new(runtime.clone(), &chain_genesis, doomslug_threshold_mode, !archive).unwrap();

    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));
    TelemetryActor::default().start();
    let config = ClientConfig::test(
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        epoch_sync_enabled,
    );

    let adv = crate::adversarial::Controls::default();

    start_view_client(
        Some(signer.validator_id().clone()),
        chain_genesis,
        runtime,
        network_adapter.clone(),
        config,
        adv,
    )
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    peer_manager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
        ) -> PeerManagerMessageResponse,
    >,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period_and_no_epoch_sync(
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        peer_manager_mock,
        100,
    )
}

pub fn setup_mock_with_validity_period_and_no_epoch_sync(
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    mut peermanager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
        ) -> PeerManagerMessageResponse,
    >,
    transaction_validity_period: NumBlocks,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let network_adapter = Arc::new(NetworkRecipient::default());
    let mut vca: Option<Addr<ViewClientActor>> = None;
    let client_addr = ClientActor::create(|ctx: &mut Context<ClientActor>| {
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![validators]);
        let (_, client, view_client_addr) = setup(
            vs,
            5,
            account_id,
            skip_sync_wait,
            MIN_BLOCK_PROD_TIME.as_millis() as u64,
            MAX_BLOCK_PROD_TIME.as_millis() as u64,
            enable_doomslug,
            false,
            false,
            network_adapter.clone(),
            transaction_validity_period,
            Clock::utc(),
            ctx,
        );
        vca = Some(view_client_addr);
        client
    });
    let client_addr1 = client_addr.clone();

    let network_actor =
        PeerManagerMock::new(move |msg, ctx| peermanager_mock(&msg, ctx, client_addr1.clone()))
            .start();

    network_adapter.set_recipient(network_actor);

    (client_addr, vca.unwrap())
}

pub struct BlockStats {
    hash2depth: HashMap<CryptoHash, u64>,
    num_blocks: u64,
    max_chain_length: u64,
    last_check: Instant,
    max_divergence: u64,
    last_hash: Option<CryptoHash>,
    parent: HashMap<CryptoHash, CryptoHash>,
}

impl BlockStats {
    fn new() -> BlockStats {
        BlockStats {
            hash2depth: HashMap::new(),
            num_blocks: 0,
            max_chain_length: 0,
            last_check: Clock::instant(),
            max_divergence: 0,
            last_hash: None,
            parent: HashMap::new(),
        }
    }

    fn calculate_distance(&mut self, mut lhs: CryptoHash, mut rhs: CryptoHash) -> u64 {
        let mut dlhs = *self.hash2depth.get(&lhs).unwrap();
        let mut drhs = *self.hash2depth.get(&rhs).unwrap();

        let mut result: u64 = 0;
        while dlhs > drhs {
            lhs = *self.parent.get(&lhs).unwrap();
            dlhs -= 1;
            result += 1;
        }
        while dlhs < drhs {
            rhs = *self.parent.get(&rhs).unwrap();
            drhs -= 1;
            result += 1;
        }
        while lhs != rhs {
            lhs = *self.parent.get(&lhs).unwrap();
            rhs = *self.parent.get(&rhs).unwrap();
            result += 2;
        }
        result
    }

    fn add_block(&mut self, block: &Block) {
        if self.hash2depth.contains_key(block.hash()) {
            return;
        }
        let prev_height = self.hash2depth.get(block.header().prev_hash()).map(|v| *v).unwrap_or(0);
        self.hash2depth.insert(*block.hash(), prev_height + 1);
        self.num_blocks += 1;
        self.max_chain_length = max(self.max_chain_length, prev_height + 1);
        self.parent.insert(*block.hash(), *block.header().prev_hash());

        if let Some(last_hash2) = self.last_hash {
            self.max_divergence =
                max(self.max_divergence, self.calculate_distance(last_hash2, *block.hash()));
        }

        self.last_hash = Some(*block.hash());
    }

    pub fn check_stats(&mut self, force: bool) {
        let now = Clock::instant();
        let diff = now.duration_since(self.last_check);
        if !force && diff.lt(&Duration::from_secs(60)) {
            return;
        }
        self.last_check = now;
        let cur_ratio = (self.num_blocks as f64) / (max(1, self.max_chain_length) as f64);
        info!(
            "Block stats: ratio: {:.2}, num_blocks: {} max_chain_length: {} max_divergence: {}",
            cur_ratio, self.num_blocks, self.max_chain_length, self.max_divergence
        );
    }

    pub fn check_block_ratio(&mut self, min_ratio: Option<f64>, max_ratio: Option<f64>) {
        let cur_ratio = (self.num_blocks as f64) / (max(1, self.max_chain_length) as f64);
        if let Some(min_ratio2) = min_ratio {
            if cur_ratio < min_ratio2 {
                panic!(
                    "ratio of blocks to longest chain is too low got: {:.2} expected: {:.2}",
                    cur_ratio, min_ratio2
                );
            }
        }
        if let Some(max_ratio2) = max_ratio {
            if cur_ratio > max_ratio2 {
                panic!(
                    "ratio of blocks to longest chain is too high got: {:.2} expected: {:.2}",
                    cur_ratio, max_ratio2
                );
            }
        }
    }
}

fn send_chunks<T, I, F>(
    connectors: &[(Addr<ClientActor>, Addr<ViewClientActor>)],
    recipients: I,
    target: T,
    drop_chunks: bool,
    create_msg: F,
) where
    T: Eq,
    I: Iterator<Item = (usize, T)>,
    F: Fn() -> NetworkClientMessages,
{
    for (i, name) in recipients {
        if name == target {
            if !drop_chunks || !thread_rng().gen_ratio(1, 5) {
                connectors[i].0.do_send(create_msg());
            }
        }
    }
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
///
/// # Arguments
/// * `validators` - a vector or vector of validator names. Each vector is a set of validators for a
///                 particular epoch. E.g. if `validators` has three elements, then the each epoch
///                 with id % 3 == 0 will have the first set of validators, with id % 3 == 1 will
///                 have the second set of validators, and with id % 3 == 2 will have the third
/// * `key_pairs` - a flattened list of key pairs for the `validators`
/// * `validator_groups` - how many groups to split validators into. E.g. say there are four shards,
///                 and four validators in a particular epoch. If `validator_groups == 1`, all vals
///                 will validate all shards. If `validator_groups == 2`, shards 0 and 1 will have
///                 two validators validating them, and shards 2 and 3 will have the remaining two.
///                 If `validator_groups == 4`, each validator will validate a single shard
/// `skip_sync_wait`
/// `block_prod_time` - Minimum block production time, assuming there is enough approvals. The
///                 maximum block production time depends on the value of `tamper_with_fg`, and is
///                 equal to `block_prod_time` if `tamper_with_fg` is `true`, otherwise it is
///                 `block_prod_time * 2`
/// `drop_chunks` - if set to true, 10% of all the chunk messages / requests will be dropped
/// `tamper_with_fg` - if set to true, will split the heights into groups of 100. For some groups
///                 all the approvals will be dropped (thus completely disabling the finality gadget
///                 and introducing severe forkfulness if `block_prod_time` is sufficiently small),
///                 for some groups will keep all the approvals (and test the fg invariants), and
///                 for some will drop 50% of the approvals.
/// `epoch_length` - approximate length of the epoch as measured
///                 by the block heights difference of it's last and first block.
/// `network_mock` - the callback that is called for each message sent. The `mock` is called before
///                 the default processing. `mock` returns `(response, perform_default)`. If
///                 `perform_default` is false, then the message is not processed or broadcasted
///                 further and `response` is returned to the requester immediately. Otherwise
///                 the default action is performed, that might (and likely will) overwrite the
///                 `response` before it is sent back to the requester.
pub fn setup_mock_all_validators(
    vs: ValidatorSchedule,
    key_pairs: Vec<PeerInfo>,
    skip_sync_wait: bool,
    block_prod_time: u64,
    drop_chunks: bool,
    tamper_with_fg: bool,
    epoch_length: BlockHeightDelta,
    enable_doomslug: bool,
    archive: Vec<bool>,
    epoch_sync_enabled: Vec<bool>,
    check_block_stats: bool,
    peer_manager_mock: Box<
        dyn FnMut(
            &[(Addr<ClientActor>, Addr<ViewClientActor>)],
            AccountId,
            &PeerManagerMessageRequest,
        ) -> (PeerManagerMessageResponse, bool),
    >,
) -> (Block, Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>, Arc<RwLock<BlockStats>>) {
    let peer_manager_mock = Arc::new(RwLock::new(peer_manager_mock));
    let validators = vs.all_validators().cloned().collect::<Vec<_>>();
    let key_pairs = key_pairs;

    let addresses: Vec<_> = (0..key_pairs.len()).map(|i| hash(vec![i as u8].as_ref())).collect();
    let genesis_time = Clock::utc();
    let mut ret = vec![];

    let connectors: Arc<OnceCell<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
        Default::default();

    let announced_accounts = Arc::new(RwLock::new(HashSet::new()));
    let genesis_block = Arc::new(RwLock::new(None));

    let last_height = Arc::new(RwLock::new(vec![0; key_pairs.len()]));
    let largest_endorsed_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let largest_skipped_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let hash_to_height = Arc::new(RwLock::new(HashMap::new()));
    let block_stats = Arc::new(RwLock::new(BlockStats::new()));

    for (index, account_id) in validators.clone().into_iter().enumerate() {
        let vs = vs.clone();
        let block_stats1 = block_stats.clone();
        let mut view_client_addr_slot = None;
        let validators_clone2 = validators.clone();
        let genesis_block1 = genesis_block.clone();
        let key_pairs = key_pairs.clone();
        let key_pairs1 = key_pairs.clone();
        let addresses = addresses.clone();
        let connectors1 = connectors.clone();
        let network_mock1 = peer_manager_mock.clone();
        let announced_accounts1 = announced_accounts.clone();
        let last_height1 = last_height.clone();
        let last_height2 = last_height.clone();
        let largest_endorsed_height1 = largest_endorsed_height.clone();
        let largest_skipped_height1 = largest_skipped_height.clone();
        let hash_to_height1 = hash_to_height.clone();
        let archive1 = archive.clone();
        let epoch_sync_enabled1 = epoch_sync_enabled.clone();
        let client_addr = ClientActor::create(|ctx| {
            let client_addr = ctx.address();
            let _account_id = account_id.clone();
            let pm = PeerManagerMock::new(move |msg, _ctx| {
                // Note: this `.wait` will block until all `ClientActors` are created.
                let connectors1 = connectors1.wait();
                let mut guard = network_mock1.write().unwrap();
                let (resp, perform_default) = guard.deref_mut()(connectors1.as_slice(), account_id.clone(), &msg);
                drop(guard);

                if perform_default {
                    let my_ord = validators_clone2.iter().position(|it| it == &account_id).unwrap();
                    let my_key_pair = key_pairs[my_ord].clone();
                    let my_address = addresses[my_ord];

                    {
                        let last_height2 = last_height2.read().unwrap();
                        let peers: Vec<_> = key_pairs1
                            .iter()
                            .take(connectors1.len())
                            .enumerate()
                            .map(|(i, peer_info)| ConnectedPeerInfo {
                                full_peer_info: FullPeerInfo {
                                peer_info: peer_info.clone(),
                                chain_info: PeerChainInfoV2 {
                                    genesis_id: GenesisId {
                                        chain_id: "unittest".to_string(),
                                        hash: Default::default(),
                                    },
                                    height: last_height2[i],
                                    tracked_shards: vec![],
                                    archival: true,
                                },
                                partial_edge_info: PartialEdgeInfo::default(),
                            },
                                received_bytes_per_sec: 0,
                                sent_bytes_per_sec: 0,
                                last_time_peer_requested: near_network_primitives::time::Instant::now(),
                                last_time_received_message: near_network_primitives::time::Instant::now(),
                                connection_established_time: near_network_primitives::time::Instant::now(),
                                peer_type: PeerType::Outbound, })
                            .collect();
                        let peers2 = peers.iter().map(|it| it.full_peer_info.clone()).collect();
                        let info = NetworkInfo {
                            connected_peers: peers,
                            num_connected_peers: key_pairs1.len(),
                            peer_max_count: key_pairs1.len() as u32,
                            highest_height_peers: peers2,
                            sent_bytes_per_sec: 0,
                            received_bytes_per_sec: 0,
                            known_producers: vec![],
                            tier1_accounts: vec![],
                        };
                        client_addr.do_send(NetworkClientMessages::NetworkInfo(info));
                    }

                    match msg.as_network_requests_ref() {
                        NetworkRequests::Block { block } => {
                            if check_block_stats {
                                let block_stats2 = &mut *block_stats1.write().unwrap();
                                block_stats2.add_block(block);
                                block_stats2.check_stats(false);
                            }

                            for (client, _) in connectors1 {
                                client.do_send(NetworkClientMessages::Block(
                                    block.clone(),
                                    PeerInfo::random().id,
                                    false,
                                ))
                            }

                            let mut last_height1 = last_height1.write().unwrap();

                            let my_height = &mut last_height1[my_ord];

                            *my_height = max(*my_height, block.header().height());

                            hash_to_height1
                                .write()
                                .unwrap()
                                .insert(*block.header().hash(), block.header().height());
                        }
                        NetworkRequests::PartialEncodedChunkRequest { target, request , ..} => {
                            let create_msg = || {
                                NetworkClientMessages::PartialEncodedChunkRequest(
                                    request.clone(),
                                    my_address,
                                )
                            };
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().map(|s| Some(s.clone())).enumerate(),
                                target.account_id.as_ref().map(|s| s.clone()),
                                drop_chunks,
                                create_msg,
                            );
                        }
                        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                            let create_msg = || {
                                NetworkClientMessages::PartialEncodedChunkResponse(response.clone(), Clock::instant())
                            };
                            send_chunks(
                                connectors1,
                                addresses.iter().enumerate(),
                                route_back,
                                drop_chunks,
                                create_msg,
                            );
                        }
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        } => {
                            let create_msg = || {
                                NetworkClientMessages::PartialEncodedChunk(
                                    partial_encoded_chunk.clone().into(),
                                )
                            };
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().cloned().enumerate(),
                                account_id.clone(),
                                drop_chunks,
                                create_msg,
                            );
                        }
                        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                            let create_msg = || {
                                NetworkClientMessages::PartialEncodedChunkForward(forward.clone())
                            };
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().cloned().enumerate(),
                                account_id.clone(),
                                drop_chunks,
                                create_msg,
                            );
                        }
                        NetworkRequests::BlockRequest { hash, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::BlockRequest(*hash))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::Block(block) => {
                                                        me.do_send(NetworkClientMessages::Block(*block, peer_id, true));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::EpochSyncRequest { epoch_id, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::EpochSyncRequest{
                                                epoch_id: epoch_id.clone(),
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::EpochSyncResponse(response) => {
                                                        me.do_send(NetworkClientMessages::EpochSyncResponse(peer_id, response));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::EpochSyncFinalizationRequest { epoch_id, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::EpochSyncFinalizationRequest{
                                                epoch_id: epoch_id.clone(),
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::EpochSyncFinalizationResponse(response) => {
                                                        me.do_send(NetworkClientMessages::EpochSyncFinalizationResponse(peer_id, response));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::BlockHeadersRequest(
                                                hashes.clone(),
                                            ))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::BlockHeaders(
                                                        headers,
                                                    ) => {
                                                        me.do_send(NetworkClientMessages::BlockHeaders(headers, peer_id));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateRequestHeader {
                            shard_id,
                            sync_hash,
                            target: target_account_id,
                        } => {
                            let target_account_id = match target_account_id {
                                AccountOrPeerIdOrHash::AccountId(x) => x,
                                _ => panic!(),
                            };
                            for (i, name) in validators_clone2.iter().enumerate() {
                                if name == target_account_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::StateRequestHeader {
                                                shard_id: *shard_id,
                                                sync_hash: *sync_hash,
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::StateResponse(
                                                        response,
                                                    ) => {
                                                        me.do_send(NetworkClientMessages::StateResponse(*response));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateRequestPart {
                            shard_id,
                            sync_hash,
                            part_id,
                            target: target_account_id,
                        } => {
                            let target_account_id = match target_account_id {
                                AccountOrPeerIdOrHash::AccountId(x) => x,
                                _ => panic!(),
                            };
                            for (i, name) in validators_clone2.iter().enumerate() {
                                if name == target_account_id {
                                    let me = connectors1[my_ord].0.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .1
                                            .send(NetworkViewClientMessages::StateRequestPart {
                                                shard_id: *shard_id,
                                                sync_hash: *sync_hash,
                                                part_id: *part_id,
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::StateResponse(
                                                        response,
                                                    ) => {
                                                        me.do_send(NetworkClientMessages::StateResponse(*response));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateResponse { route_back, response } => {
                            for (i, address) in addresses.iter().enumerate() {
                                if route_back == address {
                                    connectors1[i].0.do_send(
                                        NetworkClientMessages::StateResponse(response.clone()),
                                    );
                                }
                            }
                        }
                        NetworkRequests::AnnounceAccount(announce_account) => {
                            let mut aa = announced_accounts1.write().unwrap();
                            let key = (
                                announce_account.account_id.clone(),
                                announce_account.epoch_id.clone(),
                            );
                            if aa.get(&key).is_none() {
                                aa.insert(key);
                                for (_, view_client) in connectors1 {
                                    view_client.do_send(NetworkViewClientMessages::AnnounceAccount(
                                        vec![(announce_account.clone(), None)],
                                    ))
                                }
                            }
                        }
                        NetworkRequests::Approval { approval_message } => {
                            let height_mod = approval_message.approval.target_height % 300;

                            let do_propagate = if tamper_with_fg {
                                if height_mod < 100 {
                                    false
                                } else if height_mod < 200 {
                                    let mut rng = rand::thread_rng();
                                    rng.gen()
                                } else {
                                    true
                                }
                            } else {
                                true
                            };

                            let approval = approval_message.approval.clone();

                            if do_propagate {
                                for (i, name) in validators_clone2.iter().enumerate() {
                                    if name == &approval_message.target {
                                        connectors1[i].0.do_send(
                                            NetworkClientMessages::BlockApproval(
                                                approval.clone(),
                                                my_key_pair.id.clone(),
                                            ),
                                        );
                                    }
                                }
                            }

                            // Verify doomslug invariant
                            match approval.inner {
                                ApprovalInner::Endorsement(parent_hash) => {
                                    assert!(
                                        approval.target_height
                                            > largest_skipped_height1.read().unwrap()[my_ord]
                                    );
                                    largest_endorsed_height1.write().unwrap()[my_ord] =
                                        approval.target_height;

                                    if let Some(prev_height) =
                                        hash_to_height1.read().unwrap().get(&parent_hash)
                                    {
                                        assert_eq!(prev_height + 1, approval.target_height);
                                    }
                                }
                                ApprovalInner::Skip(prev_height) => {
                                    largest_skipped_height1.write().unwrap()[my_ord] =
                                        approval.target_height;
                                    let e = largest_endorsed_height1.read().unwrap()[my_ord];
                                    // `e` is the *target* height of the last endorsement. `prev_height`
                                    // is allowed to be anything >= to the source height, which is e-1.
                                    assert!(
                                        prev_height + 1 >= e,
                                        "New: {}->{}, Old: {}->{}",
                                        prev_height,
                                        approval.target_height,
                                        e - 1,
                                        e
                                    );
                                }
                            };
                        }
                        NetworkRequests::ForwardTx(_, _)
                        | NetworkRequests::BanPeer { .. }
                        | NetworkRequests::TxStatus(_, _, _)
                        | NetworkRequests::Challenge(_) => {}
                    };
                }
                resp
            }).start();
            let network_adapter = NetworkRecipient::default();
            network_adapter.set_recipient(pm);
            let (block, client, view_client_addr) = setup(
                vs,
                epoch_length,
                _account_id,
                skip_sync_wait,
                block_prod_time,
                block_prod_time * 3,
                enable_doomslug,
                archive1[index],
                epoch_sync_enabled1[index],
                Arc::new(network_adapter),
                10000,
                genesis_time,
                ctx,
            );
            view_client_addr_slot = Some(view_client_addr);
            *genesis_block1.write().unwrap() = Some(block);
            client
        });
        ret.push((client_addr, view_client_addr_slot.unwrap()));
    }
    hash_to_height.write().unwrap().insert(CryptoHash::default(), 0);
    hash_to_height
        .write()
        .unwrap()
        .insert(*genesis_block.read().unwrap().as_ref().unwrap().header().clone().hash(), 0);
    connectors.set(ret.clone()).unwrap();
    let value = genesis_block.read().unwrap();
    (value.clone().unwrap(), ret, block_stats)
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_no_network_with_validity_period_and_no_epoch_sync(
        validators,
        account_id,
        skip_sync_wait,
        100,
        enable_doomslug,
    )
}

pub fn setup_no_network_with_validity_period_and_no_epoch_sync(
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period_and_no_epoch_sync(
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        Box::new(|_, _, _| {
            PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
        }),
        transaction_validity_period,
    )
}

pub fn setup_client_with_runtime(
    num_validator_seats: NumSeats,
    account_id: Option<AccountId>,
    enable_doomslug: bool,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    client_adapter: Arc<dyn ClientAdapterForShardsManager>,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    rng_seed: RngSeed,
) -> Client {
    let validator_signer = account_id.map(|x| {
        Arc::new(InMemoryValidatorSigner::from_seed(x.clone(), KeyType::ED25519, x.as_ref()))
            as Arc<dyn ValidatorSigner>
    });
    let mut config = ClientConfig::test(true, 10, 20, num_validator_seats, false, true);
    config.epoch_length = chain_genesis.epoch_length;
    let mut client = Client::new(
        config,
        chain_genesis,
        runtime_adapter,
        network_adapter,
        client_adapter,
        validator_signer,
        enable_doomslug,
        rng_seed,
    )
    .unwrap();
    client.sync_status = SyncStatus::NoSync;
    client
}

pub fn setup_client(
    store: Store,
    vs: ValidatorSchedule,
    account_id: Option<AccountId>,
    enable_doomslug: bool,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    client_adapter: Arc<dyn ClientAdapterForShardsManager>,
    chain_genesis: ChainGenesis,
    rng_seed: RngSeed,
) -> Client {
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let runtime_adapter =
        Arc::new(KeyValueRuntime::new_with_validators(store, vs, chain_genesis.epoch_length));
    setup_client_with_runtime(
        num_validator_seats,
        account_id,
        enable_doomslug,
        network_adapter,
        client_adapter,
        chain_genesis,
        runtime_adapter,
        rng_seed,
    )
}

/// An environment for writing integration tests with multiple clients.
/// This environment can simulate near nodes without network and it can be configured to use different runtimes.
pub struct TestEnv {
    pub chain_genesis: ChainGenesis,
    pub validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockPeerManagerAdapter>>,
    pub client_adapters: Vec<Arc<MockClientAdapterForShardsManager>>,
    pub clients: Vec<Client>,
    account_to_client_index: HashMap<AccountId, usize>,
    paused_blocks: Arc<Mutex<HashMap<CryptoHash, Arc<OnceCell<()>>>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    seeds: HashMap<AccountId, RngSeed>,
}

/// A builder for the TestEnv structure.
pub struct TestEnvBuilder {
    chain_genesis: ChainGenesis,
    clients: Vec<AccountId>,
    validators: Vec<AccountId>,
    runtime_adapters: Option<Vec<Arc<dyn RuntimeAdapter>>>,
    network_adapters: Option<Vec<Arc<MockPeerManagerAdapter>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    seeds: HashMap<AccountId, RngSeed>,
}

/// Builder for the [`TestEnv`] structure.
impl TestEnvBuilder {
    /// Constructs a new builder.
    fn new(chain_genesis: ChainGenesis) -> Self {
        let clients = Self::make_accounts(1);
        let validators = clients.clone();
        let seeds: HashMap<AccountId, RngSeed> = HashMap::with_capacity(1);
        Self {
            chain_genesis,
            clients,
            validators,
            runtime_adapters: None,
            network_adapters: None,
            seeds,
        }
    }

    /// Sets list of client [`AccountId`]s to the one provided.  Panics if the
    /// vector is empty.
    pub fn clients(mut self, clients: Vec<AccountId>) -> Self {
        assert!(!clients.is_empty());
        self.clients = clients;
        self
    }

    /// Sets random seed for each client according to the provided HashMap.
    pub fn clients_random_seeds(mut self, seeds: HashMap<AccountId, RngSeed>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Sets number of clients to given one.  To get [`AccountId`] used by the
    /// validator associated with the client the [`TestEnv::get_client_id`]
    /// method can be used.  Tests should not rely on any particular format of
    /// account identifiers used by the builder.  Panics if `num` is zero.
    pub fn clients_count(self, num: usize) -> Self {
        self.clients(Self::make_accounts(num))
    }

    /// Sets list of validator [`AccountId`]s to the one provided.  Panics if
    /// the vector is empty.
    pub fn validators(mut self, validators: Vec<AccountId>) -> Self {
        assert!(!validators.is_empty());
        self.validators = validators;
        self
    }

    /// Sets number of validator seats to given one.  To get [`AccountId`] used
    /// in the test environment the `validators` field of the built [`TestEnv`]
    /// object can be used.  Tests should not rely on any particular format of
    /// account identifiers used by the builder.  Panics if `num` is zero.
    pub fn validator_seats(self, num: usize) -> Self {
        self.validators(Self::make_accounts(num))
    }

    /// Specifies custom runtime adaptors for each client.  This allows us to
    /// construct [`TestEnv`] with `NightshadeRuntime`.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn runtime_adapters(mut self, adapters: Vec<Arc<dyn RuntimeAdapter>>) -> Self {
        self.runtime_adapters = Some(adapters);
        self
    }

    /// Specifies custom network adaptors for each client.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn network_adapters(mut self, adapters: Vec<Arc<MockPeerManagerAdapter>>) -> Self {
        self.network_adapters = Some(adapters);
        self
    }

    /// Constructs new `TestEnv` structure.
    ///
    /// If no clients were configured (either through count or vector) one
    /// client is created.  Similarly, if no validator seats were configured,
    /// one seat is configured.
    ///
    /// Panics if `runtime_adapters` or `network_adapters` methods were used and
    /// the length of the vectors passed to them did not equal number of
    /// configured clients.
    pub fn build(self) -> TestEnv {
        let chain_genesis = self.chain_genesis;
        let clients = self.clients.clone();
        let num_clients = clients.len();
        let validators = self.validators;
        let num_validators = validators.len();
        let seeds = self.seeds;
        let network_adapters = self
            .network_adapters
            .unwrap_or_else(|| (0..num_clients).map(|_| Arc::new(Default::default())).collect());
        let client_adapters = (0..num_clients)
            .map(|_| Arc::new(MockClientAdapterForShardsManager::default()))
            .collect::<Vec<_>>();
        assert_eq!(clients.len(), network_adapters.len());
        let clients = match self.runtime_adapters {
            None => clients
                .into_iter()
                .zip(network_adapters.iter())
                .zip(client_adapters.iter())
                .map(|((account_id, network_adapter), client_adapter)| {
                    let rng_seed = match seeds.get(&account_id) {
                        Some(seed) => *seed,
                        None => TEST_SEED,
                    };
                    let vs = ValidatorSchedule::new()
                        .block_producers_per_epoch(vec![validators.clone()]);
                    setup_client(
                        create_test_store(),
                        vs,
                        Some(account_id),
                        false,
                        network_adapter.clone(),
                        client_adapter.clone(),
                        chain_genesis.clone(),
                        rng_seed,
                    )
                })
                .collect(),
            Some(runtime_adapters) => {
                assert!(clients.len() == runtime_adapters.len());
                clients
                    .into_iter()
                    .zip((&network_adapters).iter())
                    .zip(runtime_adapters.into_iter().zip(client_adapters.iter()))
                    .map(|((account_id, network_adapter), (runtime_adapter, client_adapter))| {
                        let rng_seed = match seeds.get(&account_id) {
                            Some(seed) => *seed,
                            None => TEST_SEED,
                        };
                        setup_client_with_runtime(
                            u64::try_from(num_validators).unwrap(),
                            Some(account_id),
                            false,
                            network_adapter.clone(),
                            client_adapter.clone(),
                            chain_genesis.clone(),
                            runtime_adapter,
                            rng_seed,
                        )
                    })
                    .collect()
            }
        };

        TestEnv {
            chain_genesis,
            validators,
            network_adapters,
            client_adapters,
            clients,
            account_to_client_index: self
                .clients
                .into_iter()
                .enumerate()
                .map(|(index, client)| (client, index))
                .collect(),
            paused_blocks: Default::default(),
            seeds,
        }
    }

    fn make_accounts(count: usize) -> Vec<AccountId> {
        (0..count).map(|i| format!("test{}", i).parse().unwrap()).collect()
    }
}

impl TestEnv {
    pub fn builder(chain_genesis: ChainGenesis) -> TestEnvBuilder {
        TestEnvBuilder::new(chain_genesis)
    }

    /// Process a given block in the client with index `id`.
    /// Simulate the block processing logic in `Client`, i.e, it would run catchup and then process accepted blocks and possibly produce chunks.
    pub fn process_block(&mut self, id: usize, block: Block, provenance: Provenance) {
        self.clients[id].process_block_test(MaybeValidated::from(block), provenance).unwrap();
    }

    /// Produces block by given client, which may kick off chunk production.
    /// This means that transactions added before this call will be included in the next block produced by this validator.
    pub fn produce_block(&mut self, id: usize, height: BlockHeight) {
        let block = self.clients[id].produce_block(height).unwrap();
        self.process_block(id, block.unwrap(), Provenance::PRODUCED);
    }

    /// Pause processing of the given block, which means that the background
    /// thread which applies the chunks on the block will get blocked until
    /// `resume_block_processing` is called.
    ///
    /// Note that you must call `resume_block_processing` at some later point to
    /// unstuck the block.
    ///
    /// Implementation is rather crude and just hijacks our logging
    /// infrastructure. Hopefully this is good enough, but, if it isn't, we can
    /// add something more robust.
    pub fn pause_block_processing(&mut self, capture: &mut TracingCapture, block: &CryptoHash) {
        let paused_blocks = Arc::clone(&self.paused_blocks);
        paused_blocks.lock().unwrap().insert(*block, Arc::new(OnceCell::new()));
        capture.set_callback(move |msg| {
            if msg.starts_with("do_apply_chunks") {
                let cell = paused_blocks.lock().unwrap().iter().find_map(|(block_hash, cell)| {
                    if msg.contains(&format!("block_hash={block_hash}")) {
                        Some(Arc::clone(cell))
                    } else {
                        None
                    }
                });
                if let Some(cell) = cell {
                    cell.wait();
                }
            }
        });
    }

    /// See `pause_block_processing`.
    pub fn resume_block_processing(&mut self, block: &CryptoHash) {
        let mut paused_blocks = self.paused_blocks.lock().unwrap();
        let cell = paused_blocks.remove(block).unwrap();
        let _ = cell.set(());
    }

    pub fn client(&mut self, account_id: &AccountId) -> &mut Client {
        &mut self.clients[self.account_to_client_index[account_id]]
    }

    pub fn process_partial_encoded_chunks(&mut self) {
        let network_adapters = self.network_adapters.clone();
        for network_adapter in network_adapters {
            // process partial encoded chunks
            while let Some(request) = network_adapter.pop() {
                if let PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id,
                        partial_encoded_chunk,
                    },
                ) = request
                {
                    self.client(&account_id)
                        .process_partial_encoded_chunk(partial_encoded_chunk.into())
                        .unwrap();
                }
            }
        }
    }

    /// Process all PartialEncodedChunkRequests in the network queue for a client
    /// `id`: id for the client
    pub fn process_partial_encoded_chunks_requests(&mut self, id: usize) {
        while let Some(request) = self.network_adapters[id].pop() {
            self.process_partial_encoded_chunk_request(id, request);
        }
    }

    /// Send the PartialEncodedChunkRequest to the target client, get response and process the response
    pub fn process_partial_encoded_chunk_request(
        &mut self,
        id: usize,
        request: PeerManagerMessageRequest,
    ) {
        if let PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedChunkRequest { target, request, .. },
        ) = request
        {
            let target_id = self.account_to_client_index[&target.account_id.unwrap()];
            let response = self.get_partial_encoded_chunk_response(target_id, request);
            self.clients[id].process_partial_encoded_chunk_response(response).unwrap();
        } else {
            panic!("The request is not a PartialEncodedChunk request {:?}", request);
        }
    }

    pub fn get_partial_encoded_chunk_response(
        &mut self,
        id: usize,
        request: PartialEncodedChunkRequestMsg,
    ) -> PartialEncodedChunkResponseMsg {
        let client = &mut self.clients[id];
        client.shards_mgr.process_partial_encoded_chunk_request(
            request,
            CryptoHash::default(),
            client.chain.mut_store(),
        );
        let response = self.network_adapters[id].pop_most_recent().unwrap();
        if let PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedChunkResponse { route_back: _, response },
        ) = response
        {
            return response;
        } else {
            panic!(
                "did not find PartialEncodedChunkResponse from the network queue {:?}",
                response
            );
        }
    }

    pub fn process_shards_manager_responses(&mut self, id: usize) {
        while let Some(msg) = self.client_adapters[id].pop() {
            match msg {
                ShardsManagerResponse::ChunkCompleted(chunk_header) => {
                    self.clients[id].on_chunk_completed(&chunk_header, Arc::new(|_| {}));
                }
            }
        }
    }

    pub fn process_shards_manager_responses_and_finish_processing_blocks(&mut self, idx: usize) {
        loop {
            self.process_shards_manager_responses(idx);
            if self.clients[idx].finish_blocks_in_processing().is_empty() {
                return;
            }
        }
    }

    pub fn send_money(&mut self, id: usize) -> NetworkClientResponses {
        let account_id = self.get_client_id(0);
        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            100,
            self.clients[id].chain.head().unwrap().last_block_hash,
        );
        self.clients[id].process_tx(tx, false, false)
    }

    pub fn upgrade_protocol(&mut self, protocol_version: ProtocolVersion) {
        assert_eq!(self.clients.len(), 1, "at the moment, this support only a single client");

        let tip = self.clients[0].chain.head().unwrap();
        let epoch_id = self.clients[0]
            .runtime_adapter
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            self.clients[0].runtime_adapter.get_block_producer(&epoch_id, tip.height).unwrap();

        let mut block = self.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        block.mut_header().set_latest_protocol_version(protocol_version);
        block.mut_header().resign(&InMemoryValidatorSigner::from_seed(
            block_producer.clone(),
            KeyType::ED25519,
            block_producer.as_ref(),
        ));

        let _ = self.clients[0]
            .process_block_test_no_produce_chunk(block.into(), Provenance::NONE)
            .unwrap();

        for i in 0..self.clients[0].chain.epoch_length * 2 {
            self.produce_block(0, tip.height + i + 2);
        }
    }

    pub fn query_account(&mut self, account_id: AccountId) -> AccountView {
        let head = self.clients[0].chain.head().unwrap();
        let last_block = self.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let last_chunk_header = &last_block.chunks()[0];
        let response = self.clients[0]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewAccount(account_view) => account_view,
            _ => panic!("Wrong return value"),
        }
    }

    pub fn query_state(&mut self, account_id: AccountId) -> Vec<StateItem> {
        let head = self.clients[0].chain.head().unwrap();
        let last_block = self.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        let last_chunk_header = &last_block.chunks()[0];
        let response = self.clients[0]
            .runtime_adapter
            .query(
                ShardUId::single_shard(),
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &QueryRequest::ViewState {
                    account_id,
                    prefix: vec![].into(),
                    include_proof: false,
                },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewState(view_state_result) => view_state_result.values,
            _ => panic!("Wrong return value"),
        }
    }

    pub fn query_balance(&mut self, account_id: AccountId) -> Balance {
        self.query_account(account_id).amount
    }

    /// Restarts client at given index.  Note that the client is restarted with
    /// the default runtime adapter (i.e. [`KeyValueRuntime`]).  That is, if
    /// this `TestEnv` was created with custom runtime adapters that
    /// customisation will be lost.
    pub fn restart(&mut self, idx: usize) {
        let store = self.clients[idx].chain.store().store().clone();
        let account_id = self.get_client_id(idx).clone();
        let rng_seed = match self.seeds.get(&account_id) {
            Some(seed) => *seed,
            None => TEST_SEED,
        };
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![self.validators.clone()]);
        self.clients[idx] = setup_client(
            store,
            vs,
            Some(self.get_client_id(idx).clone()),
            false,
            self.network_adapters[idx].clone(),
            self.client_adapters[idx].clone(),
            self.chain_genesis.clone(),
            rng_seed,
        )
    }

    /// Returns an [`AccountId`] used by a client at given index.  More
    /// specifically, returns validator id of the client’s validator signer.
    pub fn get_client_id(&self, idx: usize) -> &AccountId {
        self.clients[idx].validator_signer.as_ref().unwrap().validator_id()
    }

    pub fn get_runtime_config(&self, idx: usize, epoch_id: EpochId) -> RuntimeConfig {
        self.clients[idx].runtime_adapter.get_protocol_config(&epoch_id).unwrap().runtime_config
    }

    /// Create and sign transaction ready for execution.
    pub fn tx_from_actions(
        &mut self,
        actions: Vec<Action>,
        signer: &InMemorySigner,
        receiver: AccountId,
    ) -> SignedTransaction {
        let tip = self.clients[0].chain.head().unwrap();
        SignedTransaction::from_actions(
            tip.height + 1,
            signer.account_id.clone(),
            receiver,
            signer,
            actions,
            tip.last_block_hash,
        )
    }

    /// Process a tx and its receipts, then return the execution outcome.
    pub fn execute_tx(&mut self, tx: SignedTransaction) -> FinalExecutionOutcomeView {
        let tx_hash = tx.get_hash().clone();
        self.clients[0].process_tx(tx, false, false);
        let max_iters = 100;
        let tip = self.clients[0].chain.head().unwrap();
        for i in 0..max_iters {
            let block = self.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            self.process_block(0, block.clone(), Provenance::PRODUCED);
            if let Ok(outcome) = self.clients[0].chain.get_final_transaction_result(&tx_hash) {
                return outcome;
            }
        }
        panic!("No transaction outcome found after {max_iters} blocks.")
    }

    /// Execute a function call transaction that calls main on the `TestEnv`.
    ///
    /// This function assumes that account has been deployed and that
    /// `InMemorySigner::from_seed` produces a valid signer that has it's key
    /// deployed already.
    pub fn call_main(&mut self, account: &AccountId) -> FinalExecutionOutcomeView {
        let signer = InMemorySigner::from_seed(account.clone(), KeyType::ED25519, account.as_str());
        let actions = vec![Action::FunctionCall(FunctionCallAction {
            method_name: "main".to_string(),
            args: vec![],
            gas: 3 * 10u64.pow(14),
            deposit: 0,
        })];
        let tx = self.tx_from_actions(actions, &signer, signer.account_id.clone());
        self.execute_tx(tx)
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let paused_blocks = self.paused_blocks.lock().unwrap();
        for cell in paused_blocks.values() {
            let _ = cell.set(());
        }
        if !paused_blocks.is_empty() && !std::thread::panicking() {
            panic!("some blocks are still paused, did you call `resume_block_processing`?")
        }
    }
}

pub fn create_chunk_on_height_for_shard(
    client: &mut Client,
    next_height: BlockHeight,
    shard_id: ShardId,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>) {
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block = client.chain.get_block(&last_block_hash).unwrap();
    client
        .produce_chunk(
            last_block_hash,
            &client.runtime_adapter.get_epoch_id_from_prev_block(&last_block_hash).unwrap(),
            Chain::get_prev_chunk_header(&*client.runtime_adapter, &last_block, shard_id).unwrap(),
            next_height,
            shard_id,
        )
        .unwrap()
        .unwrap()
}

pub fn create_chunk_on_height(
    client: &mut Client,
    next_height: BlockHeight,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>) {
    create_chunk_on_height_for_shard(client, next_height, 0)
}

pub fn create_chunk_with_transactions(
    client: &mut Client,
    transactions: Vec<SignedTransaction>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    create_chunk(client, Some(transactions), None)
}

/// Create a chunk with specified transactions and possibly a new state root.
/// Useful for writing tests with challenges.
pub fn create_chunk(
    client: &mut Client,
    replace_transactions: Option<Vec<SignedTransaction>>,
    replace_tx_root: Option<CryptoHash>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    let last_block = client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap();
    let next_height = last_block.header().height() + 1;
    let (mut chunk, mut merkle_paths, receipts) = client
        .produce_chunk(
            *last_block.hash(),
            last_block.header().epoch_id(),
            last_block.chunks()[0].clone(),
            next_height,
            0,
        )
        .unwrap()
        .unwrap();
    let should_replace = replace_transactions.is_some() || replace_tx_root.is_some();
    let transactions = replace_transactions.unwrap_or_else(Vec::new);
    let tx_root = match replace_tx_root {
        Some(root) => root,
        None => merklize(&transactions).0,
    };
    // reconstruct the chunk with changes (if any)
    if should_replace {
        // The best way it to decode chunk, replace transactions and then recreate encoded chunk.
        let total_parts = client.chain.runtime_adapter.num_total_parts();
        let data_parts = client.chain.runtime_adapter.num_data_parts();
        let decoded_chunk = chunk.decode_chunk(data_parts).unwrap();
        let parity_parts = total_parts - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

        let signer = client.validator_signer.as_ref().unwrap().clone();
        let header = chunk.cloned_header();
        let (mut encoded_chunk, mut new_merkle_paths) = EncodedShardChunk::new(
            header.prev_block_hash().clone(),
            header.prev_state_root(),
            header.outcome_root(),
            header.height_created(),
            header.shard_id(),
            &mut rs,
            header.gas_used(),
            header.gas_limit(),
            header.balance_burnt(),
            tx_root,
            header.validator_proposals().collect(),
            transactions,
            decoded_chunk.receipts(),
            header.outgoing_receipts_root(),
            &*signer,
            PROTOCOL_VERSION,
        )
        .unwrap();
        swap(&mut chunk, &mut encoded_chunk);
        swap(&mut merkle_paths, &mut new_merkle_paths);
    }
    match &mut chunk {
        EncodedShardChunk::V1(chunk) => {
            chunk.header.height_included = next_height;
        }
        EncodedShardChunk::V2(chunk) => {
            *chunk.header.height_included_mut() = next_height;
        }
    }
    let block_merkle_tree = client.chain.store().get_block_merkle_tree(last_block.hash()).unwrap();
    let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
    block_merkle_tree.insert(*last_block.hash());
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        last_block.header(),
        next_height,
        last_block.header().block_ordinal() + 1,
        vec![chunk.cloned_header()],
        last_block.header().epoch_id().clone(),
        last_block.header().next_epoch_id().clone(),
        None,
        vec![],
        Ratio::new(0, 1),
        0,
        100,
        None,
        vec![],
        vec![],
        &*client.validator_signer.as_ref().unwrap().clone(),
        *last_block.header().next_bp_hash(),
        block_merkle_tree.root(),
        None,
    );
    (chunk, merkle_paths, receipts, block)
}

/// Keep running catchup until there is no more catchup work that can be done
/// Note that this function does not necessarily mean that all blocks are caught up.
/// It's possible that some blocks that need to be caught up are still being processed
/// and the catchup process can't catch up on these blocks yet.
pub fn run_catchup(
    client: &mut Client,
    highest_height_peers: &[FullPeerInfo],
) -> Result<(), Error> {
    let f = |_| {};
    let block_messages = Arc::new(RwLock::new(vec![]));
    let block_inside_messages = block_messages.clone();
    let block_catch_up = move |msg: BlockCatchUpRequest| {
        block_inside_messages.write().unwrap().push(msg);
    };
    let state_split_messages = Arc::new(RwLock::new(vec![]));
    let state_split_inside_messages = state_split_messages.clone();
    let state_split = move |msg: StateSplitRequest| {
        state_split_inside_messages.write().unwrap().push(msg);
    };
    let rt = client.runtime_adapter.clone();
    loop {
        client.run_catchup(
            highest_height_peers,
            &f,
            &block_catch_up,
            &state_split,
            Arc::new(|_| {}),
        )?;
        let mut catchup_done = true;
        for msg in block_messages.write().unwrap().drain(..) {
            let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);
            if let Some((_, _, blocks_catch_up_state)) =
                client.catchup_state_syncs.get_mut(&msg.sync_hash)
            {
                assert!(blocks_catch_up_state.scheduled_blocks.remove(&msg.block_hash));
                blocks_catch_up_state.processed_blocks.insert(msg.block_hash, results);
            } else {
                panic!("block catch up processing result from unknown sync hash");
            }
            catchup_done = false;
        }
        for msg in state_split_messages.write().unwrap().drain(..) {
            let results = rt.build_state_for_split_shards(
                msg.shard_uid,
                &msg.state_root,
                &msg.next_epoch_shard_layout,
                msg.state_split_status,
            );
            if let Some((sync, _, _)) = client.catchup_state_syncs.get_mut(&msg.sync_hash) {
                // We are doing catchup
                sync.set_split_result(msg.shard_id, results);
            } else {
                client.state_sync.set_split_result(msg.shard_id, results);
            }
            catchup_done = false;
        }
        if catchup_done {
            break;
        }
    }
    Ok(())
}
