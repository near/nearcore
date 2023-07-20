use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use actix::{Actor, Addr, AsyncContext, Context};
use actix_rt::{Arbiter, System};
use chrono::DateTime;
use futures::{future, FutureExt};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::{CanSend, IntoSender, LateBoundSender, Sender};
use near_async::time;
use near_chain::resharding::StateSplitRequest;
use near_chunks::shards_manager_actor::start_shards_manager;
use near_chunks::ShardsManager;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_primitives::errors::InvalidTxError;
use near_primitives::test_utils::create_test_signer;
use num_rational::Ratio;
use once_cell::sync::OnceCell;
use rand::{thread_rng, Rng};
use tracing::info;

use crate::{start_view_client, Client, ClientActor, SyncStatus, ViewClientActor};
use chrono::Utc;
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest};
use near_chain::state_snapshot_actor::MakeSnapshotCallback;
use near_chain::test_utils::{
    wait_for_all_blocks_in_processing, wait_for_block_in_processing, KeyValueRuntime,
    MockEpochManager, ValidatorSchedule,
};
use near_chain::types::{ChainConfig, RuntimeAdapter};
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Provenance};
use near_chain_configs::{ClientConfig, GenesisConfig};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_utils::{MockClientAdapterForShardsManager, SynchronousShardsManagerAdapter};
use near_client_primitives::types::Error;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{
    AccountOrPeerIdOrHash, HighestHeightPeerInfo, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerInfo, PeerType,
};
use near_network::types::{BlockInfo, PeerChainInfo};
use near_network::types::{
    ConnectedPeerInfo, FullPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_network::types::{
    NetworkInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
use near_o11y::testonly::TracingCapture;
use near_o11y::WithSpanContextExt;
use near_primitives::block::{ApprovalInner, Block, GenesisId};
use near_primitives::delegate_action::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::epoch_manager::RngSeed;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::network::PeerId;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{EncodedShardChunk, PartialEncodedChunk, ReedSolomonWrapper};
use near_primitives::static_clock::StaticClock;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};

use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, NumBlocks, NumSeats, NumShards,
    ShardId,
};
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, QueryRequest, QueryResponseKind, StateItem,
};
use near_store::test_utils::create_test_store;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::adapter::{
    AnnounceAccountRequest, BlockApproval, BlockHeadersRequest, BlockHeadersResponse, BlockRequest,
    BlockResponse, ProcessTxResponse, SetNetworkInfo, StateRequestHeader, StateRequestPart,
    StateResponse,
};

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

    /// This function finishes processing block with hash `hash`, if the processing of that block
    /// has started.
    pub fn finish_block_in_processing(&mut self, hash: &CryptoHash) -> Vec<CryptoHash> {
        if let Ok(()) = wait_for_block_in_processing(&mut self.chain, hash) {
            let (accepted_blocks, _) = self.postprocess_ready_blocks(Arc::new(|_| {}), true);
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
    state_sync_enabled: bool,
    network_adapter: PeerManagerAdapter,
    transaction_validity_period: NumBlocks,
    genesis_time: DateTime<Utc>,
    ctx: &Context<ClientActor>,
) -> (Block, ClientActor, Addr<ViewClientActor>, ShardsManagerAdapterForTest) {
    let store = create_test_store();
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new_with_no_gc(store.clone(), epoch_manager.as_ref(), archive);
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
    let chain = Chain::new(
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        &chain_genesis,
        doomslug_threshold_mode,
        ChainConfig {
            save_trie_changes: true,
            background_migration_threads: 1,
            state_snapshot_every_n_blocks: None,
        },
        None,
    )
    .unwrap();
    let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();

    let signer = Arc::new(create_test_signer(account_id.as_str()));
    let telemetry = TelemetryActor::default().start();
    let config = ClientConfig::test(
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        true,
        epoch_sync_enabled,
        state_sync_enabled,
    );

    let adv = crate::adversarial::Controls::default();

    let view_client_addr = start_view_client(
        Some(signer.validator_id().clone()),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.clone(),
        adv.clone(),
    );

    let (shards_manager_addr, _) = start_shards_manager(
        epoch_manager.clone(),
        shard_tracker.clone(),
        network_adapter.clone().into_sender(),
        ctx.address().with_auto_span_context().into_sender(),
        Some(account_id),
        store,
        config.chunk_request_retry_period,
    );
    let shards_manager_adapter = Arc::new(shards_manager_addr);

    let client = Client::new(
        config.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter.clone(),
        shards_manager_adapter.as_sender(),
        Some(signer.clone()),
        enable_doomslug,
        TEST_SEED,
        None,
    )
    .unwrap();
    let client_actor = ClientActor::new(
        client,
        ctx.address(),
        config,
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        network_adapter,
        Some(signer),
        telemetry,
        ctx,
        None,
        adv,
        None,
    )
    .unwrap();
    (genesis_block, client_actor, view_client_addr, shards_manager_adapter.into())
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
    state_sync_enabled: bool,
    network_adapter: PeerManagerAdapter,
    transaction_validity_period: NumBlocks,
    genesis_time: DateTime<Utc>,
) -> Addr<ViewClientActor> {
    let store = create_test_store();
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new_with_no_gc(store, epoch_manager.as_ref(), archive);
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
    Chain::new(
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        &chain_genesis,
        doomslug_threshold_mode,
        ChainConfig {
            save_trie_changes: true,
            background_migration_threads: 1,
            state_snapshot_every_n_blocks: None,
        },
        None,
    )
    .unwrap();

    let signer = Arc::new(create_test_signer(account_id.as_str()));
    TelemetryActor::default().start();
    let config = ClientConfig::test(
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        true,
        epoch_sync_enabled,
        state_sync_enabled,
    );

    let adv = crate::adversarial::Controls::default();

    start_view_client(
        Some(signer.validator_id().clone()),
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter,
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
) -> ActorHandlesForTesting {
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
) -> ActorHandlesForTesting {
    let network_adapter = Arc::new(LateBoundSender::default());
    let mut vca: Option<Addr<ViewClientActor>> = None;
    let mut sma: Option<ShardsManagerAdapterForTest> = None;
    let client_addr = ClientActor::create(|ctx: &mut Context<ClientActor>| {
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![validators]);
        let (_, client, view_client_addr, shards_manager_adapter) = setup(
            vs,
            10,
            account_id,
            skip_sync_wait,
            MIN_BLOCK_PROD_TIME.as_millis() as u64,
            MAX_BLOCK_PROD_TIME.as_millis() as u64,
            enable_doomslug,
            false,
            false,
            true,
            network_adapter.clone().into(),
            transaction_validity_period,
            StaticClock::utc(),
            ctx,
        );
        vca = Some(view_client_addr);
        sma = Some(shards_manager_adapter);
        client
    });
    let client_addr1 = client_addr.clone();

    let network_actor =
        PeerManagerMock::new(move |msg, ctx| peermanager_mock(&msg, ctx, client_addr1.clone()))
            .start();

    network_adapter.bind(network_actor);

    ActorHandlesForTesting {
        client_actor: client_addr,
        view_client_actor: vca.unwrap(),
        shards_manager_adapter: sma.unwrap(),
    }
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
            last_check: StaticClock::instant(),
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
        let now = StaticClock::instant();
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

#[derive(Clone)]
pub struct ActorHandlesForTesting {
    pub client_actor: Addr<ClientActor>,
    pub view_client_actor: Addr<ViewClientActor>,
    pub shards_manager_adapter: ShardsManagerAdapterForTest,
}

fn send_chunks<T, I, F>(
    connectors: &[ActorHandlesForTesting],
    recipients: I,
    target: T,
    drop_chunks: bool,
    send_to: F,
) where
    T: Eq,
    I: Iterator<Item = (usize, T)>,
    F: Fn(&ShardsManagerAdapterForTest),
{
    for (i, name) in recipients {
        if name == target {
            if !drop_chunks || !thread_rng().gen_ratio(1, 5) {
                send_to(&connectors[i].shards_manager_adapter);
            }
        }
    }
}

/// Setup multiple clients talking to each other via a mock network.
///
/// # Arguments
///
/// `vs` - the set of validators and how they are assigned to shards in different epochs.
///
/// `key_pairs` - keys for `validators`
///
/// `skip_sync_wait`
///
/// `block_prod_time` - Minimum block production time, assuming there is enough approvals. The
///                 maximum block production time depends on the value of `tamper_with_fg`, and is
///                 equal to `block_prod_time` if `tamper_with_fg` is `true`, otherwise it is
///                 `block_prod_time * 2`
///
/// `drop_chunks` - if set to true, 10% of all the chunk messages / requests will be dropped
///
/// `tamper_with_fg` - if set to true, will split the heights into groups of 100. For some groups
///                 all the approvals will be dropped (thus completely disabling the finality gadget
///                 and introducing severe forkfulness if `block_prod_time` is sufficiently small),
///                 for some groups will keep all the approvals (and test the fg invariants), and
///                 for some will drop 50% of the approvals.
///                 This was designed to tamper with the finality gadget when we
///                 had it, unclear if has much effect today. Must be disabled if doomslug is
///                 enabled (see below), because doomslug will stall if approvals are not delivered.
///
/// `epoch_length` - approximate length of the epoch as measured
///                 by the block heights difference of it's last and first block.
///
/// `enable_doomslug` - If false, blocks will be created when at least one approval is present, without
///                   waiting for 2/3. This allows for more forkfulness. `cross_shard_tx` has modes
///                   both with enabled doomslug (to test "production" setting) and with disabled
///                   doomslug (to test higher forkfullness)
///
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
            // Peer validators
            &[ActorHandlesForTesting],
            // Validator that sends the message
            AccountId,
            // The message itself
            &PeerManagerMessageRequest,
        ) -> (PeerManagerMessageResponse, /* perform default */ bool),
    >,
) -> (Block, Vec<ActorHandlesForTesting>, Arc<RwLock<BlockStats>>) {
    let peer_manager_mock = Arc::new(RwLock::new(peer_manager_mock));
    let validators = vs.all_validators().cloned().collect::<Vec<_>>();
    let key_pairs = key_pairs;

    let addresses: Vec<_> = (0..key_pairs.len()).map(|i| hash(vec![i as u8].as_ref())).collect();
    let genesis_time = StaticClock::utc();
    let mut ret = vec![];

    let connectors: Arc<OnceCell<Vec<ActorHandlesForTesting>>> = Default::default();

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
        let mut shards_manager_adapter_slot = None;
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
                let (resp, perform_default) =
                    guard.deref_mut()(connectors1.as_slice(), account_id.clone(), &msg);
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
                                    chain_info: PeerChainInfo {
                                        genesis_id: GenesisId {
                                            chain_id: "unittest".to_string(),
                                            hash: Default::default(),
                                        },
                                        // TODO: add the correct hash here
                                        last_block: Some(BlockInfo {
                                            height: last_height2[i],
                                            hash: CryptoHash::default(),
                                        }),
                                        tracked_shards: vec![],
                                        archival: true,
                                    },
                                },
                                received_bytes_per_sec: 0,
                                sent_bytes_per_sec: 0,
                                last_time_peer_requested: near_async::time::Instant::now(),
                                last_time_received_message: near_async::time::Instant::now(),
                                connection_established_time: near_async::time::Instant::now(),
                                peer_type: PeerType::Outbound,
                                nonce: 3,
                            })
                            .collect();
                        let peers2 = peers
                            .iter()
                            .filter_map(|it| it.full_peer_info.clone().into())
                            .collect();
                        let info = NetworkInfo {
                            connected_peers: peers,
                            tier1_connections: vec![],
                            num_connected_peers: key_pairs1.len(),
                            peer_max_count: key_pairs1.len() as u32,
                            highest_height_peers: peers2,
                            sent_bytes_per_sec: 0,
                            received_bytes_per_sec: 0,
                            known_producers: vec![],
                            tier1_accounts_keys: vec![],
                            tier1_accounts_data: vec![],
                        };
                        client_addr.do_send(SetNetworkInfo(info).with_span_context());
                    }

                    match msg.as_network_requests_ref() {
                        NetworkRequests::Block { block } => {
                            if check_block_stats {
                                let block_stats2 = &mut *block_stats1.write().unwrap();
                                block_stats2.add_block(block);
                                block_stats2.check_stats(false);
                            }

                            for actor_handles in connectors1 {
                                actor_handles.client_actor.do_send(
                                    BlockResponse {
                                        block: block.clone(),
                                        peer_id: PeerInfo::random().id,
                                        was_requested: false,
                                    }
                                    .with_span_context(),
                                );
                            }

                            let mut last_height1 = last_height1.write().unwrap();

                            let my_height = &mut last_height1[my_ord];

                            *my_height = max(*my_height, block.header().height());

                            hash_to_height1
                                .write()
                                .unwrap()
                                .insert(*block.header().hash(), block.header().height());
                        }
                        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().map(|s| Some(s.clone())).enumerate(),
                                target.account_id.as_ref().map(|s| s.clone()),
                                drop_chunks,
                                |c| {
                                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest { partial_encoded_chunk_request: request.clone(), route_back: my_address });
                                },
                            );
                        }
                        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                            send_chunks(
                                connectors1,
                                addresses.iter().enumerate(),
                                route_back,
                                drop_chunks,
                                |c| {
                                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse { partial_encoded_chunk_response: response.clone(), received_time: Instant::now() });
                                },
                            );
                        }
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        } => {
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().cloned().enumerate(),
                                account_id.clone(),
                                drop_chunks,
                                |c| {
                                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(partial_encoded_chunk.clone().into()));
                                },
                            );
                        }
                        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                            send_chunks(
                                connectors1,
                                validators_clone2.iter().cloned().enumerate(),
                                account_id.clone(),
                                drop_chunks,
                                |c| {
                                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward.clone()));
                                }
                            );
                        }
                        NetworkRequests::BlockRequest { hash, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let me = connectors1[my_ord].client_actor.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .view_client_actor
                                            .send(BlockRequest(*hash).with_span_context())
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    Some(block) => {
                                                        me.do_send(
                                                            BlockResponse {
                                                                block: *block,
                                                                peer_id,
                                                                was_requested: true,
                                                            }
                                                            .with_span_context(),
                                                        );
                                                    }
                                                    None => {}
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
                                    let me = connectors1[my_ord].client_actor.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .view_client_actor
                                            .send(
                                                BlockHeadersRequest(hashes.clone())
                                                    .with_span_context(),
                                            )
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    Some(headers) => {
                                                        me.do_send(
                                                            BlockHeadersResponse(headers, peer_id)
                                                                .with_span_context(),
                                                        );
                                                    }
                                                    None => {}
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
                                    let me = connectors1[my_ord].client_actor.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .view_client_actor
                                            .send(
                                                StateRequestHeader {
                                                    shard_id: *shard_id,
                                                    sync_hash: *sync_hash,
                                                }
                                                .with_span_context(),
                                            )
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    Some(response) => {
                                                        me.do_send(response.with_span_context());
                                                    }
                                                    None => {}
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
                                    let me = connectors1[my_ord].client_actor.clone();
                                    actix::spawn(
                                        connectors1[i]
                                            .view_client_actor
                                            .send(
                                                StateRequestPart {
                                                    shard_id: *shard_id,
                                                    sync_hash: *sync_hash,
                                                    part_id: *part_id,
                                                }
                                                .with_span_context(),
                                            )
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    Some(response) => {
                                                        me.do_send(response.with_span_context());
                                                    }
                                                    None => {}
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
                                    connectors1[i].client_actor.do_send(
                                        StateResponse(Box::new(response.clone()))
                                            .with_span_context(),
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
                                for actor_handles in connectors1 {
                                    actor_handles.view_client_actor.do_send(
                                        AnnounceAccountRequest(vec![(
                                            announce_account.clone(),
                                            None,
                                        )])
                                        .with_span_context(),
                                    )
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
                                        connectors1[i].client_actor.do_send(
                                            BlockApproval(approval.clone(), my_key_pair.id.clone())
                                                .with_span_context(),
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
            })
            .start();
            let (block, client, view_client_addr, shards_manager_adapter) = setup(
                vs,
                epoch_length,
                _account_id,
                skip_sync_wait,
                block_prod_time,
                block_prod_time * 3,
                enable_doomslug,
                archive1[index],
                epoch_sync_enabled1[index],
                true,
                Arc::new(pm).into(),
                10000,
                genesis_time,
                ctx,
            );
            view_client_addr_slot = Some(view_client_addr);
            shards_manager_adapter_slot = Some(shards_manager_adapter);
            *genesis_block1.write().unwrap() = Some(block);
            client
        });
        ret.push(ActorHandlesForTesting {
            client_actor: client_addr,
            view_client_actor: view_client_addr_slot.unwrap(),
            shards_manager_adapter: shards_manager_adapter_slot.unwrap(),
        });
    }
    hash_to_height.write().unwrap().insert(CryptoHash::default(), 0);
    hash_to_height
        .write()
        .unwrap()
        .insert(*genesis_block.read().unwrap().as_ref().unwrap().header().clone().hash(), 0);
    connectors.set(ret.clone()).ok().unwrap();
    let value = genesis_block.read().unwrap();
    (value.clone().unwrap(), ret, block_stats)
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
) -> ActorHandlesForTesting {
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
) -> ActorHandlesForTesting {
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
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: ShardsManagerAdapterForTest,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    rng_seed: RngSeed,
    archive: bool,
    save_trie_changes: bool,
    make_state_snapshot_callback: Option<MakeSnapshotCallback>,
) -> Client {
    let validator_signer =
        account_id.map(|x| Arc::new(create_test_signer(x.as_str())) as Arc<dyn ValidatorSigner>);
    let mut config = ClientConfig::test(
        true,
        10,
        20,
        num_validator_seats,
        archive,
        save_trie_changes,
        true,
        true,
    );
    config.epoch_length = chain_genesis.epoch_length;
    let mut client = Client::new(
        config,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter,
        shards_manager_adapter.client,
        validator_signer,
        enable_doomslug,
        rng_seed,
        make_state_snapshot_callback,
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
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: ShardsManagerAdapterForTest,
    chain_genesis: ChainGenesis,
    rng_seed: RngSeed,
    archive: bool,
    save_trie_changes: bool,
) -> Client {
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let epoch_manager =
        MockEpochManager::new_with_validators(store.clone(), vs, chain_genesis.epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
    setup_client_with_runtime(
        num_validator_seats,
        account_id,
        enable_doomslug,
        network_adapter,
        shards_manager_adapter,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        rng_seed,
        archive,
        save_trie_changes,
        None,
    )
}

pub fn setup_synchronous_shards_manager(
    account_id: Option<AccountId>,
    client_adapter: Sender<ShardsManagerResponse>,
    network_adapter: PeerManagerAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_genesis: &ChainGenesis,
) -> ShardsManagerAdapterForTest {
    // Initialize the chain, to make sure that if the store is empty, we write the genesis
    // into the store, and as a short cut to get the parameters needed to instantiate
    // ShardsManager. This way we don't have to wait to construct the Client first.
    // TODO(#8324): This should just be refactored so that we can construct Chain first
    // before anything else.
    let chain = Chain::new(
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime,
        chain_genesis,
        DoomslugThresholdMode::TwoThirds, // irrelevant
        ChainConfig {
            save_trie_changes: true,
            background_migration_threads: 1,
            state_snapshot_every_n_blocks: None,
        }, // irrelevant
        None,
    )
    .unwrap();
    let chain_head = chain.head().unwrap();
    let chain_header_head = chain.header_head().unwrap();
    let shards_manager = ShardsManager::new(
        time::Clock::real(),
        account_id,
        epoch_manager,
        shard_tracker,
        network_adapter.request_sender,
        client_adapter,
        chain.store().new_read_only_chunks_store(),
        chain_head,
        chain_header_head,
    );
    Arc::new(SynchronousShardsManagerAdapter::new(shards_manager)).into()
}

pub fn setup_client_with_synchronous_shards_manager(
    store: Store,
    vs: ValidatorSchedule,
    account_id: Option<AccountId>,
    enable_doomslug: bool,
    network_adapter: PeerManagerAdapter,
    client_adapter: Sender<ShardsManagerResponse>,
    chain_genesis: ChainGenesis,
    rng_seed: RngSeed,
    archive: bool,
    save_trie_changes: bool,
) -> Client {
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let epoch_manager =
        MockEpochManager::new_with_validators(store.clone(), vs, chain_genesis.epoch_length);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime = KeyValueRuntime::new(store, epoch_manager.as_ref());
    let shards_manager_adapter = setup_synchronous_shards_manager(
        account_id.clone(),
        client_adapter,
        network_adapter.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        &chain_genesis,
    );
    setup_client_with_runtime(
        num_validator_seats,
        account_id,
        enable_doomslug,
        network_adapter,
        shards_manager_adapter,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        rng_seed,
        archive,
        save_trie_changes,
        None,
    )
}

/// A combined trait bound for both the client side and network side of the ShardsManager API.
#[derive(Clone, derive_more::AsRef)]
pub struct ShardsManagerAdapterForTest {
    pub client: Sender<ShardsManagerRequestFromClient>,
    pub network: Sender<ShardsManagerRequestFromNetwork>,
}

impl<A: CanSend<ShardsManagerRequestFromClient> + CanSend<ShardsManagerRequestFromNetwork>>
    From<Arc<A>> for ShardsManagerAdapterForTest
{
    fn from(arc: Arc<A>) -> Self {
        Self { client: arc.as_sender(), network: arc.as_sender() }
    }
}

/// An environment for writing integration tests with multiple clients.
/// This environment can simulate near nodes without network and it can be configured to use different runtimes.
pub struct TestEnv {
    pub chain_genesis: ChainGenesis,
    pub validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockPeerManagerAdapter>>,
    pub client_adapters: Vec<Arc<MockClientAdapterForShardsManager>>,
    pub shards_manager_adapters: Vec<ShardsManagerAdapterForTest>,
    pub clients: Vec<Client>,
    account_to_client_index: HashMap<AccountId, usize>,
    paused_blocks: Arc<Mutex<HashMap<CryptoHash, Arc<OnceCell<()>>>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    seeds: HashMap<AccountId, RngSeed>,
    archive: bool,
    save_trie_changes: bool,
}

#[derive(derive_more::From, Clone)]
enum EpochManagerKind {
    Mock(Arc<MockEpochManager>),
    Handle(Arc<EpochManagerHandle>),
}

impl EpochManagerKind {
    pub fn into_adapter(self) -> Arc<dyn EpochManagerAdapter> {
        match self {
            Self::Mock(mock) => mock,
            Self::Handle(handle) => handle,
        }
    }
}

/// A builder for the TestEnv structure.
pub struct TestEnvBuilder {
    chain_genesis: ChainGenesis,
    clients: Vec<AccountId>,
    validators: Vec<AccountId>,
    stores: Option<Vec<Store>>,
    epoch_managers: Option<Vec<EpochManagerKind>>,
    shard_trackers: Option<Vec<ShardTracker>>,
    runtimes: Option<Vec<Arc<dyn RuntimeAdapter>>>,
    network_adapters: Option<Vec<Arc<MockPeerManagerAdapter>>>,
    num_shards: Option<NumShards>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    seeds: HashMap<AccountId, RngSeed>,
    archive: bool,
    save_trie_changes: bool,
    add_state_snapshots: bool,
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
            stores: None,
            epoch_managers: None,
            shard_trackers: None,
            runtimes: None,
            network_adapters: None,
            num_shards: None,
            seeds,
            archive: false,
            save_trie_changes: true,
            add_state_snapshots: false,
        }
    }

    /// Sets list of client [`AccountId`]s to the one provided.  Panics if the
    /// vector is empty.
    pub fn clients(mut self, clients: Vec<AccountId>) -> Self {
        assert!(!clients.is_empty());
        assert!(self.stores.is_none(), "Cannot set clients after stores");
        assert!(self.epoch_managers.is_none(), "Cannot set clients after epoch_managers");
        assert!(self.shard_trackers.is_none(), "Cannot set clients after shard_trackers");
        assert!(self.runtimes.is_none(), "Cannot set clients after runtimes");
        assert!(self.network_adapters.is_none(), "Cannot set clients after network_adapters");
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
        assert!(self.epoch_managers.is_none(), "Cannot set validators after epoch_managers");
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

    /// Overrides the stores that are used to create epoch managers and runtimes.
    pub fn stores(mut self, stores: Vec<Store>) -> Self {
        assert_eq!(stores.len(), self.clients.len());
        assert!(self.stores.is_none(), "Cannot override twice");
        assert!(self.epoch_managers.is_none(), "Cannot override store after epoch_managers");
        assert!(self.runtimes.is_none(), "Cannot override store after runtimes");
        self.stores = Some(stores);
        self
    }

    /// Internal impl to make sure the stores are initialized.
    fn ensure_stores(self) -> Self {
        if self.stores.is_some() {
            self
        } else {
            let num_clients = self.clients.len();
            self.stores((0..num_clients).map(|_| create_test_store()).collect())
        }
    }

    /// Specifies custom MockEpochManager for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn mock_epoch_managers(mut self, epoch_managers: Vec<Arc<MockEpochManager>>) -> Self {
        assert_eq!(epoch_managers.len(), self.clients.len());
        assert!(self.epoch_managers.is_none(), "Cannot override twice");
        assert!(
            self.num_shards.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        assert!(
            self.shard_trackers.is_none(),
            "Cannot override epoch_managers after shard_trackers"
        );
        assert!(self.runtimes.is_none(), "Cannot override epoch_managers after runtimes");
        self.epoch_managers =
            Some(epoch_managers.into_iter().map(|epoch_manager| epoch_manager.into()).collect());
        self
    }

    /// Specifies custom EpochManagerHandle for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    ///
    /// The vector must have the same number of elements as they are clients
    /// (one by default).  If that does not hold, [`Self::build`] method will
    /// panic.
    pub fn epoch_managers(mut self, epoch_managers: Vec<Arc<EpochManagerHandle>>) -> Self {
        assert_eq!(epoch_managers.len(), self.clients.len());
        assert!(self.epoch_managers.is_none(), "Cannot override twice");
        assert!(
            self.num_shards.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        assert!(
            self.shard_trackers.is_none(),
            "Cannot override epoch_managers after shard_trackers"
        );
        assert!(self.runtimes.is_none(), "Cannot override epoch_managers after runtimes");
        self.epoch_managers =
            Some(epoch_managers.into_iter().map(|epoch_manager| epoch_manager.into()).collect());
        self
    }

    /// Constructs real EpochManager implementations for each instance.
    pub fn real_epoch_managers(self, genesis_config: &GenesisConfig) -> Self {
        assert!(
            self.num_shards.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        let ret = self.ensure_stores();
        let epoch_managers = (0..ret.clients.len())
            .map(|i| {
                EpochManager::new_arc_handle(
                    ret.stores.as_ref().unwrap()[i].clone(),
                    genesis_config,
                )
            })
            .collect();
        ret.epoch_managers(epoch_managers)
    }

    /// Internal impl to make sure EpochManagers are initialized.
    fn ensure_epoch_managers(self) -> Self {
        let mut ret = self.ensure_stores();
        if ret.epoch_managers.is_some() {
            ret
        } else {
            let epoch_managers: Vec<EpochManagerKind> = (0..ret.clients.len())
                .map(|i| {
                    let vs = ValidatorSchedule::new_with_shards(ret.num_shards.unwrap_or(1))
                        .block_producers_per_epoch(vec![ret.validators.clone()]);
                    MockEpochManager::new_with_validators(
                        ret.stores.as_ref().unwrap()[i].clone(),
                        vs,
                        ret.chain_genesis.epoch_length,
                    )
                    .into()
                })
                .collect();
            assert!(
                ret.shard_trackers.is_none(),
                "Cannot override shard_trackers without overriding epoch_managers"
            );
            assert!(
                ret.runtimes.is_none(),
                "Cannot override runtimes without overriding epoch_managers"
            );
            ret.epoch_managers = Some(epoch_managers);
            ret
        }
    }

    /// Visible for extension methods in integration-tests.
    pub fn internal_ensure_epoch_managers_for_nightshade_runtime(
        self,
    ) -> (Self, Vec<Store>, Vec<Arc<EpochManagerHandle>>) {
        let builder = self.ensure_epoch_managers();
        let stores = builder.stores.clone().unwrap();
        let epoch_managers = builder
            .epoch_managers
            .clone()
            .unwrap()
            .into_iter()
            .map(|kind| match kind {
                EpochManagerKind::Mock(_) => {
                    panic!("NightshadeRuntime can only be instantiated with EpochManagerHandle")
                }
                EpochManagerKind::Handle(handle) => handle,
            })
            .collect();
        (builder, stores, epoch_managers)
    }

    /// Specifies custom ShardTracker for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    pub fn shard_trackers(mut self, shard_trackers: Vec<ShardTracker>) -> Self {
        assert_eq!(shard_trackers.len(), self.clients.len());
        assert!(self.shard_trackers.is_none(), "Cannot override twice");
        self.shard_trackers = Some(shard_trackers);
        self
    }

    /// Constructs ShardTracker that tracks all shards for each instance.
    ///
    /// Note that in order to track *NO* shards, just don't override shard_trackers.
    pub fn track_all_shards(self) -> Self {
        let ret = self.ensure_epoch_managers();
        let shard_trackers = ret
            .epoch_managers
            .as_ref()
            .unwrap()
            .iter()
            .map(|epoch_manager| {
                ShardTracker::new(TrackedConfig::AllShards, epoch_manager.clone().into_adapter())
            })
            .collect();
        ret.shard_trackers(shard_trackers)
    }

    /// Internal impl to make sure ShardTrackers are initialized.
    fn ensure_shard_trackers(self) -> Self {
        let ret = self.ensure_epoch_managers();
        if ret.shard_trackers.is_some() {
            ret
        } else {
            let shard_trackers = ret
                .epoch_managers
                .as_ref()
                .unwrap()
                .iter()
                .map(|epoch_manager| {
                    ShardTracker::new(
                        TrackedConfig::new_empty(),
                        epoch_manager.clone().into_adapter(),
                    )
                })
                .collect();
            ret.shard_trackers(shard_trackers)
        }
    }

    /// Specifies custom RuntimeAdapter for each client.  This allows us to
    /// construct [`TestEnv`] with a custom implementation.
    pub fn runtimes(mut self, runtimes: Vec<Arc<dyn RuntimeAdapter>>) -> Self {
        assert_eq!(runtimes.len(), self.clients.len());
        assert!(self.runtimes.is_none(), "Cannot override twice");
        self.runtimes = Some(runtimes);
        self
    }

    /// Internal impl to make sure runtimes are initialized.
    fn ensure_runtimes(self) -> Self {
        let ret = self.ensure_epoch_managers();
        if ret.runtimes.is_some() {
            ret
        } else {
            let runtimes = (0..ret.clients.len())
                .map(|i| {
                    let epoch_manager = match &ret.epoch_managers.as_ref().unwrap()[i] {
                        EpochManagerKind::Mock(mock) => mock.as_ref(),
                        EpochManagerKind::Handle(_) => {
                            panic!(
                                "Can only default construct KeyValueRuntime with MockEpochManager"
                            )
                        }
                    };
                    KeyValueRuntime::new(ret.stores.as_ref().unwrap()[i].clone(), epoch_manager)
                        as Arc<dyn RuntimeAdapter>
                })
                .collect();
            ret.runtimes(runtimes)
        }
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

    /// Internal impl to make sure network adapters are initialized.
    fn ensure_network_adapters(self) -> Self {
        if self.network_adapters.is_some() {
            self
        } else {
            let num_clients = self.clients.len();
            self.network_adapters((0..num_clients).map(|_| Arc::new(Default::default())).collect())
        }
    }

    pub fn num_shards(mut self, num_shards: NumShards) -> Self {
        assert!(
            self.epoch_managers.is_none(),
            "Cannot set both num_shards and epoch_managers at the same time"
        );
        self.num_shards = Some(num_shards);
        self
    }

    pub fn archive(mut self, archive: bool) -> Self {
        self.archive = archive;
        self
    }

    pub fn save_trie_changes(mut self, save_trie_changes: bool) -> Self {
        self.save_trie_changes = save_trie_changes;
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
        self.ensure_shard_trackers().ensure_runtimes().ensure_network_adapters().build_impl()
    }

    fn build_impl(self) -> TestEnv {
        let chain_genesis = self.chain_genesis;
        let clients = self.clients.clone();
        let num_clients = clients.len();
        let validators = self.validators;
        let num_validators = validators.len();
        let seeds = self.seeds;
        let epoch_managers = self.epoch_managers.unwrap();
        let shard_trackers = self.shard_trackers.unwrap();
        let runtimes = self.runtimes.unwrap();
        let network_adapters = self.network_adapters.unwrap();
        let client_adapters = (0..num_clients)
            .map(|_| Arc::new(MockClientAdapterForShardsManager::default()))
            .collect::<Vec<_>>();
        let shards_manager_adapters = (0..num_clients)
            .map(|i| {
                let epoch_manager = epoch_managers[i].clone();
                let shard_tracker = shard_trackers[i].clone();
                let runtime = runtimes[i].clone();
                let network_adapter = network_adapters[i].clone();
                let client_adapter = client_adapters[i].clone();
                setup_synchronous_shards_manager(
                    Some(clients[i].clone()),
                    client_adapter.as_sender(),
                    network_adapter.into(),
                    epoch_manager.into_adapter(),
                    shard_tracker,
                    runtime,
                    &chain_genesis,
                )
            })
            .collect::<Vec<_>>();
        let clients = (0..num_clients)
            .map(|i| {
                let account_id = clients[i].clone();
                let network_adapter = network_adapters[i].clone();
                let shards_manager_adapter = shards_manager_adapters[i].clone();
                let epoch_manager = epoch_managers[i].clone();
                let shard_tracker = shard_trackers[i].clone();
                let runtime = runtimes[i].clone();
                let rng_seed = match seeds.get(&account_id) {
                    Some(seed) => *seed,
                    None => TEST_SEED,
                };
                let make_state_snapshot_callback : Option<MakeSnapshotCallback> = if self.add_state_snapshots {
                    let runtime = runtime.clone();
                    let snapshot : MakeSnapshotCallback = Arc::new(move |prev_block_hash, shard_uids| {
                        tracing::info!(target: "state_snapshot", ?prev_block_hash, "make_snapshot_callback");
                        runtime.get_tries().make_state_snapshot(&prev_block_hash, &shard_uids).unwrap();
                    });
                    Some(snapshot)
                } else {
                    None
                };
                setup_client_with_runtime(
                    u64::try_from(num_validators).unwrap(),
                    Some(account_id),
                    false,
                    network_adapter.into(),
                    shards_manager_adapter,
                    chain_genesis.clone(),
                    epoch_manager.into_adapter(),
                    shard_tracker,
                    runtime,
                    rng_seed,
                    self.archive,
                    self.save_trie_changes,
                    make_state_snapshot_callback,
                )
            })
            .collect();

        TestEnv {
            chain_genesis,
            validators,
            network_adapters,
            client_adapters,
            shards_manager_adapters,
            clients,
            account_to_client_index: self
                .clients
                .into_iter()
                .enumerate()
                .map(|(index, client)| (client, index))
                .collect(),
            paused_blocks: Default::default(),
            seeds,
            archive: self.archive,
            save_trie_changes: self.save_trie_changes,
        }
    }

    fn make_accounts(count: usize) -> Vec<AccountId> {
        (0..count).map(|i| format!("test{}", i).parse().unwrap()).collect()
    }

    pub fn use_state_snapshots(mut self) -> Self {
        self.add_state_snapshots = true;
        self
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

    pub fn shards_manager(&self, account: &AccountId) -> &ShardsManagerAdapterForTest {
        &self.shards_manager_adapters[self.account_to_client_index[account]]
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
                    self.shards_manager(&account_id).send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                            PartialEncodedChunk::from(partial_encoded_chunk),
                        ),
                    );
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
            if let Some(response) = response {
                self.shards_manager_adapters[id].send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                        partial_encoded_chunk_response: response,
                        received_time: Instant::now(),
                    },
                );
            }
        } else {
            panic!("The request is not a PartialEncodedChunk request {:?}", request);
        }
    }

    pub fn get_partial_encoded_chunk_response(
        &mut self,
        id: usize,
        request: PartialEncodedChunkRequestMsg,
    ) -> Option<PartialEncodedChunkResponseMsg> {
        self.shards_manager_adapters[id].send(
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request: request.clone(),
                route_back: CryptoHash::default(),
            },
        );
        let response = self.network_adapters[id].pop_most_recent();
        match response {
            Some(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedChunkResponse { route_back: _, response },
            )) => return Some(response),
            Some(response) => {
                self.network_adapters[id].put_back_most_recent(response);
            }
            None => {}
        }

        panic!(
            "Failed to process PartialEncodedChunkRequest from shards manager {}: {:?}",
            id, request
        );
    }

    pub fn process_shards_manager_responses(&mut self, id: usize) -> bool {
        let mut any_processed = false;
        while let Some(msg) = self.client_adapters[id].pop() {
            match msg {
                ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                    self.clients[id].on_chunk_completed(
                        partial_chunk,
                        shard_chunk,
                        Arc::new(|_| {}),
                    );
                }
                ShardsManagerResponse::InvalidChunk(encoded_chunk) => {
                    self.clients[id].on_invalid_chunk(encoded_chunk);
                }
                ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                    chunk_header,
                    chunk_producer,
                } => {
                    self.clients[id]
                        .on_chunk_header_ready_for_inclusion(chunk_header, chunk_producer);
                }
            }
            any_processed = true;
        }
        any_processed
    }

    pub fn process_shards_manager_responses_and_finish_processing_blocks(&mut self, idx: usize) {
        loop {
            self.process_shards_manager_responses(idx);
            if self.clients[idx].finish_blocks_in_processing().is_empty() {
                return;
            }
        }
    }

    pub fn send_money(&mut self, id: usize) -> ProcessTxResponse {
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

    /// This function will actually bump to the latest protocol version instead of the provided one.
    /// See https://github.com/near/nearcore/issues/8590 for details.
    pub fn upgrade_protocol(&mut self, protocol_version: ProtocolVersion) {
        assert_eq!(self.clients.len(), 1, "at the moment, this support only a single client");

        let tip = self.clients[0].chain.head().unwrap();
        let epoch_id = self.clients[0]
            .epoch_manager
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            self.clients[0].epoch_manager.get_block_producer(&epoch_id, tip.height).unwrap();

        let mut block = self.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        eprintln!("Producing block with version {protocol_version}");
        block.mut_header().set_latest_protocol_version(protocol_version);
        block.mut_header().resign(&create_test_signer(block_producer.as_str()));

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

    /// Restarts client at given index. Note that the new client reuses runtime
    /// adapter of old client.
    /// TODO (#8269): create new `KeyValueRuntime` for new client. Currently it
    /// doesn't work because `KeyValueRuntime` misses info about new epochs in
    /// memory caches.
    /// Though, it seems that it is not necessary for current use cases.
    pub fn restart(&mut self, idx: usize) {
        let account_id = self.get_client_id(idx).clone();
        let rng_seed = match self.seeds.get(&account_id) {
            Some(seed) => *seed,
            None => TEST_SEED,
        };
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![self.validators.clone()]);
        let num_validator_seats = vs.all_block_producers().count() as NumSeats;
        self.clients[idx] = setup_client_with_runtime(
            num_validator_seats,
            Some(self.get_client_id(idx).clone()),
            false,
            self.network_adapters[idx].clone().into(),
            self.shards_manager_adapters[idx].clone(),
            self.chain_genesis.clone(),
            self.clients[idx].epoch_manager.clone(),
            self.clients[idx].shard_tracker.clone(),
            self.clients[idx].runtime_adapter.clone(),
            rng_seed,
            self.archive,
            self.save_trie_changes,
            None,
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

    /// Wrap actions in a delegate action, put it in a transaction, sign.
    pub fn meta_tx_from_actions(
        &mut self,
        actions: Vec<Action>,
        sender: AccountId,
        relayer: AccountId,
        receiver_id: AccountId,
    ) -> SignedTransaction {
        let inner_signer = InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, &sender);
        let relayer_signer = InMemorySigner::from_seed(relayer.clone(), KeyType::ED25519, &relayer);
        let tip = self.clients[0].chain.head().unwrap();
        let user_nonce = tip.height + 1;
        let relayer_nonce = tip.height + 1;
        let delegate_action = DelegateAction {
            sender_id: inner_signer.account_id.clone(),
            receiver_id,
            actions: actions
                .into_iter()
                .map(|action| NonDelegateAction::try_from(action).unwrap())
                .collect(),
            nonce: user_nonce,
            max_block_height: tip.height + 100,
            public_key: inner_signer.public_key(),
        };
        let signature = inner_signer.sign(delegate_action.get_nep461_hash().as_bytes());
        let signed_delegate_action = SignedDelegateAction { delegate_action, signature };
        SignedTransaction::from_actions(
            relayer_nonce,
            relayer,
            sender,
            &relayer_signer,
            vec![Action::Delegate(signed_delegate_action)],
            tip.last_block_hash,
        )
    }

    /// Process a tx and its receipts, then return the execution outcome.
    pub fn execute_tx(
        &mut self,
        tx: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let tx_hash = tx.get_hash();
        let response = self.clients[0].process_tx(tx, false, false);
        // Check if the transaction got rejected
        match response {
            ProcessTxResponse::NoResponse
            | ProcessTxResponse::RequestRouted
            | ProcessTxResponse::ValidTx => (),
            ProcessTxResponse::InvalidTx(e) => return Err(e),
            ProcessTxResponse::DoesNotTrackShard => panic!("test setup is buggy"),
        }
        let max_iters = 100;
        let tip = self.clients[0].chain.head().unwrap();
        for i in 0..max_iters {
            let block = self.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            self.process_block(0, block.clone(), Provenance::PRODUCED);
            if let Ok(outcome) = self.clients[0].chain.get_final_transaction_result(&tx_hash) {
                return Ok(outcome);
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
        self.execute_tx(tx).unwrap()
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
            &client.epoch_manager.get_epoch_id_from_prev_block(&last_block_hash).unwrap(),
            Chain::get_prev_chunk_header(client.epoch_manager.as_ref(), &last_block, shard_id)
                .unwrap(),
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
        let total_parts = client.chain.epoch_manager.num_total_parts();
        let data_parts = client.chain.epoch_manager.num_data_parts();
        let decoded_chunk = chunk.decode_chunk(data_parts).unwrap();
        let parity_parts = total_parts - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

        let signer = client.validator_signer.as_ref().unwrap().clone();
        let header = chunk.cloned_header();
        let (mut encoded_chunk, mut new_merkle_paths) = EncodedShardChunk::new(
            *header.prev_block_hash(),
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
    highest_height_peers: &[HighestHeightPeerInfo],
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
    let _ = System::new();
    let state_parts_arbiter_handle = Arbiter::new().handle();
    loop {
        client.run_catchup(
            highest_height_peers,
            &f,
            &block_catch_up,
            &state_split,
            Arc::new(|_| {}),
            &state_parts_arbiter_handle,
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
            let sync_hash = msg.sync_hash;
            let shard_id = msg.shard_id;
            let results = Chain::build_state_for_split_shards(msg);
            if let Some((sync, _, _)) = client.catchup_state_syncs.get_mut(&sync_hash) {
                // We are doing catchup
                sync.set_split_result(shard_id, results);
            } else {
                client.state_sync.set_split_result(shard_id, results);
            }
            catchup_done = false;
        }
        if catchup_done {
            break;
        }
    }
    Ok(())
}
