//! Client actor orchestrates Client and facilitates network connection.
//! It should just serve as a coordinator class to handle messages and check triggers but immediately
//! pass the control to Client. This means, any real block processing or production logic should
//! be put in Client.
//! Unfortunately, this is not the case today. We are in the process of refactoring ClientActor
//! https://github.com/near/nearcore/issues/7899

use crate::adapter::{
    BlockApproval, BlockHeadersResponse, BlockResponse, ProcessTxRequest, ProcessTxResponse,
    RecvChallenge, SetNetworkInfo, StateResponse,
};
use crate::client::{Client, EPOCH_START_INFO_BLOCKS};
use crate::config_updater::ConfigUpdater;
use crate::debug::new_network_info_view;
use crate::info::{display_sync_status, InfoHelper};
use crate::sync::state::{StateSync, StateSyncResult};
use crate::sync_jobs_actor::{create_sync_job_scheduler, SyncJobsActor};
use crate::{metrics, StatusResponse};
use actix::{Actor, Addr, Arbiter, AsyncContext, Context, Handler};
use actix_rt::ArbiterHandle;
use chrono::{DateTime, Utc};
use near_async::messaging::{CanSend, Sender};
use near_chain::chain::{
    ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest, BlockCatchUpResponse,
};
use near_chain::resharding::{StateSplitRequest, StateSplitResponse};
use near_chain::state_snapshot_actor::MakeSnapshotCallback;
use near_chain::test_utils::format_hash;
use near_chain::types::RuntimeAdapter;
#[cfg(feature = "test_features")]
use near_chain::ChainStoreAccess;
use near_chain::{
    byzantine_assert, near_chain_primitives, Block, BlockHeader, BlockProcessingArtifact,
    ChainGenesis, DoneApplyChunkCallback, Provenance,
};
use near_chain_configs::{ClientConfig, LogSummaryStyle};
use near_chain_primitives::error::EpochErrorResultToChainError;
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::logic::cares_about_shard_this_or_next_epoch;
use near_client_primitives::types::{
    Error, GetClientConfig, GetClientConfigError, GetNetworkInfo, NetworkInfoResponse,
    StateSyncStatus, Status, StatusError, StatusSyncInfo, SyncStatus,
};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::ReasonForBan;
use near_network::types::{
    NetworkInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics;
use near_performance_metrics_macros::perf;
use near_primitives::block::Tip;
use near_primitives::block_header::ApprovalType;
use near_primitives::epoch_manager::RngSeed;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::static_clock::StaticClock;
use near_primitives::types::BlockHeight;
use near_primitives::unwrap_or_return;
use near_primitives::utils::{from_timestamp, MaybeValidated};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{DetailedDebugStatus, ValidatorInfo};
#[cfg(feature = "test_features")]
use near_store::DBCol;
use near_telemetry::TelemetryActor;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace, warn};

/// Multiplier on `max_block_time` to wait until deciding that chain stalled.
const STATUS_WAIT_TIME_MULTIPLIER: u64 = 10;
/// `max_block_production_time` times this multiplier is how long we wait before rebroadcasting
/// the current `head`
const HEAD_STALL_MULTIPLIER: u32 = 4;

pub struct ClientActor {
    /// Adversarial controls
    pub adv: crate::adversarial::Controls,

    // Address of this ClientActor. Can be used to send messages to self.
    my_address: Addr<ClientActor>,
    pub(crate) client: Client,
    network_adapter: PeerManagerAdapter,
    network_info: NetworkInfo,
    /// Identity that represents this Client at the network level.
    /// It is used as part of the messages that identify this client.
    node_id: PeerId,
    /// Last time we announced our accounts as validators.
    last_validator_announce_time: Option<Instant>,
    /// Info helper.
    info_helper: InfoHelper,

    /// Last time handle_block_production method was called
    block_production_next_attempt: DateTime<Utc>,

    // Last time when log_summary method was called.
    log_summary_timer_next_attempt: DateTime<Utc>,

    block_production_started: bool,
    doomslug_timer_next_attempt: DateTime<Utc>,
    sync_timer_next_attempt: DateTime<Utc>,
    sync_started: bool,
    state_parts_task_scheduler: Box<dyn Fn(ApplyStatePartsRequest)>,
    block_catch_up_scheduler: Box<dyn Fn(BlockCatchUpRequest)>,
    state_split_scheduler: Box<dyn Fn(StateSplitRequest)>,
    state_parts_client_arbiter: Arbiter,

    #[cfg(feature = "sandbox")]
    fastforward_delta: near_primitives::types::BlockHeightDelta,

    /// Synchronization measure to allow graceful shutdown.
    /// Informs the system when a ClientActor gets dropped.
    shutdown_signal: Option<broadcast::Sender<()>>,

    /// Manages updating the config.
    config_updater: Option<ConfigUpdater>,
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    loop {
        // Get chrono::Duration::num_seconds() by deducting genesis_time from now.
        let duration = genesis_time.signed_duration_since(StaticClock::utc());
        let chrono_seconds = duration.num_seconds();
        // Check if number of seconds in chrono::Duration larger than zero.
        if chrono_seconds <= 0 {
            break;
        }
        info!(target: "near", "Waiting until genesis: {}d {}h {}m {}s", duration.num_days(),
              (duration.num_hours() % 24),
              (duration.num_minutes() % 60),
              (duration.num_seconds() % 60));
        let wait =
            std::cmp::min(Duration::from_secs(10), Duration::from_secs(chrono_seconds as u64));
        thread::sleep(wait);
    }
}

impl ClientActor {
    pub fn new(
        client: Client,
        address: Addr<ClientActor>,
        config: ClientConfig,
        node_id: PeerId,
        network_adapter: PeerManagerAdapter,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        telemetry_actor: Addr<TelemetryActor>,
        ctx: &Context<ClientActor>,
        shutdown_signal: Option<broadcast::Sender<()>>,
        adv: crate::adversarial::Controls,
        config_updater: Option<ConfigUpdater>,
    ) -> Result<Self, Error> {
        let state_parts_arbiter = Arbiter::new();
        let self_addr = ctx.address();
        let self_addr_clone = self_addr;
        let sync_jobs_actor_addr = SyncJobsActor::start_in_arbiter(
            &state_parts_arbiter.handle(),
            move |ctx: &mut Context<SyncJobsActor>| -> SyncJobsActor {
                ctx.set_mailbox_capacity(SyncJobsActor::MAILBOX_CAPACITY);
                SyncJobsActor { client_addr: self_addr_clone }
            },
        );
        if let Some(vs) = &validator_signer {
            info!(target: "client", "Starting validator node: {}", vs.validator_id());
        }
        let info_helper = InfoHelper::new(Some(telemetry_actor), &config, validator_signer.clone());

        let now = Utc::now();
        Ok(ClientActor {
            adv,
            my_address: address,
            client,
            network_adapter,
            node_id,
            network_info: NetworkInfo {
                connected_peers: vec![],
                tier1_connections: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                tier1_accounts_keys: vec![],
                tier1_accounts_data: vec![],
            },
            last_validator_announce_time: None,
            info_helper,
            block_production_next_attempt: now,
            log_summary_timer_next_attempt: now,
            block_production_started: false,
            doomslug_timer_next_attempt: now,
            sync_timer_next_attempt: now,
            sync_started: false,
            state_parts_task_scheduler: create_sync_job_scheduler::<ApplyStatePartsRequest>(
                sync_jobs_actor_addr.clone(),
            ),
            block_catch_up_scheduler: create_sync_job_scheduler::<BlockCatchUpRequest>(
                sync_jobs_actor_addr.clone(),
            ),
            state_split_scheduler: create_sync_job_scheduler::<StateSplitRequest>(
                sync_jobs_actor_addr,
            ),
            state_parts_client_arbiter: state_parts_arbiter,

            #[cfg(feature = "sandbox")]
            fastforward_delta: 0,
            shutdown_signal,
            config_updater,
        })
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_flat_storage_creation(ctx);

        // Start syncing job.
        self.start_sync(ctx);

        // Start block production tracking if have block producer info.
        if self.client.validator_signer.is_some() {
            self.block_production_started = true;
        }

        // Start triggers
        self.schedule_triggers(ctx);

        // Start catchup job.
        self.catchup(ctx);

        if let Err(err) = self.client.send_network_chain_info() {
            error!(target: "client", ?err, "Failed to update network chain info");
        }
    }
}

impl ClientActor {
    /// Wrapper for processing actix message which must be called after receiving it.
    ///
    /// Due to a bug in Actix library, while there are messages in mailbox, Actix
    /// will prioritize processing messages until mailbox is empty. In such case execution
    /// of any other task scheduled with `run_later` will be delayed. At the same time,
    /// we have several important functions which have to be called regularly, so we put
    /// these calls into `check_triggers` and call it here as a quick hack.
    fn wrap<Req: std::fmt::Debug + actix::Message, Res>(
        &mut self,
        msg: WithSpanContext<Req>,
        ctx: &mut Context<Self>,
        msg_type: &str,
        f: impl FnOnce(&mut Self, Req) -> Res,
    ) -> Res {
        let (_span, msg) = handler_debug_span!(target: "client", msg, msg_type);
        self.check_triggers(ctx);
        let _d =
            delay_detector::DelayDetector::new(|| format!("NetworkClientMessage {:?}", msg).into());
        metrics::CLIENT_MESSAGES_COUNT.with_label_values(&[msg_type]).inc();
        let timer =
            metrics::CLIENT_MESSAGES_PROCESSING_TIME.with_label_values(&[msg_type]).start_timer();
        let res = f(self, msg);
        timer.observe_duration();
        res
    }
}

#[cfg(feature = "test_features")]
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<u64>")]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvSwitchToHeight(u64),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvCheckStorageConsistency,
}

#[cfg(feature = "test_features")]
impl Handler<WithSpanContext<NetworkAdversarialMessage>> for ClientActor {
    type Result = Option<u64>;

    fn handle(
        &mut self,
        msg: WithSpanContext<NetworkAdversarialMessage>,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        self.wrap(msg, ctx, "NetworkAdversarialMessage", |this, msg| match msg {
            NetworkAdversarialMessage::AdvDisableDoomslug => {
                info!(target: "adversary", "Turning Doomslug off");
                this.adv.set_disable_doomslug(true);
                this.client.doomslug.adv_disable();
                this.client.chain.adv_disable_doomslug();
                None
            }
            NetworkAdversarialMessage::AdvDisableHeaderSync => {
                info!(target: "adversary", "Blocking header sync");
                this.adv.set_disable_header_sync(true);
                None
            }
            NetworkAdversarialMessage::AdvProduceBlocks(
                num_blocks,
                only_valid,
            ) => {
                info!(target: "adversary", "Producing {} blocks", num_blocks);
                this.client.adv_produce_blocks = true;
                this.client.adv_produce_blocks_only_valid = only_valid;
                let start_height =
                    this.client.chain.mut_store().get_latest_known().unwrap().height + 1;
                let mut blocks_produced = 0;
                for height in start_height.. {
                    let block = this
                        .client
                        .produce_block(height)
                        .expect("block should be produced");
                    if only_valid && block == None {
                        continue;
                    }
                    let block = block.expect("block should exist after produced");
                    info!(target: "adversary", "Producing {} block out of {}, height = {}", blocks_produced, num_blocks, height);
                    this.network_adapter.send(
                        PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::Block { block: block.clone() },
                        )
                    );
                    let _ = this.client.start_process_block(
                        block.into(),
                        Provenance::PRODUCED,
                        this.get_apply_chunks_done_callback(),
                    );
                    blocks_produced += 1;
                    if blocks_produced == num_blocks {
                        break;
                    }
                }
                None
            }
            NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                info!(target: "adversary", "Switching to height {:?}", height);
                let mut chain_store_update = this.client.chain.mut_store().store_update();
                chain_store_update.save_largest_target_height(height);
                chain_store_update
                    .adv_save_latest_known(height)
                    .expect("adv method should not fail");
                chain_store_update.commit().expect("adv method should not fail");
                None
            }
            NetworkAdversarialMessage::AdvGetSavedBlocks => {
                info!(target: "adversary", "Requested number of saved blocks");
                let store = this.client.chain.store().store();
                let mut num_blocks = 0;
                for _ in store.iter(DBCol::Block) {
                    num_blocks += 1;
                }
                Some(num_blocks)
            }
            NetworkAdversarialMessage::AdvCheckStorageConsistency => {
                // timeout is set to 1.5 seconds to give some room as we wait in Nightly for 2 seconds
                let timeout = 1500;
                info!(target: "adversary", "Check Storage Consistency, timeout set to {:?} milliseconds", timeout);
                let mut genesis = near_chain_configs::GenesisConfig::default();
                genesis.genesis_height = this.client.chain.store().get_genesis_height();
                let mut store_validator = near_chain::store_validator::StoreValidator::new(
                    this.client.validator_signer.as_ref().map(|x| x.validator_id().clone()),
                    genesis,
                    this.client.epoch_manager.clone(),
                    this.client.shard_tracker.clone(),
                    this.client.runtime_adapter.clone(),
                    this.client.chain.store().store().clone(),
                    this.adv.is_archival(),
                );
                store_validator.set_timeout(timeout);
                store_validator.validate();
                if store_validator.is_failed() {
                    error!(target: "client", "Storage Validation failed, {:?}", store_validator.errors);
                    Some(0)
                } else {
                    Some(store_validator.tests_done())
                }
            }
        })
    }
}

impl Handler<WithSpanContext<ProcessTxRequest>> for ClientActor {
    type Result = ProcessTxResponse;

    fn handle(
        &mut self,
        msg: WithSpanContext<ProcessTxRequest>,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        self.wrap(msg, ctx, "ProcessTxRequest", |this: &mut Self, msg| {
            let ProcessTxRequest { transaction, is_forwarded, check_only } = msg;
            this.client.process_tx(transaction, is_forwarded, check_only)
        })
    }
}

impl Handler<WithSpanContext<BlockResponse>> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: WithSpanContext<BlockResponse>, ctx: &mut Context<Self>) {
        self.wrap(msg,ctx,"BlockResponse",|this:&mut Self,msg|{
            let BlockResponse{ block, peer_id, was_requested } = msg;
            let blocks_at_height = this
                .client
                .chain
                .store()
                .get_all_block_hashes_by_height(block.header().height());
            if was_requested || blocks_at_height.is_err() || blocks_at_height.as_ref().unwrap().is_empty() {
                if let SyncStatus::StateSync(StateSyncStatus{ sync_hash, .. }) = &mut this.client.sync_status {
                    if let Ok(header) = this.client.chain.get_block_header(sync_hash) {
                        if block.hash() == header.prev_hash() {
                            if let Err(e) = this.client.chain.save_block(block.into()) {
                                error!(target: "client", "Failed to save a block during state sync: {}", e);
                            }
                        } else if block.hash() == sync_hash {
                            // This is the immediate block after a state sync
                            // We can afford to delay requesting missing chunks for this one block
                            if let Err(e) = this.client.chain.save_orphan(block.into(), false) {
                                error!(target: "client", "Received an invalid block during state sync: {}", e);
                            }
                        }
                        return;
                    }
                }
                this.client.receive_block(
                    block,
                    peer_id,
                    was_requested,
                    this.get_apply_chunks_done_callback(),
                );
            } else {
                match this
                    .client
                    .epoch_manager
                    .get_epoch_id_from_prev_block(block.header().prev_hash())
                {
                    Ok(epoch_id) => {
                        if let Some(hashes) = blocks_at_height.unwrap().get(&epoch_id) {
                            if !hashes.contains(block.header().hash()) {
                                warn!(target: "client", "Rejecting unrequested block {}, height {}", block.header().hash(), block.header().height());
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
    }
}

impl Handler<WithSpanContext<BlockHeadersResponse>> for ClientActor {
    type Result = Result<(), ReasonForBan>;

    fn handle(
        &mut self,
        msg: WithSpanContext<BlockHeadersResponse>,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        self.wrap(msg, ctx, "BlockHeadersResponse", |this, msg| {
            let BlockHeadersResponse(headers, peer_id) = msg;
            if this.receive_headers(headers, peer_id) {
                Ok(())
            } else {
                warn!(target: "client", "Banning node for sending invalid block headers");
                Err(ReasonForBan::BadBlockHeader)
            }
        })
    }
}

impl Handler<WithSpanContext<BlockApproval>> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: WithSpanContext<BlockApproval>, ctx: &mut Context<Self>) {
        self.wrap(msg, ctx, "BlockApproval", |this, msg| {
            let BlockApproval(approval, peer_id) = msg;
            debug!(target: "client", "Receive approval {:?} from peer {:?}", approval, peer_id);
            this.client.collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id));
        })
    }
}

/// StateResponse is used during StateSync and catchup.
/// It contains either StateSync header information (that tells us how many parts there are etc) or a single part.
impl Handler<WithSpanContext<StateResponse>> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: WithSpanContext<StateResponse>, ctx: &mut Context<Self>) {
        self.wrap(msg,ctx,"StateResponse",|this,msg| {
            let StateResponse(state_response_info) = msg;
            let shard_id = state_response_info.shard_id();
            let hash = state_response_info.sync_hash();
            let state_response = state_response_info.take_state_response();

            trace!(target: "sync", "Received state response shard_id: {} sync_hash: {:?} part(id/size): {:?}",
                   shard_id,
                   hash,
                   state_response.part().as_ref().map(|(part_id, data)| (part_id, data.len()))
            );
            // Get the download that matches the shard_id and hash

            // ... It could be that the state was requested by the state sync
            if let SyncStatus::StateSync(StateSyncStatus{ sync_hash, sync_status: shards_to_download }) =
                &mut this.client.sync_status
            {
                if hash == *sync_hash {
                    if let Some(shard_download) = shards_to_download.get_mut(&shard_id) {
                        this.client.state_sync.update_download_on_state_response_message(shard_download, hash, shard_id, state_response, &mut this.client.chain);
                        return;
                    }
                }
            }

            // ... Or one of the catchups
            if let Some((state_sync, shards_to_download, _)) =
                this.client.catchup_state_syncs.get_mut(&hash)
            {
                if let Some(shard_download) = shards_to_download.get_mut(&shard_id) {
                    state_sync.update_download_on_state_response_message(shard_download, hash, shard_id, state_response, &mut this.client.chain);
                    return;
                }
            }

            error!(target: "sync", "State sync received hash {} that we're not expecting, potential malicious peer or a very delayed response.", hash);
        })
    }
}

impl Handler<WithSpanContext<RecvChallenge>> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: WithSpanContext<RecvChallenge>, ctx: &mut Context<Self>) {
        self.wrap(msg, ctx, "RecvChallenge", |this, msg| {
            let RecvChallenge(challenge) = msg;
            match this.client.process_challenge(challenge) {
                Ok(_) => {}
                Err(err) => error!(target: "client", "Error processing challenge: {}", err),
            }
        });
    }
}

impl Handler<WithSpanContext<SetNetworkInfo>> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: WithSpanContext<SetNetworkInfo>, ctx: &mut Context<Self>) {
        self.wrap(msg, ctx, "SetNetworkInfo", |this, msg| {
            let SetNetworkInfo(network_info) = msg;
            this.network_info = network_info;
        })
    }
}

#[cfg(feature = "sandbox")]
impl Handler<WithSpanContext<near_client_primitives::types::SandboxMessage>> for ClientActor {
    type Result = near_client_primitives::types::SandboxResponse;

    fn handle(
        &mut self,
        msg: WithSpanContext<near_client_primitives::types::SandboxMessage>,
        _ctx: &mut Context<Self>,
    ) -> near_client_primitives::types::SandboxResponse {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        match msg {
            near_client_primitives::types::SandboxMessage::SandboxPatchState(state) => {
                self.client.chain.patch_state(
                    near_primitives::sandbox::state_patch::SandboxStatePatch::new(state),
                );
                near_client_primitives::types::SandboxResponse::SandboxNoResponse
            }
            near_client_primitives::types::SandboxMessage::SandboxPatchStateStatus => {
                near_client_primitives::types::SandboxResponse::SandboxPatchStateFinished(
                    !self.client.chain.patch_state_in_progress(),
                )
            }
            near_client_primitives::types::SandboxMessage::SandboxFastForward(delta_height) => {
                if self.fastforward_delta > 0 {
                    return near_client_primitives::types::SandboxResponse::SandboxFastForwardFailed(
                        "Consecutive fast_forward requests cannot be made while a current one is going on.".to_string());
                }

                self.fastforward_delta = delta_height;
                near_client_primitives::types::SandboxResponse::SandboxNoResponse
            }
            near_client_primitives::types::SandboxMessage::SandboxFastForwardStatus => {
                near_client_primitives::types::SandboxResponse::SandboxFastForwardFinished(
                    self.fastforward_delta == 0,
                )
            }
        }
    }
}

impl Handler<WithSpanContext<Status>> for ClientActor {
    type Result = Result<StatusResponse, StatusError>;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<Status>, ctx: &mut Context<Self>) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let _d = delay_detector::DelayDetector::new(|| "client status".into());
        self.check_triggers(ctx);

        let head = self.client.chain.head()?;
        let head_header = self.client.chain.get_block_header(&head.last_block_hash)?;
        let latest_block_time = head_header.raw_timestamp();
        let latest_state_root = *head_header.prev_state_root();
        if msg.is_health_check {
            let now = Utc::now();
            let block_timestamp = from_timestamp(latest_block_time);
            if now > block_timestamp {
                let elapsed = (now - block_timestamp).to_std().unwrap();
                if elapsed
                    > Duration::from_millis(
                        self.client.config.max_block_production_delay.as_millis() as u64
                            * STATUS_WAIT_TIME_MULTIPLIER,
                    )
                {
                    return Err(StatusError::NoNewBlocks { elapsed });
                }
            }

            if self.client.sync_status.is_syncing() {
                return Err(StatusError::NodeIsSyncing);
            }
        }
        let validators: Vec<ValidatorInfo> = self
            .client
            .epoch_manager
            .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash)
            .into_chain_error()?
            .into_iter()
            .map(|(validator_stake, is_slashed)| ValidatorInfo {
                account_id: validator_stake.take_account_id(),
                is_slashed,
            })
            .collect();

        let epoch_start_height =
            self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash).ok();

        let protocol_version = self
            .client
            .epoch_manager
            .get_epoch_protocol_version(&head.epoch_id)
            .into_chain_error()?;

        let node_public_key = self.node_id.public_key().clone();
        let (validator_account_id, validator_public_key) = match &self.client.validator_signer {
            Some(vs) => (Some(vs.validator_id().clone()), Some(vs.public_key())),
            None => (None, None),
        };
        let node_key = validator_public_key.clone();

        let mut earliest_block_hash = None;
        let mut earliest_block_height = None;
        let mut earliest_block_time = None;
        if let Some(earliest_block_hash_value) = self.client.chain.get_earliest_block_hash()? {
            earliest_block_hash = Some(earliest_block_hash_value);
            if let Ok(earliest_block) =
                self.client.chain.get_block_header(&earliest_block_hash_value)
            {
                earliest_block_height = Some(earliest_block.height());
                earliest_block_time = Some(earliest_block.timestamp());
            }
        }
        // Provide more detailed information about the current state of chain.
        // For now - provide info about last 50 blocks.
        let detailed_debug_status = if msg.detailed {
            Some(DetailedDebugStatus {
                network_info: new_network_info_view(&self.client.chain, &self.network_info),
                sync_status: format!(
                    "{} ({})",
                    self.client.sync_status.as_variant_name().to_string(),
                    display_sync_status(
                        &self.client.sync_status,
                        &self.client.chain.head()?,
                        &self.client.config.state_sync.sync,
                    ),
                ),
                catchup_status: self.client.get_catchup_status()?,
                current_head_status: head.clone().into(),
                current_header_head_status: self.client.chain.header_head()?.into(),
                block_production_delay_millis: self
                    .client
                    .config
                    .min_block_production_delay
                    .as_millis() as u64,
            })
        } else {
            None
        };
        let uptime_sec = StaticClock::utc().timestamp() - self.info_helper.boot_time_seconds;
        Ok(StatusResponse {
            version: self.client.config.version.clone(),
            protocol_version,
            latest_protocol_version: PROTOCOL_VERSION,
            chain_id: self.client.config.chain_id.clone(),
            rpc_addr: self.client.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash,
                latest_block_height: head.height,
                latest_state_root,
                latest_block_time: from_timestamp(latest_block_time),
                syncing: self.client.sync_status.is_syncing(),
                earliest_block_hash,
                earliest_block_height,
                earliest_block_time,
                epoch_id: Some(head.epoch_id),
                epoch_start_height,
            },
            validator_account_id,
            validator_public_key,
            node_public_key,
            node_key,
            uptime_sec,
            detailed_debug_status,
        })
    }
}

/// Private to public API conversion.
fn make_peer_info(from: near_network::types::PeerInfo) -> near_client_primitives::types::PeerInfo {
    near_client_primitives::types::PeerInfo {
        id: from.id,
        addr: from.addr,
        account_id: from.account_id,
    }
}

/// Private to public API conversion.
fn make_known_producer(
    from: near_network::types::KnownProducer,
) -> near_client_primitives::types::KnownProducer {
    near_client_primitives::types::KnownProducer {
        peer_id: from.peer_id,
        account_id: from.account_id,
        addr: from.addr,
        next_hops: from.next_hops,
    }
}

impl Handler<WithSpanContext<GetNetworkInfo>> for ClientActor {
    type Result = Result<NetworkInfoResponse, String>;

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<GetNetworkInfo>,
        ctx: &mut Context<Self>,
    ) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "client", msg);
        let _d = delay_detector::DelayDetector::new(|| "client get network info".into());
        self.check_triggers(ctx);

        Ok(NetworkInfoResponse {
            connected_peers: (self.network_info.connected_peers.iter())
                .map(|fpi| make_peer_info(fpi.full_peer_info.peer_info.clone()))
                .collect(),
            num_connected_peers: self.network_info.num_connected_peers,
            peer_max_count: self.network_info.peer_max_count,
            sent_bytes_per_sec: self.network_info.sent_bytes_per_sec,
            received_bytes_per_sec: self.network_info.received_bytes_per_sec,
            known_producers: self
                .network_info
                .known_producers
                .iter()
                .map(|p| make_known_producer(p.clone()))
                .collect(),
        })
    }
}

/// `ApplyChunksDoneMessage` is a message that signals the finishing of applying chunks of a block.
/// Upon receiving this message, ClientActors knows that it's time to finish processing the blocks that
/// just finished applying chunks.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ApplyChunksDoneMessage;

impl Handler<WithSpanContext<ApplyChunksDoneMessage>> for ClientActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyChunksDoneMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "client", msg);
        self.try_process_unfinished_blocks();
    }
}

#[derive(Debug)]
enum SyncRequirement {
    SyncNeeded { peer_id: PeerId, highest_height: BlockHeight, head: Tip },
    AlreadyCaughtUp { peer_id: PeerId, highest_height: BlockHeight, head: Tip },
    NoPeers,
    AdvHeaderSyncDisabled,
}

impl SyncRequirement {
    fn sync_needed(&self) -> bool {
        matches!(self, Self::SyncNeeded { .. })
    }
}

impl fmt::Display for SyncRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SyncNeeded { peer_id, highest_height, head: my_head } => write!(
                f,
                "sync needed at #{} [{}]. highest height peer: {} at #{}",
                my_head.height,
                format_hash(my_head.last_block_hash),
                peer_id,
                highest_height
            ),
            Self::AlreadyCaughtUp { peer_id, highest_height, head: my_head } => write!(
                f,
                "synced at #{} [{}]. highest height peer: {} at #{}",
                my_head.height,
                format_hash(my_head.last_block_hash),
                peer_id,
                highest_height
            ),
            Self::NoPeers => write!(f, "no available peers"),
            Self::AdvHeaderSyncDisabled => {
                write!(f, "syncing disabled via adv_disable_header_sync")
            }
        }
    }
}

impl ClientActor {
    /// Check if client Account Id should be sent and send it.
    /// Account Id is sent when is not current a validator but are becoming a validator soon.
    fn check_send_announce_account(&mut self, prev_block_hash: CryptoHash) {
        // If no peers, there is no one to announce to.
        if self.network_info.num_connected_peers == 0 {
            debug!(target: "client", "No peers: skip account announce");
            return;
        }

        // First check that we currently have an AccountId
        let validator_signer = match self.client.validator_signer.as_ref() {
            None => return,
            Some(signer) => signer,
        };

        let now = StaticClock::instant();
        // Check that we haven't announced it too recently
        if let Some(last_validator_announce_time) = self.last_validator_announce_time {
            // Don't make announcement if have passed less than half of the time in which other peers
            // should remove our Account Id from their Routing Tables.
            if 2 * (now - last_validator_announce_time) < self.client.config.ttl_account_id_router {
                return;
            }
        }

        debug!(target: "client", "Check announce account for {}, last announce time {:?}", validator_signer.validator_id(), self.last_validator_announce_time);

        // Announce AccountId if client is becoming a validator soon.
        let next_epoch_id = unwrap_or_return!(self
            .client
            .epoch_manager
            .get_next_epoch_id_from_prev_block(&prev_block_hash));

        // Check client is part of the futures validators
        if self.client.is_validator(&next_epoch_id, &prev_block_hash) {
            debug!(target: "client", "Sending announce account for {}", validator_signer.validator_id());
            self.last_validator_announce_time = Some(now);

            let signature = validator_signer.sign_account_announce(
                validator_signer.validator_id(),
                &self.node_id,
                &next_epoch_id,
            );
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::AnnounceAccount(AnnounceAccount {
                    account_id: validator_signer.validator_id().clone(),
                    peer_id: self.node_id.clone(),
                    epoch_id: next_epoch_id,
                    signature,
                }),
            ));
        }
    }

    /// Process the sandbox fast forward request. If the change in block height is past an epoch,
    /// we fast forward to just right before the epoch, produce some blocks to get past and into
    /// a new epoch, then we continue on with the residual amount to fast forward.
    #[cfg(feature = "sandbox")]
    fn sandbox_process_fast_forward(
        &mut self,
        block_height: BlockHeight,
    ) -> Result<Option<near_chain::types::LatestKnown>, Error> {
        let mut delta_height = std::mem::replace(&mut self.fastforward_delta, 0);
        if delta_height == 0 {
            return Ok(None);
        }

        let epoch_length = self.client.config.epoch_length;
        if epoch_length <= 3 {
            return Err(Error::Other(
                "Unsupported: fast_forward with an epoch length of 3 or less".to_string(),
            ));
        }

        // Check if we are at epoch boundary. If we are, do not fast forward until new
        // epoch is here. Decrement the fast_forward count by 1 when a block is produced
        // during this period of waiting
        let block_height_wrt_epoch = block_height % epoch_length;
        if epoch_length - block_height_wrt_epoch <= 3 || block_height_wrt_epoch == 0 {
            // wait for doomslug to call into produce block
            self.fastforward_delta = delta_height;
            return Ok(None);
        }

        let delta_height = if block_height_wrt_epoch + delta_height >= epoch_length {
            // fast forward to just right before epoch boundary to have epoch_manager
            // handle the epoch_height updates as normal. `- 3` since this is being
            // done 3 blocks before the epoch ends.
            let right_before_epoch_update = epoch_length - block_height_wrt_epoch - 3;

            delta_height -= right_before_epoch_update;
            self.fastforward_delta = delta_height;
            right_before_epoch_update
        } else {
            delta_height
        };

        self.client.accrued_fastforward_delta += delta_height;
        let delta_time = self.client.sandbox_delta_time();
        let new_latest_known = near_chain::types::LatestKnown {
            height: block_height + delta_height,
            seen: near_primitives::utils::to_timestamp(StaticClock::utc() + delta_time),
        };

        Ok(Some(new_latest_known))
    }

    fn pre_block_production(&mut self) -> Result<(), Error> {
        #[cfg(feature = "sandbox")]
        {
            let latest_known = self.client.chain.mut_store().get_latest_known()?;
            if let Some(new_latest_known) =
                self.sandbox_process_fast_forward(latest_known.height)?
            {
                self.client.chain.mut_store().save_latest_known(new_latest_known.clone())?;
                self.client.sandbox_update_tip(new_latest_known.height)?;
            }
        }
        Ok(())
    }

    fn post_block_production(&mut self) {
        #[cfg(feature = "sandbox")]
        if self.fastforward_delta > 0 {
            // Decrease the delta_height by 1 since we've produced a single block. This
            // ensures that we advanced the right amount of blocks when fast forwarding
            // and fast forwarding triggers regular block production in the case of
            // stepping between epoch boundaries.
            self.fastforward_delta -= 1;
        }
    }

    /// Retrieves latest height, and checks if must produce next block.
    /// Otherwise wait for block arrival or suggest to skip after timeout.
    fn handle_block_production(&mut self) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "client", "handle_block_production").entered();
        // If syncing, don't try to produce blocks.
        if self.client.sync_status.is_syncing() {
            debug!(target:"client", sync_status=format!("{:#?}", self.client.sync_status), "Syncing - block production disabled");
            return Ok(());
        }

        let _ = self.client.check_and_update_doomslug_tip();

        self.pre_block_production()?;
        let head = self.client.chain.head()?;
        let latest_known = self.client.chain.store().get_latest_known()?;

        assert!(
            head.height <= latest_known.height,
            "Latest known height is invalid {} vs {}",
            head.height,
            latest_known.height
        );

        let epoch_id =
            self.client.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        let log_block_production_info =
            if self.client.epoch_manager.is_next_block_epoch_start(&head.last_block_hash)? {
                true
            } else {
                // the next block is still the same epoch
                let epoch_start_height =
                    self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash)?;
                latest_known.height - epoch_start_height < EPOCH_START_INFO_BLOCKS
            };

        // We try to produce block for multiple heights (up to the highest height for which we've seen 2/3 of approvals).
        if latest_known.height + 1 <= self.client.doomslug.get_largest_height_crossing_threshold() {
            debug!(target: "client", "Considering blocks for production between {} and {} ", latest_known.height + 1, self.client.doomslug.get_largest_height_crossing_threshold());
        } else {
            debug!(target: "client", "Cannot produce any block: not enough approvals beyond {}", latest_known.height);
        }

        let me = if let Some(me) = &self.client.validator_signer {
            me.validator_id().clone()
        } else {
            return Ok(());
        };

        // For debug purpose, we record the approvals we have seen so far to the future blocks
        for height in latest_known.height + 1..=self.client.doomslug.get_largest_approval_height() {
            let next_block_producer_account =
                self.client.epoch_manager.get_block_producer(&epoch_id, height)?;

            if me == next_block_producer_account {
                self.client.block_production_info.record_approvals(
                    height,
                    self.client.doomslug.approval_status_at_height(&height),
                );
            }
        }

        for height in
            latest_known.height + 1..=self.client.doomslug.get_largest_height_crossing_threshold()
        {
            let next_block_producer_account =
                self.client.epoch_manager.get_block_producer(&epoch_id, height)?;

            if me == next_block_producer_account {
                let num_chunks = self
                    .client
                    .num_chunk_headers_ready_for_inclusion(&epoch_id, &head.last_block_hash);
                let have_all_chunks = head.height == 0
                    || num_chunks as u64
                        == self.client.epoch_manager.num_shards(&epoch_id).unwrap();

                if self.client.doomslug.ready_to_produce_block(
                    StaticClock::instant(),
                    height,
                    have_all_chunks,
                    log_block_production_info,
                ) {
                    if let Err(err) = self.produce_block(height) {
                        // If there is an error, report it and let it retry on the next loop step.
                        error!(target: "client", height, "Block production failed: {}", err);
                    } else {
                        self.post_block_production();
                    }
                }
            }
        }

        Ok(())
    }

    fn schedule_triggers(&mut self, ctx: &mut Context<Self>) {
        let wait = self.check_triggers(ctx);

        near_performance_metrics::actix::run_later(ctx, wait, move |act, ctx| {
            act.schedule_triggers(ctx);
        });
    }

    /// Check if the scheduled time of any "triggers" has passed, and if so, call the trigger.
    /// Triggers are important functions of client, like running single step of state sync or
    /// checking if we can produce a block.
    ///
    /// It is called before processing Actix message and also in schedule_triggers.
    /// This is to ensure all triggers enjoy higher priority than any actix message.
    /// Otherwise due to a bug in Actix library Actix prioritizes processing messages
    /// while there are messages in mailbox. Because of that we handle scheduling
    /// triggers with custom `run_timer` function instead of `run_later` in Actix.
    ///
    /// Returns the delay before the next time `check_triggers` should be called, which is
    /// min(time until the closest trigger, 1 second).
    fn check_triggers(&mut self, ctx: &mut Context<ClientActor>) -> Duration {
        if let Some(config_updater) = &mut self.config_updater {
            config_updater.try_update(&|updateable_client_config| {
                self.client.update_client_config(updateable_client_config)
            });
        }

        // Check block height to trigger expected shutdown
        if let Ok(head) = self.client.chain.head() {
            if let Some(block_height_to_shutdown) = self.client.config.expected_shutdown.get() {
                if head.height >= block_height_to_shutdown {
                    info!(target: "client", "Expected shutdown triggered: head block({}) >= ({:?})", head.height, block_height_to_shutdown);
                    if let Some(tx) = self.shutdown_signal.take() {
                        let _ = tx.send(()); // Ignore send signal fail, it will send again in next trigger
                    }
                }
            }
        }

        let _d = delay_detector::DelayDetector::new(|| "client triggers".into());

        self.try_process_unfinished_blocks();

        let mut delay = Duration::from_secs(1);
        let now = Utc::now();

        let timer = metrics::CHECK_TRIGGERS_TIME.start_timer();
        if self.sync_started {
            self.sync_timer_next_attempt = self.run_timer(
                self.sync_wait_period(),
                self.sync_timer_next_attempt,
                ctx,
                |act, _| act.run_sync_step(),
                "sync",
            );

            delay = std::cmp::min(
                delay,
                self.sync_timer_next_attempt.signed_duration_since(now).to_std().unwrap_or(delay),
            );

            self.doomslug_timer_next_attempt = self.run_timer(
                self.client.config.doosmslug_step_period,
                self.doomslug_timer_next_attempt,
                ctx,
                |act, ctx| act.try_doomslug_timer(ctx),
                "doomslug",
            );
            delay = core::cmp::min(
                delay,
                self.doomslug_timer_next_attempt
                    .signed_duration_since(now)
                    .to_std()
                    .unwrap_or(delay),
            )
        }
        if self.block_production_started {
            self.block_production_next_attempt = self.run_timer(
                self.client.config.block_production_tracking_delay,
                self.block_production_next_attempt,
                ctx,
                |act, _ctx| act.try_handle_block_production(),
                "block_production",
            );

            let _ = self.client.check_head_progress_stalled(
                self.client.config.max_block_production_delay * HEAD_STALL_MULTIPLIER,
            );

            delay = core::cmp::min(
                delay,
                self.block_production_next_attempt
                    .signed_duration_since(now)
                    .to_std()
                    .unwrap_or(delay),
            )
        }

        self.log_summary_timer_next_attempt = self.run_timer(
            self.client.config.log_summary_period,
            self.log_summary_timer_next_attempt,
            ctx,
            |act, _ctx| act.log_summary(),
            "log_summary",
        );
        delay = core::cmp::min(
            delay,
            self.log_summary_timer_next_attempt
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(delay),
        );
        timer.observe_duration();
        delay
    }

    /// "Unfinished" blocks means that blocks that client has started the processing and haven't
    /// finished because it was waiting for applying chunks to be done. This function checks
    /// if there are any "unfinished" blocks that are ready to be processed again and finish processing
    /// these blocks.
    /// This function is called at two places, upon receiving ApplyChunkDoneMessage and `check_triggers`.
    /// The job that executes applying chunks will send an ApplyChunkDoneMessage to ClientActor after
    /// applying chunks is done, so when receiving ApplyChunkDoneMessage messages, ClientActor
    /// calls this function to finish processing the unfinished blocks. ClientActor also calls
    /// this function in `check_triggers`, because the actix queue may be blocked by other messages
    /// and we want to prioritize block processing.
    fn try_process_unfinished_blocks(&mut self) {
        let (accepted_blocks, _errors) =
            self.client.postprocess_ready_blocks(self.get_apply_chunks_done_callback(), true);
        // TODO: log the errors
        self.process_accepted_blocks(accepted_blocks);
    }

    fn try_handle_block_production(&mut self) {
        if let Err(err) = self.handle_block_production() {
            tracing::error!(target: "client", ?err, "Handle block production failed")
        }
    }

    fn try_doomslug_timer(&mut self, _: &mut Context<ClientActor>) {
        let _span = tracing::debug_span!(target: "client", "try_doomslug_timer").entered();
        let _ = self.client.check_and_update_doomslug_tip();
        let approvals = self.client.doomslug.process_timer(StaticClock::instant());

        // Important to save the largest approval target height before sending approvals, so
        // that if the node crashes in the meantime, we cannot get slashed on recovery
        let mut chain_store_update = self.client.chain.mut_store().store_update();
        chain_store_update
            .save_largest_target_height(self.client.doomslug.get_largest_target_height());

        match chain_store_update.commit() {
            Ok(_) => {
                let head = unwrap_or_return!(self.client.chain.head());
                if self.client.is_validator(&head.epoch_id, &head.last_block_hash)
                    || self.client.is_validator(&head.next_epoch_id, &head.last_block_hash)
                {
                    for approval in approvals {
                        if let Err(e) =
                            self.client.send_approval(&self.client.doomslug.get_tip().0, approval)
                        {
                            error!("Error while sending an approval {:?}", e);
                        }
                    }
                }
            }
            Err(e) => error!("Error while committing largest skipped height {:?}", e),
        };
    }

    /// Produce block if we are block producer for given `next_height` height.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block(&mut self, next_height: BlockHeight) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "client", "produce_block", next_height).entered();
        if let Some(block) = self.client.produce_block(next_height)? {
            // If we produced the block, send it out before we apply the block.
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block.clone() },
            ));
            // We’ve produced the block so that counts as validated block.
            let block = MaybeValidated::from_validated(block);
            let res = self.client.start_process_block(
                block,
                Provenance::PRODUCED,
                self.get_apply_chunks_done_callback(),
            );
            if let Err(e) = &res {
                match e {
                    near_chain::Error::ChunksMissing(_) => {
                        // missing chunks were already handled in Client::process_block, we don't need to
                        // do anything here
                        return Ok(());
                    }
                    _ => {
                        error!(target: "client", "Failed to process freshly produced block: {:?}", res);
                        byzantine_assert!(false);
                        return res.map_err(|err| err.into());
                    }
                }
            }
        }
        Ok(())
    }

    fn send_chunks_metrics(&mut self, block: &Block) {
        let chunks = block.chunks();
        for (chunk, &included) in chunks.iter().zip(block.header().chunk_mask().iter()) {
            if included {
                self.info_helper.chunk_processed(
                    chunk.shard_id(),
                    chunk.gas_used(),
                    chunk.balance_burnt(),
                );
            } else {
                self.info_helper.chunk_skipped(chunk.shard_id());
            }
        }
    }

    fn send_block_metrics(&mut self, block: &Block) {
        let chunks_in_block = block.header().chunk_mask().iter().filter(|&&m| m).count();
        let gas_used = Block::compute_gas_used(block.chunks().iter(), block.header().height());

        let last_final_hash = block.header().last_final_block();
        let last_final_ds_hash = block.header().last_ds_final_block();
        let last_final_block_height = self
            .client
            .chain
            .get_block(&last_final_hash)
            .map_or(0, |block| block.header().height());
        let last_final_ds_block_height = self
            .client
            .chain
            .get_block(&last_final_ds_hash)
            .map_or(0, |block| block.header().height());

        let epoch_height =
            self.client.epoch_manager.get_epoch_height_from_prev_block(block.hash()).unwrap_or(0);
        let epoch_start_height = self
            .client
            .epoch_manager
            .get_epoch_start_height(&last_final_hash)
            .unwrap_or(last_final_block_height);
        let last_final_block_height_in_epoch =
            last_final_block_height.checked_sub(epoch_start_height);

        self.info_helper.block_processed(
            gas_used,
            chunks_in_block as u64,
            block.header().gas_price(),
            block.header().total_supply(),
            last_final_block_height,
            last_final_ds_block_height,
            epoch_height,
            last_final_block_height_in_epoch,
        );
    }

    /// Process all blocks that were accepted by calling other relevant services.
    fn process_accepted_blocks(&mut self, accepted_blocks: Vec<CryptoHash>) {
        let _span = tracing::debug_span!(
            target: "client",
            "process_accepted_blocks",
            num_blocks = accepted_blocks.len())
        .entered();
        for accepted_block in accepted_blocks {
            let block = self.client.chain.get_block(&accepted_block).unwrap().clone();
            self.send_chunks_metrics(&block);
            self.send_block_metrics(&block);
            self.check_send_announce_account(*block.header().last_final_block());
        }
    }

    /// Returns the callback function that will be passed to various functions that may trigger
    /// the processing of new blocks. This callback will be called at the end of applying chunks
    /// for every block.
    fn get_apply_chunks_done_callback(&self) -> DoneApplyChunkCallback {
        let addr = self.my_address.clone();
        Arc::new(move |_| {
            addr.do_send(ApplyChunksDoneMessage {}.with_span_context());
        })
    }

    fn receive_headers(&mut self, headers: Vec<BlockHeader>, peer_id: PeerId) -> bool {
        info!(target: "client", "Received {} block headers from {}", headers.len(), peer_id);
        if headers.is_empty() {
            return true;
        }
        info!(target: "client", "Received block headers from height {} to {}", headers.first().unwrap().height(), headers.last().unwrap().height());
        match self.client.sync_block_headers(headers) {
            Ok(_) => true,
            Err(err) => {
                if err.is_bad_data() {
                    error!(target: "client", "Error processing sync blocks: {}", err);
                    false
                } else {
                    debug!(target: "client", "Block headers refused by chain: {}", err);
                    true
                }
            }
        }
    }

    /// Check whether need to (continue) sync.
    /// Also return higher height with known peers at that height.
    fn syncing_info(&self) -> Result<SyncRequirement, near_chain::Error> {
        if self.adv.disable_header_sync() {
            return Ok(SyncRequirement::AdvHeaderSyncDisabled);
        }

        let head = self.client.chain.head()?;
        let is_syncing = self.client.sync_status.is_syncing();

        // Only consider peers whose latest block is not invalid blocks
        let eligible_peers: Vec<_> = self
            .network_info
            .highest_height_peers
            .iter()
            .filter(|p| !self.client.chain.is_block_invalid(&p.highest_block_hash))
            .collect();
        metrics::PEERS_WITH_INVALID_HASH
            .set(self.network_info.highest_height_peers.len() as i64 - eligible_peers.len() as i64);
        let peer_info = if let Some(peer_info) = eligible_peers.choose(&mut thread_rng()) {
            peer_info
        } else {
            return Ok(SyncRequirement::NoPeers);
        };

        let peer_id = peer_info.peer_info.id.clone();
        let highest_height = peer_info.highest_block_height;

        if is_syncing {
            if highest_height <= head.height {
                Ok(SyncRequirement::AlreadyCaughtUp { peer_id, highest_height, head })
            } else {
                Ok(SyncRequirement::SyncNeeded { peer_id, highest_height, head })
            }
        } else {
            if highest_height > head.height + self.client.config.sync_height_threshold {
                Ok(SyncRequirement::SyncNeeded { peer_id, highest_height, head })
            } else {
                Ok(SyncRequirement::AlreadyCaughtUp { peer_id, highest_height, head })
            }
        }
    }

    fn start_flat_storage_creation(&mut self, ctx: &mut Context<ClientActor>) {
        if !self.client.config.flat_storage_creation_enabled {
            return;
        }
        match self.client.run_flat_storage_creation_step() {
            Ok(false) => {}
            Ok(true) => {
                return;
            }
            Err(err) => {
                error!(target: "client", "Error occurred during flat storage creation step: {:?}", err);
            }
        }

        near_performance_metrics::actix::run_later(
            ctx,
            self.client.config.flat_storage_creation_period,
            move |act, ctx| {
                act.start_flat_storage_creation(ctx);
            },
        );
    }

    /// Starts syncing and then switches to either syncing or regular mode.
    fn start_sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Wait for connections reach at least minimum peers unless skipping sync.
        if self.network_info.num_connected_peers < self.client.config.min_num_peers
            && !self.client.config.skip_sync_wait
        {
            near_performance_metrics::actix::run_later(
                ctx,
                self.client.config.sync_step_period,
                move |act, ctx| {
                    act.start_sync(ctx);
                },
            );
            return;
        }
        self.sync_started = true;

        // Sync loop will be started by check_triggers.
    }

    /// Select the block hash we are using to sync state. It will sync with the state before applying the
    /// content of such block.
    ///
    /// The selected block will always be the first block on a new epoch:
    /// <https://github.com/nearprotocol/nearcore/issues/2021#issuecomment-583039862>.
    ///
    /// To prevent syncing from a fork, we move `state_fetch_horizon` steps backwards and use that epoch.
    /// Usually `state_fetch_horizon` is much less than the expected number of produced blocks on an epoch,
    /// so this is only relevant on epoch boundaries.
    fn find_sync_hash(&mut self) -> Result<CryptoHash, near_chain::Error> {
        let header_head = self.client.chain.header_head()?;
        let mut sync_hash = header_head.prev_block_hash;
        for _ in 0..self.client.config.state_fetch_horizon {
            sync_hash = *self.client.chain.get_block_header(&sync_hash)?.prev_hash();
        }
        let mut epoch_start_sync_hash =
            StateSync::get_epoch_start_sync_hash(&mut self.client.chain, &sync_hash)?;

        if &epoch_start_sync_hash == self.client.chain.genesis().hash() {
            // If we are within `state_fetch_horizon` blocks of the second epoch, the sync hash will
            // be the first block of the first epoch (or, the genesis block). Due to implementation
            // details of the state sync, we can't state sync to the genesis block, so redo the
            // search without going back `state_fetch_horizon` blocks.
            epoch_start_sync_hash = StateSync::get_epoch_start_sync_hash(
                &mut self.client.chain,
                &header_head.last_block_hash,
            )?;
            assert_ne!(&epoch_start_sync_hash, self.client.chain.genesis().hash());
        }
        Ok(epoch_start_sync_hash)
    }

    /// Runs catchup on repeat, if this client is a validator.
    /// Schedules itself again if it was not ran as response to state parts job result
    fn catchup(&mut self, ctx: &mut Context<ClientActor>) {
        let _d = delay_detector::DelayDetector::new(|| "client catchup".into());
        if let Err(err) = self.client.run_catchup(
            &self.network_info.highest_height_peers,
            &self.state_parts_task_scheduler,
            &self.block_catch_up_scheduler,
            &self.state_split_scheduler,
            self.get_apply_chunks_done_callback(),
            &self.state_parts_client_arbiter.handle(),
        ) {
            error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), err);
        }

        near_performance_metrics::actix::run_later(
            ctx,
            self.client.config.catchup_step_period,
            move |act, ctx| {
                act.catchup(ctx);
            },
        );
    }

    /// Runs given callback if the time now is at least `next_attempt`.
    /// Returns time for next run which should be made based on given `delay` between runs.
    fn run_timer<F>(
        &mut self,
        delay: Duration,
        next_attempt: DateTime<Utc>,
        ctx: &mut Context<ClientActor>,
        f: F,
        timer_label: &str,
    ) -> DateTime<Utc>
    where
        F: FnOnce(&mut Self, &mut <Self as Actor>::Context) + 'static,
    {
        let now = Utc::now();
        if now < next_attempt {
            return next_attempt;
        }

        let timer =
            metrics::CLIENT_TRIGGER_TIME_BY_TYPE.with_label_values(&[timer_label]).start_timer();
        f(self, ctx);
        timer.observe_duration();

        now.checked_add_signed(chrono::Duration::from_std(delay).unwrap()).unwrap()
    }

    fn sync_wait_period(&self) -> Duration {
        if let Ok(sync) = self.syncing_info() {
            if !sync.sync_needed() {
                // If we don't need syncing - retry the sync call rarely.
                self.client.config.sync_check_period
            } else {
                // If we need syncing - retry the sync call often.
                self.client.config.sync_step_period
            }
        } else {
            self.client.config.sync_step_period
        }
    }

    /// Main syncing job responsible for syncing client with other peers.
    /// Runs itself iff it was not ran as reaction for message with results of
    /// finishing state part job
    fn run_sync_step(&mut self) {
        let _span = tracing::debug_span!(target: "client", "sync").entered();
        let _d = delay_detector::DelayDetector::new(|| "client sync".into());

        macro_rules! unwrap_and_report (($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);
                return;
            }
        }));

        let currently_syncing = self.client.sync_status.is_syncing();
        let sync = unwrap_and_report!(self.syncing_info());

        match sync {
            SyncRequirement::AlreadyCaughtUp { .. }
            | SyncRequirement::NoPeers
            | SyncRequirement::AdvHeaderSyncDisabled => {
                if currently_syncing {
                    info!(target: "client", "disabling sync: {}", &sync);
                    self.client.sync_status = SyncStatus::NoSync;

                    // Initial transition out of "syncing" state.
                    // Announce this client's account id if their epoch is coming up.
                    let head = unwrap_and_report!(self.client.chain.head());
                    self.check_send_announce_account(head.prev_block_hash);
                }
            }

            SyncRequirement::SyncNeeded { highest_height, .. } => {
                if !currently_syncing {
                    info!(
                        target: "client",
                        "enabling sync: {}", &sync,
                    );
                }
                // Run each step of syncing separately.
                unwrap_and_report!(self.client.header_sync.run(
                    &mut self.client.sync_status,
                    &mut self.client.chain,
                    highest_height,
                    &self.network_info.highest_height_peers
                ));
                // Only body / state sync if header height is close to the latest.
                let header_head = unwrap_and_report!(self.client.chain.header_head());

                // Sync state if already running sync state or if block sync is too far.
                let sync_state = match self.client.sync_status {
                    SyncStatus::StateSync(_) => true,
                    _ if header_head.height
                        >= highest_height
                            .saturating_sub(self.client.config.block_header_fetch_horizon) =>
                    {
                        unwrap_and_report!(self.client.block_sync.run(
                            &mut self.client.sync_status,
                            &self.client.chain,
                            highest_height,
                            &self.network_info.highest_height_peers
                        ))
                    }
                    _ => false,
                };
                if sync_state {
                    let (sync_hash, mut new_shard_sync, just_enter_state_sync) =
                        match &self.client.sync_status {
                            SyncStatus::StateSync(StateSyncStatus {
                                sync_hash,
                                sync_status: shard_sync,
                            }) => (*sync_hash, shard_sync.clone(), false),
                            _ => {
                                let sync_hash = unwrap_and_report!(self.find_sync_hash());
                                (sync_hash, HashMap::default(), true)
                            }
                        };

                    let me =
                        self.client.validator_signer.as_ref().map(|x| x.validator_id().clone());
                    let block_header =
                        unwrap_and_report!(self.client.chain.get_block_header(&sync_hash));
                    let prev_hash = *block_header.prev_hash();
                    let epoch_id =
                        self.client.chain.get_block_header(&sync_hash).unwrap().epoch_id().clone();
                    let shards_to_sync =
                        (0..self.client.epoch_manager.num_shards(&epoch_id).unwrap())
                            .filter(|x| {
                                cares_about_shard_this_or_next_epoch(
                                    me.as_ref(),
                                    &prev_hash,
                                    *x,
                                    true,
                                    &self.client.shard_tracker,
                                )
                            })
                            .collect();

                    if !self.client.config.archive && just_enter_state_sync {
                        unwrap_and_report!(self.client.chain.reset_data_pre_state_sync(sync_hash));
                    }

                    let use_colour =
                        matches!(self.client.config.log_summary_style, LogSummaryStyle::Colored);
                    match unwrap_and_report!(self.client.state_sync.run(
                        &me,
                        sync_hash,
                        &mut new_shard_sync,
                        &mut self.client.chain,
                        self.client.epoch_manager.as_ref(),
                        &self.network_info.highest_height_peers,
                        shards_to_sync,
                        &self.state_parts_task_scheduler,
                        &self.state_split_scheduler,
                        &self.state_parts_client_arbiter.handle(),
                        use_colour,
                    )) {
                        StateSyncResult::Unchanged => (),
                        StateSyncResult::Changed(fetch_block) => {
                            self.client.sync_status = SyncStatus::StateSync(StateSyncStatus {
                                sync_hash,
                                sync_status: new_shard_sync,
                            });
                            if fetch_block {
                                if let Some(peer_info) =
                                    self.network_info.highest_height_peers.choose(&mut thread_rng())
                                {
                                    let id = peer_info.peer_info.id.clone();

                                    if let Ok(header) =
                                        self.client.chain.get_block_header(&sync_hash)
                                    {
                                        for hash in
                                            vec![*header.prev_hash(), *header.hash()].into_iter()
                                        {
                                            self.client.request_block(hash, id.clone());
                                        }
                                    }
                                }
                            }
                        }
                        StateSyncResult::Completed => {
                            info!(target: "sync", "State sync: all shards are done");

                            let mut block_processing_artifacts = BlockProcessingArtifact::default();

                            unwrap_and_report!(self.client.chain.reset_heads_post_state_sync(
                                &me,
                                sync_hash,
                                &mut block_processing_artifacts,
                                self.get_apply_chunks_done_callback(),
                            ));

                            self.client
                                .process_block_processing_artifact(block_processing_artifacts);

                            self.client.sync_status = SyncStatus::BodySync {
                                start_height: 0,
                                current_height: 0,
                                highest_height: 0,
                            };
                        }
                    }
                }
            }
        }
    }

    /// Print current summary.
    fn log_summary(&mut self) {
        let _span = tracing::debug_span!(target: "client", "log_summary").entered();
        let _d = delay_detector::DelayDetector::new(|| "client log summary".into());
        self.info_helper.log_summary(
            &self.client,
            &self.node_id,
            &self.network_info,
            &self.config_updater,
        )
    }
}

impl Drop for ClientActor {
    fn drop(&mut self) {
        let _span = tracing::debug_span!(target: "client", "drop").entered();
        self.state_parts_client_arbiter.stop();
    }
}

impl Handler<WithSpanContext<ApplyStatePartsResponse>> for ClientActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyStatePartsResponse>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        if let Some((sync, _, _)) = self.client.catchup_state_syncs.get_mut(&msg.sync_hash) {
            // We are doing catchup
            sync.set_apply_result(msg.shard_id, msg.apply_result);
        } else {
            self.client.state_sync.set_apply_result(msg.shard_id, msg.apply_result);
        }
    }
}

impl Handler<WithSpanContext<BlockCatchUpResponse>> for ClientActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<BlockCatchUpResponse>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        if let Some((_, _, blocks_catch_up_state)) =
            self.client.catchup_state_syncs.get_mut(&msg.sync_hash)
        {
            assert!(blocks_catch_up_state.scheduled_blocks.remove(&msg.block_hash));
            blocks_catch_up_state.processed_blocks.insert(msg.block_hash, msg.results);
        } else {
            panic!("block catch up processing result from unknown sync hash");
        }
    }
}

impl Handler<WithSpanContext<StateSplitResponse>> for ClientActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<StateSplitResponse>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        if let Some((sync, _, _)) = self.client.catchup_state_syncs.get_mut(&msg.sync_hash) {
            // We are doing catchup
            sync.set_split_result(msg.shard_id, msg.new_state_roots);
        } else {
            self.client.state_sync.set_split_result(msg.shard_id, msg.new_state_roots);
        }
    }
}

impl Handler<WithSpanContext<ShardsManagerResponse>> for ClientActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<ShardsManagerResponse>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        match msg {
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                self.client.on_chunk_completed(
                    partial_chunk,
                    shard_chunk,
                    self.get_apply_chunks_done_callback(),
                );
            }
            ShardsManagerResponse::InvalidChunk(encoded_chunk) => {
                self.client.on_invalid_chunk(encoded_chunk);
            }
            ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                chunk_header,
                chunk_producer,
            } => {
                self.client.on_chunk_header_ready_for_inclusion(chunk_header, chunk_producer);
            }
        }
    }
}

impl Handler<WithSpanContext<GetClientConfig>> for ClientActor {
    type Result = Result<ClientConfig, GetClientConfigError>;

    fn handle(
        &mut self,
        msg: WithSpanContext<GetClientConfig>,
        _: &mut Context<Self>,
    ) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "client", msg);
        let _d = delay_detector::DelayDetector::new(|| "client get client config".into());

        Ok(self.client.config.clone())
    }
}

/// Returns random seed sampled from the current thread
pub fn random_seed_from_thread() -> RngSeed {
    let mut rng_seed: RngSeed = [0; 32];
    rand::thread_rng().fill(&mut rng_seed);
    rng_seed
}

/// Starts client in a separate Arbiter (thread).
pub fn start_client(
    client_config: ClientConfig,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    node_id: PeerId,
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    telemetry_actor: Addr<TelemetryActor>,
    make_state_snapshot_callback: Option<MakeSnapshotCallback>,
    sender: Option<broadcast::Sender<()>>,
    adv: crate::adversarial::Controls,
    config_updater: Option<ConfigUpdater>,
) -> (Addr<ClientActor>, ArbiterHandle) {
    let client_arbiter = Arbiter::new();
    let client_arbiter_handle = client_arbiter.handle();

    wait_until_genesis(&chain_genesis.time);
    let client = Client::new(
        client_config.clone(),
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter.clone(),
        shards_manager_adapter,
        validator_signer.clone(),
        true,
        random_seed_from_thread(),
        make_state_snapshot_callback,
    )
    .unwrap();
    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |ctx| {
        ClientActor::new(
            client,
            ctx.address(),
            client_config,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_actor,
            ctx,
            sender,
            adv,
            config_updater,
        )
        .unwrap()
    });
    (client_addr, client_arbiter_handle)
}
