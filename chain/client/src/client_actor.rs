//! Client actor orchestrates Client and facilitates network connection.

use crate::client::{Client, EPOCH_START_INFO_BLOCKS};
use crate::info::{
    display_sync_status, get_validator_epoch_stats, InfoHelper, ValidatorInfoHelper,
};
use crate::metrics::PARTIAL_ENCODED_CHUNK_RESPONSE_DELAY;
use crate::sync::{StateSync, StateSyncResult};
use crate::{metrics, StatusResponse};
use actix::dev::SendError;
use actix::{Actor, Addr, Arbiter, AsyncContext, Context, Handler, Message};
use actix_rt::ArbiterHandle;
use borsh::BorshSerialize;
use chrono::DateTime;
use near_chain::chain::{
    do_apply_chunks, ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest,
    BlockCatchUpResponse, StateSplitRequest, StateSplitResponse,
};
use near_chain::test_utils::format_hash;
use near_chain::types::ValidatorInfoIdentifier;
use near_chain::{
    byzantine_assert, near_chain_primitives, Block, BlockHeader, BlockProcessingArtifact,
    ChainGenesis, DoneApplyChunkCallback, Provenance, RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
use near_client_primitives::types::{
    Error, GetNetworkInfo, NetworkInfoResponse, ShardSyncDownload, ShardSyncStatus, Status,
    StatusError, StatusSyncInfo, SyncStatus,
};

#[cfg(feature = "test_features")]
use near_chain::ChainStoreAccess;
use near_network::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkInfo, NetworkRequests,
    PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_network_primitives::types::ReasonForBan;
use near_performance_metrics;
use near_performance_metrics_macros::{perf, perf_with_debug};
use near_primitives::block_header::ApprovalType;
use near_primitives::epoch_manager::RngSeed;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::state_part::PartId;
use near_primitives::syncing::StatePartKey;
use near_primitives::time::{Clock, Utc};
use near_primitives::types::BlockHeight;
use near_primitives::unwrap_or_return;
use near_primitives::utils::{from_timestamp, MaybeValidated};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{DetailedDebugStatus, ValidatorInfo};
use near_store::DBCol;
use near_telemetry::TelemetryActor;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

/// Multiplier on `max_block_time` to wait until deciding that chain stalled.
const STATUS_WAIT_TIME_MULTIPLIER: u64 = 10;
/// Drop blocks whose height are beyond head + horizon if it is not in the current epoch.
const BLOCK_HORIZON: u64 = 500;
/// `max_block_production_time` times this multiplier is how long we wait before rebroadcasting
/// the current `head`
const HEAD_STALL_MULTIPLIER: u32 = 4;

pub struct ClientActor {
    /// Adversarial controls
    pub adv: crate::adversarial::Controls,

    // Address of this ClientActor. Can be used to send messages to self.
    my_address: Addr<ClientActor>,
    pub(crate) client: Client,
    network_adapter: Arc<dyn PeerManagerAdapter>,
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
    chunk_request_retry_next_attempt: DateTime<Utc>,
    sync_started: bool,
    state_parts_task_scheduler: Box<dyn Fn(ApplyStatePartsRequest)>,
    block_catch_up_scheduler: Box<dyn Fn(BlockCatchUpRequest)>,
    state_split_scheduler: Box<dyn Fn(StateSplitRequest)>,
    state_parts_client_arbiter: Arbiter,

    #[cfg(feature = "sandbox")]
    fastforward_delta: near_primitives::types::BlockHeightDelta,

    /// Synchronization measure to allow graceful shutdown.
    /// Informs the system when a ClientActor gets dropped.
    _shutdown_signal: Option<oneshot::Sender<()>>,
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    loop {
        // Get chrono::Duration::num_seconds() by deducting genesis_time from now.
        let duration = genesis_time.signed_duration_since(Clock::utc());
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
        address: Addr<ClientActor>,
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        node_id: PeerId,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        telemetry_actor: Addr<TelemetryActor>,
        enable_doomslug: bool,
        rng_seed: RngSeed,
        ctx: &Context<ClientActor>,
        shutdown_signal: Option<oneshot::Sender<()>>,
        adv: crate::adversarial::Controls,
    ) -> Result<Self, Error> {
        let state_parts_arbiter = Arbiter::new();
        let self_addr = ctx.address();
        let sync_jobs_actor_addr = SyncJobsActor::start_in_arbiter(
            &state_parts_arbiter.handle(),
            move |ctx: &mut Context<SyncJobsActor>| -> SyncJobsActor {
                ctx.set_mailbox_capacity(SyncJobsActor::MAILBOX_CAPACITY);
                SyncJobsActor { client_addr: self_addr }
            },
        );
        wait_until_genesis(&chain_genesis.time);
        if let Some(vs) = &validator_signer {
            info!(target: "client", "Starting validator node: {}", vs.validator_id());
        }
        let info_helper = InfoHelper::new(Some(telemetry_actor), &config, validator_signer.clone());
        let client = Client::new(
            config,
            chain_genesis,
            runtime_adapter,
            network_adapter.clone(),
            validator_signer,
            enable_doomslug,
            rng_seed,
        )?;

        let now = Utc::now();
        Ok(ClientActor {
            adv,
            my_address: address,
            client,
            network_adapter,
            node_id,
            network_info: NetworkInfo {
                connected_peers: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                peer_counter: 0,
                tier1_accounts: vec![],
            },
            last_validator_announce_time: None,
            info_helper,
            block_production_next_attempt: now,
            log_summary_timer_next_attempt: now,
            block_production_started: false,
            doomslug_timer_next_attempt: now,
            chunk_request_retry_next_attempt: now,
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
            _shutdown_signal: shutdown_signal,
        })
    }
}

fn create_sync_job_scheduler<M>(address: Addr<SyncJobsActor>) -> Box<dyn Fn(M)>
where
    M: Message + Send + 'static,
    M::Result: Send,
    SyncJobsActor: Handler<M>,
{
    Box::new(move |msg: M| {
        if let Err(err) = address.try_send(msg) {
            match err {
                SendError::Full(request) => {
                    address.do_send(request);
                }
                SendError::Closed(_) => {
                    error!("Can't send message to SyncJobsActor, mailbox is closed");
                }
            }
        }
    })
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
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

        self.client.send_network_chain_info().unwrap();
    }
}

impl Handler<NetworkClientMessages> for ClientActor {
    type Result = NetworkClientResponses;

    #[perf_with_debug]
    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Context<Self>) -> Self::Result {
        let _span = tracing::debug_span!(
            target: "client",
            "handle",
            handler="NetworkClientMessages",
            msg=msg.as_ref())
        .entered();

        self.check_triggers(ctx);

        let _d = delay_detector::DelayDetector::new(|| {
            format!("NetworkClientMessage {}", msg.as_ref()).into()
        });
        metrics::CLIENT_MESSAGES_COUNT.with_label_values(&[msg.as_ref()]).inc();
        let timer = metrics::CLIENT_MESSAGES_PROCESSING_TIME
            .with_label_values(&[msg.as_ref()])
            .start_timer();
        let res = self.handle_client_messages(msg);
        timer.observe_duration();
        res
    }
}

impl ClientActor {
    fn handle_client_messages(&mut self, msg: NetworkClientMessages) -> NetworkClientResponses {
        match msg {
            #[cfg(feature = "test_features")]
            NetworkClientMessages::Adversarial(adversarial_msg) => {
                return match adversarial_msg {
                    near_network_primitives::types::NetworkAdversarialMessage::AdvDisableDoomslug => {
                        info!(target: "adversary", "Turning Doomslug off");
                        self.adv.set_disable_doomslug(true);
                        self.client.doomslug.adv_disable();
                        self.client.chain.adv_disable_doomslug();
                        NetworkClientResponses::NoResponse
                    }
                    near_network_primitives::types::NetworkAdversarialMessage::AdvDisableHeaderSync => {
                        info!(target: "adversary", "Blocking header sync");
                        self.adv.set_disable_header_sync(true);
                        NetworkClientResponses::NoResponse
                    }
                    near_network_primitives::types::NetworkAdversarialMessage::AdvProduceBlocks(num_blocks, only_valid) => {
                        info!(target: "adversary", "Producing {} blocks", num_blocks);
                        self.client.adv_produce_blocks = true;
                        self.client.adv_produce_blocks_only_valid = only_valid;
                        let start_height =
                            self.client.chain.mut_store().get_latest_known().unwrap().height + 1;
                        let mut blocks_produced = 0;
                        for height in start_height.. {
                            let block = self
                                .client
                                .produce_block(height)
                                .expect("block should be produced");
                            if only_valid && block == None {
                                continue;
                            }
                            let block = block.expect("block should exist after produced");
                            info!(target: "adversary", "Producing {} block out of {}, height = {}", blocks_produced, num_blocks, height);
                            self.network_adapter.do_send(
                                PeerManagerMessageRequest::NetworkRequests(
                                    NetworkRequests::Block { block: block.clone() },
                                ),
                            );
                            let _ =
                                self.client.start_process_block(block.into(), Provenance::PRODUCED, self.get_apply_chunks_done_callback());
                            blocks_produced += 1;
                            if blocks_produced == num_blocks {
                                break;
                            }
                        }
                        NetworkClientResponses::NoResponse
                    }
                    near_network_primitives::types::NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                        info!(target: "adversary", "Switching to height {:?}", height);
                        let mut chain_store_update = self.client.chain.mut_store().store_update();
                        chain_store_update.save_largest_target_height(height);
                        chain_store_update
                            .adv_save_latest_known(height)
                            .expect("adv method should not fail");
                        chain_store_update.commit().expect("adv method should not fail");
                        NetworkClientResponses::NoResponse
                    }
                    near_network_primitives::types::NetworkAdversarialMessage::AdvGetSavedBlocks => {
                        info!(target: "adversary", "Requested number of saved blocks");
                        let store = self.client.chain.store().store();
                        let mut num_blocks = 0;
                        for _ in store.iter(DBCol::Block) {
                            num_blocks += 1;
                        }
                        NetworkClientResponses::AdvResult(num_blocks)
                    }
                    near_network_primitives::types::NetworkAdversarialMessage::AdvCheckStorageConsistency => {
                        // timeout is set to 1.5 seconds to give some room as we wait in Nightly for 2 seconds
                        let timeout = 1500;
                        info!(target: "adversary", "Check Storage Consistency, timeout set to {:?} milliseconds", timeout);
                        let mut genesis = near_chain_configs::GenesisConfig::default();
                        genesis.genesis_height = self.client.chain.store().get_genesis_height();
                        let mut store_validator = near_chain::store_validator::StoreValidator::new(
                            self.client.validator_signer.as_ref().map(|x| x.validator_id().clone()),
                            genesis,
                            self.client.runtime_adapter.clone(),
                            self.client.chain.store().store().clone(),
                            self.adv.is_archival(),
                        );
                        store_validator.set_timeout(timeout);
                        store_validator.validate();
                        if store_validator.is_failed() {
                            error!(target: "client", "Storage Validation failed, {:?}", store_validator.errors);
                            NetworkClientResponses::AdvResult(0)
                        } else {
                            NetworkClientResponses::AdvResult(store_validator.tests_done())
                        }
                    }
                    _ => panic!("invalid adversary message"),
                };
            }
            NetworkClientMessages::Transaction { transaction, is_forwarded, check_only } => {
                self.client.process_tx(transaction, is_forwarded, check_only)
            }
            NetworkClientMessages::Block(block, peer_id, was_requested) => {
                let blocks_at_height = self
                    .client
                    .chain
                    .store()
                    .get_all_block_hashes_by_height(block.header().height());
                if was_requested || !blocks_at_height.is_ok() {
                    if let SyncStatus::StateSync(sync_hash, _) = &mut self.client.sync_status {
                        if let Ok(header) = self.client.chain.get_block_header(sync_hash) {
                            if block.hash() == header.prev_hash() {
                                if let Err(e) = self.client.chain.save_block(block.into()) {
                                    error!(target: "client", "Failed to save a block during state sync: {}", e);
                                }
                            } else if block.hash() == sync_hash {
                                // This is the immediate block after a state sync
                                // We can afford to delay requesting missing chunks for this one block
                                if let Err(e) = self.client.chain.save_orphan(block.into(), false) {
                                    error!(target: "client", "Received an invalid block during state sync: {}", e);
                                }
                            }
                            return NetworkClientResponses::NoResponse;
                        }
                    }
                    self.receive_block(block, peer_id, was_requested);
                    NetworkClientResponses::NoResponse
                } else {
                    match self
                        .client
                        .runtime_adapter
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
                    NetworkClientResponses::NoResponse
                }
            }
            NetworkClientMessages::BlockHeaders(headers, peer_id) => {
                if self.receive_headers(headers, peer_id) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block headers");
                    NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockHeader }
                }
            }
            NetworkClientMessages::BlockApproval(approval, peer_id) => {
                debug!(target: "client", "Receive approval {:?} from peer {:?}", approval, peer_id);
                self.client.collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id));
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::StateResponse(state_response_info) => {
                let shard_id = state_response_info.shard_id();
                let hash = state_response_info.sync_hash();
                let state_response = state_response_info.take_state_response();

                trace!(target: "sync", "Received state response shard_id: {} sync_hash: {:?} part(id/size): {:?}",
                       shard_id,
                       hash,
                       state_response.part().as_ref().map(|(part_id, data)| (part_id, data.len()))
                );
                // Get the download that matches the shard_id and hash
                let download = {
                    let mut download: Option<&mut ShardSyncDownload> = None;

                    // ... It could be that the state was requested by the state sync
                    if let SyncStatus::StateSync(sync_hash, shards_to_download) =
                        &mut self.client.sync_status
                    {
                        if hash == *sync_hash {
                            if let Some(part_id) = state_response.part_id() {
                                self.client
                                    .state_sync
                                    .received_requested_part(part_id, shard_id, hash);
                            }

                            if let Some(shard_download) = shards_to_download.get_mut(&shard_id) {
                                assert!(
                                    download.is_none(),
                                    "Internal downloads set has duplicates"
                                );
                                download = Some(shard_download);
                            } else {
                                // This may happen because of sending too many StateRequests to different peers.
                                // For example, we received StateResponse after StateSync completion.
                            }
                        }
                    }

                    // ... Or one of the catchups
                    if let Some((_, shards_to_download, _)) =
                        self.client.catchup_state_syncs.get_mut(&hash)
                    {
                        if let Some(part_id) = state_response.part_id() {
                            self.client.state_sync.received_requested_part(part_id, shard_id, hash);
                        }

                        if let Some(shard_download) = shards_to_download.get_mut(&shard_id) {
                            assert!(download.is_none(), "Internal downloads set has duplicates");
                            download = Some(shard_download);
                        } else {
                            // This may happen because of sending too many StateRequests to different peers.
                            // For example, we received StateResponse after StateSync completion.
                        }
                    }
                    // We should not be requesting the same state twice.
                    download
                };

                if let Some(shard_sync_download) = download {
                    match shard_sync_download.status {
                        ShardSyncStatus::StateDownloadHeader => {
                            if let Some(header) = state_response.take_header() {
                                if !shard_sync_download.downloads[0].done {
                                    match self.client.chain.set_state_header(shard_id, hash, header)
                                    {
                                        Ok(()) => {
                                            shard_sync_download.downloads[0].done = true;
                                        }
                                        Err(err) => {
                                            error!(target: "sync", "State sync set_state_header error, shard = {}, hash = {}: {:?}", shard_id, hash, err);
                                            shard_sync_download.downloads[0].error = true;
                                        }
                                    }
                                }
                            } else {
                                // No header found.
                                // It may happen because requested node couldn't build state response.
                                if !shard_sync_download.downloads[0].done {
                                    info!(target: "sync", "state_response doesn't have header, should be re-requested, shard = {}, hash = {}", shard_id, hash);
                                    shard_sync_download.downloads[0].error = true;
                                }
                            }
                        }
                        ShardSyncStatus::StateDownloadParts => {
                            if let Some(part) = state_response.take_part() {
                                let num_parts = shard_sync_download.downloads.len() as u64;
                                let (part_id, data) = part;
                                if part_id >= num_parts {
                                    error!(target: "sync", "State sync received incorrect part_id # {:?} for hash {:?}, potential malicious peer", part_id, hash);
                                    return NetworkClientResponses::NoResponse;
                                }
                                if !shard_sync_download.downloads[part_id as usize].done {
                                    match self.client.chain.set_state_part(
                                        shard_id,
                                        hash,
                                        PartId::new(part_id, num_parts),
                                        &data,
                                    ) {
                                        Ok(()) => {
                                            shard_sync_download.downloads[part_id as usize].done =
                                                true;
                                        }
                                        Err(err) => {
                                            error!(target: "sync", "State sync set_state_part error, shard = {}, part = {}, hash = {}: {:?}", shard_id, part_id, hash, err);
                                            shard_sync_download.downloads[part_id as usize].error =
                                                true;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    error!(target: "sync", "State sync received hash {} that we're not expecting, potential malicious peer", hash);
                }

                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::EpochSyncResponse(_peer_id, _response) => {
                // TODO #3488
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::EpochSyncFinalizationResponse(_peer_id, _response) => {
                // TODO #3488
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunkRequest(part_request_msg, route_back) => {
                let _ = self.client.shards_mgr.process_partial_encoded_chunk_request(
                    part_request_msg,
                    route_back,
                    self.client.chain.mut_store(),
                    &mut self.client.rs,
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunkResponse(response, time) => {
                PARTIAL_ENCODED_CHUNK_RESPONSE_DELAY.observe(time.elapsed().as_secs_f64());
                let _ = self.client.process_partial_encoded_chunk_response(
                    response,
                    self.get_apply_chunks_done_callback(),
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunk(partial_encoded_chunk) => {
                self.client.block_production_info.record_chunk_collected(
                    partial_encoded_chunk.height_created(),
                    partial_encoded_chunk.shard_id(),
                );
                let _ = self.client.process_partial_encoded_chunk(
                    MaybeValidated::from(partial_encoded_chunk),
                    self.get_apply_chunks_done_callback(),
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunkForward(forward) => {
                match self.client.process_partial_encoded_chunk_forward(
                    forward,
                    self.get_apply_chunks_done_callback(),
                ) {
                    Ok(()) => {}
                    // Unknown chunk is normal if we get parts before the header
                    Err(Error::Chunk(near_chunks::Error::UnknownChunk)) => (),
                    Err(err) => {
                        error!(target: "client", "Error processing forwarded chunk: {}", err)
                    }
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::Challenge(challenge) => {
                match self.client.process_challenge(challenge) {
                    Ok(_) => {}
                    Err(err) => {
                        error!(target: "client", "Error processing challenge: {}", err);
                    }
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::NetworkInfo(network_info) => {
                self.network_info = network_info;
                NetworkClientResponses::NoResponse
            }
        }
    }
}
#[cfg(feature = "sandbox")]
impl Handler<near_client_primitives::types::SandboxMessage> for ClientActor {
    type Result = near_client_primitives::types::SandboxResponse;

    fn handle(
        &mut self,
        msg: near_client_primitives::types::SandboxMessage,
        _ctx: &mut Context<Self>,
    ) -> near_client_primitives::types::SandboxResponse {
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

impl Handler<Status> for ClientActor {
    type Result = Result<StatusResponse, StatusError>;

    #[perf]
    fn handle(&mut self, msg: Status, ctx: &mut Context<Self>) -> Self::Result {
        let _span = tracing::debug_span!(target: "client", "handle", handler = "Status").entered();
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
            .runtime_adapter
            .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash)?
            .into_iter()
            .map(|(validator_stake, is_slashed)| ValidatorInfo {
                account_id: validator_stake.take_account_id(),
                is_slashed,
            })
            .collect();

        let epoch_start_height =
            self.client.runtime_adapter.get_epoch_start_height(&head.last_block_hash).ok();

        let protocol_version =
            self.client.runtime_adapter.get_epoch_protocol_version(&head.epoch_id)?;

        let validator_and_key =
            self.client.validator_signer.as_ref().map(|vs| (vs.validator_id(), vs.public_key()));

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
                network_info: self.network_info.clone().into(),
                sync_status: format!(
                    "{} ({})",
                    self.client.sync_status.as_variant_name().to_string(),
                    display_sync_status(&self.client.sync_status, &self.client.chain.head()?,),
                ),
                current_head_status: head.clone().into(),
                current_header_head_status: self.client.chain.header_head()?.into(),
                block_production_delay_millis: self
                    .client
                    .config
                    .min_block_production_delay
                    .as_millis() as u64,
                chain_processing_info: self.client.chain.get_chain_processing_info(),
            })
        } else {
            None
        };
        let uptime_sec = Clock::utc().timestamp() - self.info_helper.boot_time_seconds;
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
            validator_account_id: validator_and_key.as_ref().map(|v| v.0.clone()),
            node_key: validator_and_key.as_ref().map(|v| v.1.clone()),
            uptime_sec,
            detailed_debug_status,
        })
    }
}

impl Handler<GetNetworkInfo> for ClientActor {
    type Result = Result<NetworkInfoResponse, String>;

    #[perf]
    fn handle(&mut self, _msg: GetNetworkInfo, ctx: &mut Context<Self>) -> Self::Result {
        let _span = tracing::debug_span!(
            target: "client",
            "handle",
            handler="GetNetworkInfo")
        .entered();
        let _d = delay_detector::DelayDetector::new(|| "client get network info".into());
        self.check_triggers(ctx);

        Ok(NetworkInfoResponse {
            connected_peers: (self.network_info.connected_peers.iter())
                .map(|fpi| fpi.full_peer_info.peer_info.clone())
                .collect(),
            num_connected_peers: self.network_info.num_connected_peers,
            peer_max_count: self.network_info.peer_max_count,
            sent_bytes_per_sec: self.network_info.sent_bytes_per_sec,
            received_bytes_per_sec: self.network_info.received_bytes_per_sec,
            known_producers: self.network_info.known_producers.clone(),
        })
    }
}

/// `ApplyChunksDoneMessage` is a message that signals the finishing of applying chunks of a block.
/// Upon receiving this message, ClientActors knows that it's time to finish processing the blocks that
/// just finished applying chunks.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ApplyChunksDoneMessage;

impl Handler<ApplyChunksDoneMessage> for ClientActor {
    type Result = ();

    fn handle(&mut self, _msg: ApplyChunksDoneMessage, _ctx: &mut Self::Context) -> Self::Result {
        self.try_process_unfinished_blocks();
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

        let now = Clock::instant();
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
            .runtime_adapter
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
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
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
            seen: near_primitives::utils::to_timestamp(Clock::utc() + delta_time),
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
            debug!(target:"client", "Syncing - block production disabled");
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
            self.client.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        let log_block_production_info =
            if self.client.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
                true
            } else {
                // the next block is still the same epoch
                let epoch_start_height =
                    self.client.runtime_adapter.get_epoch_start_height(&head.last_block_hash)?;
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
                self.client.runtime_adapter.get_block_producer(&epoch_id, height)?;

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
                self.client.runtime_adapter.get_block_producer(&epoch_id, height)?;

            if me == next_block_producer_account {
                let num_chunks = self.client.shards_mgr.num_chunks_for_block(&head.last_block_hash);
                let have_all_chunks = head.height == 0
                    || num_chunks == self.client.runtime_adapter.num_shards(&epoch_id).unwrap();

                if self.client.doomslug.ready_to_produce_block(
                    Clock::instant(),
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

    fn check_triggers(&mut self, ctx: &mut Context<ClientActor>) -> Duration {
        // There is a bug in Actix library. While there are messages in mailbox, Actix
        // will prioritize processing messages until mailbox is empty. Execution of any other task
        // scheduled with run_later will be delayed.

        let _d = delay_detector::DelayDetector::new(|| "client triggers".into());

        self.try_process_unfinished_blocks();

        let mut delay = Duration::from_secs(1);
        let now = Utc::now();

        let timer = metrics::CHECK_TRIGGERS_TIME.start_timer();
        if self.sync_started {
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

        self.chunk_request_retry_next_attempt = self.run_timer(
            self.client.config.chunk_request_retry_period,
            self.chunk_request_retry_next_attempt,
            ctx,
            |act, _ctx| {
                if let Ok(header_head) = act.client.chain.header_head() {
                    act.client.shards_mgr.resend_chunk_requests(&header_head)
                }
            },
            "resend_chunk_requests",
        );
        timer.observe_duration();
        core::cmp::min(
            delay,
            self.chunk_request_retry_next_attempt
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(delay),
        )
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
        let approvals = self.client.doomslug.process_timer(Clock::instant());

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
            let peer_id = self.node_id.clone();
            // We’ve produced the block so that counts as validated block.
            let block = MaybeValidated::from_validated(block);
            let res = self.process_block(block, Provenance::PRODUCED, &peer_id);
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

    /// Process all blocks that were accepted by calling other relevant services.
    fn process_accepted_blocks(&mut self, accepted_blocks: Vec<CryptoHash>) {
        let _span = tracing::debug_span!(
            target: "client",
            "process_accepted_blocks",
            num_blocks = accepted_blocks.len())
        .entered();
        for accepted_block in accepted_blocks {
            let block = self.client.chain.get_block(&accepted_block).unwrap().clone();
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

            let epoch_height = self
                .client
                .runtime_adapter
                .get_epoch_height_from_prev_block(block.hash())
                .unwrap_or(0);

            self.info_helper.block_processed(
                gas_used,
                chunks_in_block as u64,
                block.header().gas_price(),
                block.header().total_supply(),
                last_final_block_height,
                last_final_ds_block_height,
                epoch_height,
            );
            self.check_send_announce_account(*last_final_hash);
        }
    }

    /// Process block and execute callbacks.
    fn process_block(
        &mut self,
        block: MaybeValidated<Block>,
        provenance: Provenance,
        peer_id: &PeerId,
    ) -> Result<(), near_chain::Error> {
        let _span = tracing::debug_span!(
            target: "client",
            "process_block",
            height = block.header().height())
        .entered();
        debug!(target: "client", ?provenance, ?peer_id);
        // If we produced the block, send it out before we apply the block.
        // If we didn't produce the block and didn't request it, do basic validation
        // before sending it out.
        if provenance == Provenance::PRODUCED {
            self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block.as_ref().into_inner().clone() },
            ));
            // If we produced it, we don’t need to validate it.  Mark the block
            // as valid.
            block.mark_as_valid();
        } else {
            let chain = &mut self.client.chain;
            // TODO: refactor this after we make apply_chunks async. After that, process_block
            // will return before the full block is finished processing, and we can simply move the
            // rebroadcast_block logic to after self.client.process_block
            let res = chain.process_block_header(block.header(), &mut vec![]);
            let res = res.and_then(|_| chain.validate_block(&block));
            match res {
                Ok(_) => {
                    let head = self.client.chain.head()?;
                    // do not broadcast blocks that are too far back.
                    if (head.height < block.header().height()
                        || &head.epoch_id == block.header().epoch_id())
                        && provenance == Provenance::NONE
                        && !self.client.sync_status.is_syncing()
                    {
                        self.client.rebroadcast_block(block.as_ref().into_inner());
                    }
                }
                Err(e) if e.is_bad_data() => {
                    self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::BanPeer {
                            peer_id: peer_id.clone(),
                            ban_reason: ReasonForBan::BadBlockHeader,
                        },
                    ));
                    return Err(e);
                }
                Err(_) => {
                    // We are ignoring all other errors and proceeding with the
                    // block.  If it is an orphan (i.e. we haven’t processed its
                    // previous block) than we will get MissingBlock errors.  In
                    // those cases we shouldn’t reject the block instead passing
                    // it along.  Eventually, it’ll get saved as an orphan.
                }
            }
        }
        self.client.start_process_block(block, provenance, self.get_apply_chunks_done_callback())
    }

    /// Returns the callback function that will be passed to various functions that may trigger
    /// the processing of new blocks. This callback will be called at the end of applying chunks
    /// for every block.
    fn get_apply_chunks_done_callback(&self) -> DoneApplyChunkCallback {
        let addr = self.my_address.clone();
        Arc::new(move |_| {
            addr.do_send(ApplyChunksDoneMessage {});
        })
    }

    /// Processes received block. Ban peer if the block header is invalid or the block is ill-formed.
    fn receive_block(&mut self, block: Block, peer_id: PeerId, was_requested: bool) {
        let hash = *block.hash();
        let _span = tracing::debug_span!(
            target: "client",
            "receive_block",
            me = ?self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
            prev_hash = %block.header().prev_hash(),
            %hash,
            height = block.header().height(),
            %peer_id,
            was_requested)
        .entered();
        let head = unwrap_or_return!(self.client.chain.head());
        let is_syncing = self.client.sync_status.is_syncing();
        if block.header().height() >= head.height + BLOCK_HORIZON && is_syncing && !was_requested {
            debug!(target: "client", head_height = head.height, "Dropping a block that is too far ahead.");
            return;
        }
        let tail = unwrap_or_return!(self.client.chain.tail());
        if block.header().height() < tail {
            debug!(target: "client", tail_height = tail, "Dropping a block that is too far behind.");
            return;
        }
        let prev_hash = *block.header().prev_hash();
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(block.into(), provenance, &peer_id) {
            Ok(_) => {}
            Err(ref err) if err.is_bad_data() => {
                warn!(target: "client", "Receive bad block: {}", err);
            }
            Err(ref err) if err.is_error() => {
                if let near_chain::Error::DBNotFoundErr(msg) = err {
                    debug_assert!(!msg.starts_with("BLOCK HEIGHT"), "{:?}", err);
                }
                if self.client.sync_status.is_syncing() {
                    // While syncing, we may receive blocks that are older or from next epochs.
                    // This leads to Old Block or EpochOutOfBounds errors.
                    debug!(target: "client", "Error on receival of block: {}", err);
                } else {
                    error!(target: "client", "Error on receival of block: {}", err);
                }
            }
            Err(e) => match e {
                near_chain::Error::Orphan => {
                    if !self.client.chain.is_orphan(&prev_hash) {
                        self.request_block(prev_hash, peer_id)
                    }
                }
                // missing chunks are already handled in self.client.process_block()
                // we don't need to do anything here
                near_chain::Error::ChunksMissing(_) => {}
                _ => {
                    debug!(target: "client", error = %e, "Process block: refused by chain");
                }
            },
        }
    }

    fn receive_headers(&mut self, headers: Vec<BlockHeader>, peer_id: PeerId) -> bool {
        info!(target: "client", "Received {} block headers from {}", headers.len(), peer_id);
        if headers.len() == 0 {
            return true;
        }
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

    fn request_block(&mut self, hash: CryptoHash, peer_id: PeerId) {
        match self.client.chain.block_exists(&hash) {
            Ok(false) => {
                self.network_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::BlockRequest { hash, peer_id },
                ));
            }
            Ok(true) => {
                debug!(target: "client", "send_block_request_to_peer: block {} already known", hash)
            }
            Err(e) => {
                error!(target: "client", "send_block_request_to_peer: failed to check block exists: {:?}", e)
            }
        }
    }

    /// Check whether need to (continue) sync.
    /// Also return higher height with known peers at that height.
    fn syncing_info(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.client.chain.head()?;
        let mut is_syncing = self.client.sync_status.is_syncing();

        let full_peer_info = if let Some(full_peer_info) =
            self.network_info.highest_height_peers.choose(&mut thread_rng())
        {
            full_peer_info
        } else {
            if !self.client.config.skip_sync_wait {
                warn!(target: "client", "Sync: no peers available, disabling sync");
            }
            return Ok((false, 0));
        };

        if is_syncing {
            if full_peer_info.chain_info.height <= head.height {
                info!(target: "client", "Sync: synced at {} [{}], {}, highest height peer: {}",
                      head.height, format_hash(head.last_block_hash),
                      full_peer_info.peer_info.id, full_peer_info.chain_info.height
                );
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.height
                > head.height + self.client.config.sync_height_threshold
            {
                info!(
                    target: "client",
                    "Sync: height: {}, peer id/height: {}/{}, enabling sync",
                    head.height,
                    full_peer_info.peer_info.id,
                    full_peer_info.chain_info.height,
                );
                is_syncing = true;
            }
        }
        Ok((is_syncing, full_peer_info.chain_info.height))
    }

    fn needs_syncing(&self, needs_syncing: bool) -> bool {
        !self.adv.disable_header_sync() && needs_syncing
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

        // Start main sync loop.
        self.sync(ctx);
    }

    /// Select the block hash we are using to sync state. It will sync with the state before applying the
    /// content of such block.
    ///
    /// The selected block will always be the first block on a new epoch:
    /// https://github.com/nearprotocol/nearcore/issues/2021#issuecomment-583039862
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

    fn run_timer<F>(
        &mut self,
        duration: Duration,
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

        now.checked_add_signed(chrono::Duration::from_std(duration).unwrap()).unwrap()
    }

    /// Main syncing job responsible for syncing client with other peers.
    /// Runs itself iff it was not ran as reaction for message with results of
    /// finishing state part job
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        let _span = tracing::debug_span!(target: "client", "sync").entered();
        let _d = delay_detector::DelayDetector::new(|| "client sync".into());
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later (($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);

                near_performance_metrics::actix::run_later(
                    ctx,
                    self.client.config.sync_step_period, move |act, ctx| {
                        act.sync(ctx);
                    }
                );
                return;
            }
        }));

        let mut wait_period = self.client.config.sync_step_period;

        let currently_syncing = self.client.sync_status.is_syncing();
        let (needs_syncing, highest_height) = unwrap_or_run_later!(self.syncing_info());

        if !self.needs_syncing(needs_syncing) {
            if currently_syncing {
                debug!(
                    target: "client",
                    "{:?} transitions to no sync",
                    self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
                );
                self.client.sync_status = SyncStatus::NoSync;

                // Initial transition out of "syncing" state.
                // Announce this client's account id if their epoch is coming up.
                let head = unwrap_or_run_later!(self.client.chain.head());
                self.check_send_announce_account(head.prev_block_hash);
            }
            wait_period = self.client.config.sync_check_period;
        } else {
            // Run each step of syncing separately.
            unwrap_or_run_later!(self.client.header_sync.run(
                &mut self.client.sync_status,
                &mut self.client.chain,
                highest_height,
                &self.network_info.highest_height_peers
            ));
            // Only body / state sync if header height is close to the latest.
            let header_head = unwrap_or_run_later!(self.client.chain.header_head());

            // Sync state if already running sync state or if block sync is too far.
            let sync_state = match self.client.sync_status {
                SyncStatus::StateSync(_, _) => true,
                _ if header_head.height
                    >= highest_height
                        .saturating_sub(self.client.config.block_header_fetch_horizon) =>
                {
                    unwrap_or_run_later!(self.client.block_sync.run(
                        &mut self.client.sync_status,
                        &mut self.client.chain,
                        highest_height,
                        &self.network_info.highest_height_peers
                    ))
                }
                _ => false,
            };
            if sync_state {
                let (sync_hash, mut new_shard_sync, just_enter_state_sync) =
                    match &self.client.sync_status {
                        SyncStatus::StateSync(sync_hash, shard_sync) => {
                            (*sync_hash, shard_sync.clone(), false)
                        }
                        _ => {
                            let sync_hash = unwrap_or_run_later!(self.find_sync_hash());
                            (sync_hash, HashMap::default(), true)
                        }
                    };

                let me = self.client.validator_signer.as_ref().map(|x| x.validator_id().clone());
                let block_header =
                    unwrap_or_run_later!(self.client.chain.get_block_header(&sync_hash));
                let prev_hash = *block_header.prev_hash();
                let epoch_id =
                    self.client.chain.get_block_header(&sync_hash).unwrap().epoch_id().clone();
                let shards_to_sync =
                    (0..self.client.runtime_adapter.num_shards(&epoch_id).unwrap())
                        .filter(|x| {
                            self.client.shards_mgr.cares_about_shard_this_or_next_epoch(
                                me.as_ref(),
                                &prev_hash,
                                *x,
                                true,
                            )
                        })
                        .collect();

                if !self.client.config.archive && just_enter_state_sync {
                    unwrap_or_run_later!(self.client.chain.reset_data_pre_state_sync(sync_hash));
                }

                match unwrap_or_run_later!(self.client.state_sync.run(
                    &me,
                    sync_hash,
                    &mut new_shard_sync,
                    &mut self.client.chain,
                    &self.client.runtime_adapter,
                    &self.network_info.highest_height_peers,
                    shards_to_sync,
                    &self.state_parts_task_scheduler,
                    &self.state_split_scheduler,
                )) {
                    StateSyncResult::Unchanged => (),
                    StateSyncResult::Changed(fetch_block) => {
                        self.client.sync_status = SyncStatus::StateSync(sync_hash, new_shard_sync);
                        if fetch_block {
                            if let Some(peer_info) =
                                self.network_info.highest_height_peers.choose(&mut thread_rng())
                            {
                                let id = peer_info.peer_info.id.clone();

                                if let Ok(header) = self.client.chain.get_block_header(&sync_hash) {
                                    for hash in
                                        vec![*header.prev_hash(), *header.hash()].into_iter()
                                    {
                                        self.request_block(hash, id.clone());
                                    }
                                }
                            }
                        }
                    }
                    StateSyncResult::Completed => {
                        info!(target: "sync", "State sync: all shards are done");

                        let mut block_processing_artifacts = BlockProcessingArtifact::default();

                        unwrap_or_run_later!(self.client.chain.reset_heads_post_state_sync(
                            &me,
                            sync_hash,
                            &mut block_processing_artifacts,
                            self.get_apply_chunks_done_callback(),
                        ));

                        self.client.process_block_processing_artifact(block_processing_artifacts);

                        self.client.sync_status = SyncStatus::BodySync {
                            start_height: 0,
                            current_height: 0,
                            highest_height: 0,
                        };
                    }
                }
            }
        }

        near_performance_metrics::actix::run_later(ctx, wait_period, move |act, ctx| {
            act.sync(ctx);
        });
    }

    /// Print current summary.
    fn log_summary(&mut self) {
        let _span = tracing::debug_span!(target: "client", "log_summary").entered();
        let _d = delay_detector::DelayDetector::new(|| "client log summary".into());
        let is_syncing = self.client.sync_status.is_syncing();
        let head = unwrap_or_return!(self.client.chain.head());
        let validator_info = if !is_syncing {
            let validators = unwrap_or_return!(self
                .client
                .runtime_adapter
                .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash));
            let num_validators = validators.len();
            let account_id = self.client.validator_signer.as_ref().map(|x| x.validator_id());
            let is_validator = if let Some(account_id) = account_id {
                match self.client.runtime_adapter.get_validator_by_account_id(
                    &head.epoch_id,
                    &head.last_block_hash,
                    account_id,
                ) {
                    Ok((_, is_slashed)) => !is_slashed,
                    Err(_) => false,
                }
            } else {
                false
            };
            Some(ValidatorInfoHelper { is_validator, num_validators })
        } else {
            None
        };

        let header_head = unwrap_or_return!(self.client.chain.header_head());
        let validator_epoch_stats = if is_syncing {
            // EpochManager::get_validator_info method (which is what runtime
            // adapter calls) is expensive when node is syncing so we’re simply
            // not collecting the statistics.  The statistics are used to update
            // a few Prometheus metrics only so we prefer to leave the metrics
            // unset until node finishes synchronising.  TODO(#6763): If we
            // manage to get get_validator_info fasts again (or return an error
            // if computation would be too slow), remove the ‘if is_syncing’
            // check.
            Default::default()
        } else {
            let epoch_identifier = ValidatorInfoIdentifier::BlockHash(header_head.last_block_hash);
            self.client
                .runtime_adapter
                .get_validator_info(epoch_identifier)
                .map(get_validator_epoch_stats)
                .unwrap_or_default()
        };
        let statistics = if self.client.config.enable_statistics_export {
            self.client.chain.store().get_store_statistics()
        } else {
            None
        };
        self.info_helper.info(
            &head,
            &self.client.sync_status,
            &self.node_id,
            &self.network_info,
            validator_info,
            validator_epoch_stats,
            self.client
                .runtime_adapter
                .get_protocol_upgrade_block_height(head.last_block_hash)
                .unwrap_or(None)
                .unwrap_or(0),
            statistics,
            &self.client.config,
        );
        debug!(target: "stats", "{}", self.client.chain.print_chain_processing_info_to_string(self.client.config.log_summary_style).unwrap_or(String::from("Upcoming block info failed.")));
    }
}

impl Drop for ClientActor {
    fn drop(&mut self) {
        let _span = tracing::debug_span!(target: "client", "drop").entered();
        self.state_parts_client_arbiter.stop();
    }
}

struct SyncJobsActor {
    client_addr: Addr<ClientActor>,
}

impl SyncJobsActor {
    const MAILBOX_CAPACITY: usize = 100;

    fn apply_parts(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<(), near_chain_primitives::error::Error> {
        let _span = tracing::debug_span!(target: "client", "apply_parts").entered();
        let store = msg.runtime.get_store();

        for part_id in 0..msg.num_parts {
            let key = StatePartKey(msg.sync_hash, msg.shard_id, part_id).try_to_vec()?;
            let part = store.get(DBCol::StateParts, &key)?.unwrap();

            msg.runtime.apply_state_part(
                msg.shard_id,
                &msg.state_root,
                PartId::new(part_id, msg.num_parts),
                &part,
                &msg.epoch_id,
            )?;
        }

        Ok(())
    }
}

impl Actor for SyncJobsActor {
    type Context = Context<Self>;
}

impl Handler<ApplyStatePartsRequest> for SyncJobsActor {
    type Result = ();

    fn handle(&mut self, msg: ApplyStatePartsRequest, _: &mut Self::Context) -> Self::Result {
        let _span =
            tracing::debug_span!(target: "client", "handle", handler = "ApplyStatePartsRequest")
                .entered();
        let result = self.apply_parts(&msg);

        self.client_addr.do_send(ApplyStatePartsResponse {
            apply_result: result,
            shard_id: msg.shard_id,
            sync_hash: msg.sync_hash,
        });
    }
}

impl Handler<ApplyStatePartsResponse> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: ApplyStatePartsResponse, _: &mut Self::Context) -> Self::Result {
        let _span =
            tracing::debug_span!(target: "client", "handle", handler = "ApplyStatePartsResponse")
                .entered();
        if let Some((sync, _, _)) = self.client.catchup_state_syncs.get_mut(&msg.sync_hash) {
            // We are doing catchup
            sync.set_apply_result(msg.shard_id, msg.apply_result);
        } else {
            self.client.state_sync.set_apply_result(msg.shard_id, msg.apply_result);
        }
    }
}

impl Handler<BlockCatchUpRequest> for SyncJobsActor {
    type Result = ();

    fn handle(&mut self, msg: BlockCatchUpRequest, _: &mut Self::Context) -> Self::Result {
        let _span =
            tracing::debug_span!(target: "client", "handle", handler = "BlockCatchUpRequest")
                .entered();
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_addr.do_send(BlockCatchUpResponse {
            sync_hash: msg.sync_hash,
            block_hash: msg.block_hash,
            results,
        });
    }
}

impl Handler<BlockCatchUpResponse> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: BlockCatchUpResponse, _: &mut Self::Context) -> Self::Result {
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

impl Handler<StateSplitRequest> for SyncJobsActor {
    type Result = ();

    fn handle(&mut self, msg: StateSplitRequest, _: &mut Self::Context) -> Self::Result {
        let _span = tracing::debug_span!(target: "client", "handle", handler = "StateSplitRequest")
            .entered();
        let results = msg.runtime.build_state_for_split_shards(
            msg.shard_uid,
            &msg.state_root,
            &msg.next_epoch_shard_layout,
        );

        self.client_addr.do_send(StateSplitResponse {
            sync_hash: msg.sync_hash,
            shard_id: msg.shard_id,
            new_state_roots: results,
        });
    }
}

impl Handler<StateSplitResponse> for ClientActor {
    type Result = ();

    fn handle(&mut self, msg: StateSplitResponse, _: &mut Self::Context) -> Self::Result {
        if let Some((sync, _, _)) = self.client.catchup_state_syncs.get_mut(&msg.sync_hash) {
            // We are doing catchup
            sync.set_split_result(msg.shard_id, msg.new_state_roots);
        } else {
            self.client.state_sync.set_split_result(msg.shard_id, msg.new_state_roots);
        }
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
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    node_id: PeerId,
    network_adapter: Arc<dyn PeerManagerAdapter>,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    telemetry_actor: Addr<TelemetryActor>,
    sender: Option<oneshot::Sender<()>>,
    adv: crate::adversarial::Controls,
) -> (Addr<ClientActor>, ArbiterHandle) {
    let client_arbiter = Arbiter::new();
    let client_arbiter_handle = client_arbiter.handle();
    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |ctx| {
        ClientActor::new(
            ctx.address(),
            client_config,
            chain_genesis,
            runtime_adapter,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_actor,
            true,
            random_seed_from_thread(),
            ctx,
            sender,
            adv,
        )
        .unwrap()
    });
    (client_addr, client_arbiter_handle)
}
