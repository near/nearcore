//! Client actor orchestrates Client and facilitates network connection.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use actix::{Actor, Addr, Arbiter, Context, Handler};
use actix_rt::ArbiterHandle;
use chrono::Duration as OldDuration;
use chrono::{DateTime, Utc};
use log::{debug, error, info, trace, warn};

#[cfg(feature = "delay_detector")]
use delay_detector::DelayDetector;
use near_chain::test_utils::format_hash;
use near_chain::types::AcceptedBlock;
#[cfg(feature = "adversarial")]
use near_chain::StoreValidator;
use near_chain::{
    byzantine_assert, Block, BlockHeader, ChainGenesis, ChainStoreAccess, Provenance,
    RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
#[cfg(feature = "adversarial")]
use near_chain_configs::GenesisConfig;
use near_crypto::Signature;
#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
#[cfg(feature = "adversarial")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{NetworkInfo, ReasonForBan};
use near_network::{
    NetworkAdapter, NetworkClientMessages, NetworkClientResponses, NetworkRequests,
};
use near_performance_metrics;
use near_performance_metrics_macros::{perf, perf_with_debug};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::{from_timestamp, MaybeValidated};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::ValidatorInfo;
#[cfg(feature = "adversarial")]
use near_store::ColBlock;
use near_telemetry::TelemetryActor;

use crate::client::Client;
use crate::info::{InfoHelper, ValidatorInfoHelper};
use crate::sync::{highest_height_peer, StateSync, StateSyncResult};
#[cfg(feature = "adversarial")]
use crate::AdversarialControls;
use crate::StatusResponse;
use near_client_primitives::types::{
    Error, GetNetworkInfo, NetworkInfoResponse, ShardSyncDownload, ShardSyncStatus, Status,
    StatusError, StatusSyncInfo, SyncStatus,
};
use near_primitives::block_header::ApprovalType;

/// Multiplier on `max_block_time` to wait until deciding that chain stalled.
const STATUS_WAIT_TIME_MULTIPLIER: u64 = 10;
/// Drop blocks whose height are beyond head + horizon if it is not in the current epoch.
const BLOCK_HORIZON: u64 = 500;
/// `max_block_production_time` times this multiplier is how long we wait before rebroadcasting
/// the current `head`
const HEAD_STALL_MULTIPLIER: u32 = 4;

pub struct ClientActor {
    /// Adversarial controls
    #[cfg(feature = "adversarial")]
    pub adv: Arc<RwLock<AdversarialControls>>,

    client: Client,
    network_adapter: Arc<dyn NetworkAdapter>,
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
    block_production_started: bool,
    doomslug_timer_next_attempt: DateTime<Utc>,
    chunk_request_retry_next_attempt: DateTime<Utc>,
    sync_started: bool,
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    loop {
        // Get chrono::Duration::num_seconds() by deducting genesis_time from now.
        let duration = genesis_time.signed_duration_since(Utc::now());
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
        config: ClientConfig,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        node_id: PeerId,
        network_adapter: Arc<dyn NetworkAdapter>,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        telemetry_actor: Addr<TelemetryActor>,
        enable_doomslug: bool,
        #[cfg(feature = "adversarial")] adv: Arc<RwLock<AdversarialControls>>,
    ) -> Result<Self, Error> {
        wait_until_genesis(&chain_genesis.time);
        if let Some(vs) = &validator_signer {
            info!(target: "client", "Starting validator node: {}", vs.validator_id());
        }
        let info_helper = InfoHelper::new(telemetry_actor, &config, validator_signer.clone());
        let client = Client::new(
            config,
            chain_genesis,
            runtime_adapter,
            network_adapter.clone(),
            validator_signer,
            enable_doomslug,
        )?;

        let now = Utc::now();
        Ok(ClientActor {
            #[cfg(feature = "adversarial")]
            adv,
            client,
            network_adapter,
            node_id,
            network_info: NetworkInfo {
                active_peers: vec![],
                num_active_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                #[cfg(feature = "metric_recorder")]
                metric_recorder: MetricRecorder::default(),
                peer_counter: 0,
            },
            last_validator_announce_time: None,
            info_helper,
            block_production_next_attempt: now,
            block_production_started: false,
            doomslug_timer_next_attempt: now,
            chunk_request_retry_next_attempt: now,
            sync_started: false,
        })
    }
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

        // Start periodic logging of current state of the client.
        self.log_summary(ctx);
    }
}

impl Handler<NetworkClientMessages> for ClientActor {
    type Result = NetworkClientResponses;

    #[perf_with_debug]
    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Context<Self>) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new(format!("NetworkClientMessage {}", msg.as_ref()).into());
        self.check_triggers(ctx);

        match msg {
            #[cfg(feature = "adversarial")]
            NetworkClientMessages::Adversarial(adversarial_msg) => {
                return match adversarial_msg {
                    NetworkAdversarialMessage::AdvDisableDoomslug => {
                        info!(target: "adversary", "Turning Doomslug off");
                        self.adv.write().unwrap().adv_disable_doomslug = true;
                        self.client.doomslug.adv_disable();
                        self.client.chain.adv_disable_doomslug();
                        NetworkClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvDisableHeaderSync => {
                        info!(target: "adversary", "Blocking header sync");
                        self.adv.write().unwrap().adv_disable_header_sync = true;
                        NetworkClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvProduceBlocks(num_blocks, only_valid) => {
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
                            self.network_adapter
                                .do_send(NetworkRequests::Block { block: block.clone() });
                            let (accepted_blocks, _) =
                                self.client.process_block(block, Provenance::PRODUCED);
                            for accepted_block in accepted_blocks {
                                self.client.on_block_accepted(
                                    accepted_block.hash,
                                    accepted_block.status,
                                    accepted_block.provenance,
                                );
                            }
                            blocks_produced += 1;
                            if blocks_produced == num_blocks {
                                break;
                            }
                        }
                        NetworkClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                        info!(target: "adversary", "Switching to height {:?}", height);
                        let mut chain_store_update = self.client.chain.mut_store().store_update();
                        chain_store_update.save_largest_target_height(height);
                        chain_store_update
                            .adv_save_latest_known(height)
                            .expect("adv method should not fail");
                        chain_store_update.commit().expect("adv method should not fail");
                        NetworkClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvGetSavedBlocks => {
                        info!(target: "adversary", "Requested number of saved blocks");
                        let store = self.client.chain.store().store();
                        let mut num_blocks = 0;
                        for _ in store.iter(ColBlock) {
                            num_blocks += 1;
                        }
                        NetworkClientResponses::AdvResult(num_blocks)
                    }
                    NetworkAdversarialMessage::AdvCheckStorageConsistency => {
                        // timeout is set to 1.5 seconds to give some room as we wait in Nightly for 2 seconds
                        let timeout = 1500;
                        info!(target: "adversary", "Check Storage Consistency, timeout set to {:?} milliseconds", timeout);
                        let mut genesis = GenesisConfig::default();
                        genesis.genesis_height = self.client.chain.store().get_genesis_height();
                        let mut store_validator = StoreValidator::new(
                            self.client.validator_signer.as_ref().map(|x| x.validator_id().clone()),
                            genesis,
                            self.client.runtime_adapter.clone(),
                            self.client.chain.store().owned_store(),
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
                    .mut_store()
                    .get_all_block_hashes_by_height(block.header().height());
                if was_requested || !blocks_at_height.is_ok() {
                    if let SyncStatus::StateSync(sync_hash, _) = &mut self.client.sync_status {
                        if let Ok(header) = self.client.chain.get_block_header(sync_hash) {
                            if block.hash() == header.prev_hash() {
                                if let Err(e) = self.client.chain.save_block(&block) {
                                    error!(target: "client", "Failed to save a block during state sync: {}", e);
                                }
                                return NetworkClientResponses::NoResponse;
                            } else if block.hash() == sync_hash {
                                if let Err(e) = self.client.chain.save_orphan(&block) {
                                    error!(target: "client", "Received an invalid block during state sync: {}", e);
                                }
                                return NetworkClientResponses::NoResponse;
                            }
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
                    if let Some((_, shards_to_download)) =
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
                                    match self
                                        .client
                                        .chain
                                        .set_state_part(shard_id, hash, part_id, num_parts, &data)
                                    {
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
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunkResponse(response) => {
                if let Ok(accepted_blocks) =
                    self.client.process_partial_encoded_chunk_response(response)
                {
                    self.process_accepted_blocks(accepted_blocks);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunk(partial_encoded_chunk) => {
                if let Ok(accepted_blocks) = self.client.process_partial_encoded_chunk(
                    MaybeValidated::NotValidated(partial_encoded_chunk),
                ) {
                    self.process_accepted_blocks(accepted_blocks);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunkForward(forward) => {
                match self.client.process_partial_encoded_chunk_forward(forward) {
                    Ok(accepted_blocks) => self.process_accepted_blocks(accepted_blocks),
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

impl Handler<Status> for ClientActor {
    type Result = Result<StatusResponse, StatusError>;

    #[perf]
    fn handle(&mut self, msg: Status, ctx: &mut Context<Self>) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("client status".to_string().into());
        self.check_triggers(ctx);

        let head = self.client.chain.head()?;
        let header = self.client.chain.get_block_header(&head.last_block_hash)?;
        let latest_block_time = header.raw_timestamp().clone();
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
        let validators = self
            .client
            .runtime_adapter
            .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash)?
            .into_iter()
            .map(|(validator_stake, is_slashed)| ValidatorInfo {
                account_id: validator_stake.take_account_id(),
                is_slashed,
            })
            .collect();

        let protocol_version =
            self.client.runtime_adapter.get_epoch_protocol_version(&head.epoch_id)?;

        let validator_account_id =
            self.client.validator_signer.as_ref().map(|vs| vs.validator_id()).cloned();

        Ok(StatusResponse {
            version: self.client.config.version.clone(),
            protocol_version,
            latest_protocol_version: PROTOCOL_VERSION,
            chain_id: self.client.config.chain_id.clone(),
            rpc_addr: self.client.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash.into(),
                latest_block_height: head.height,
                latest_state_root: header.prev_state_root().clone().into(),
                latest_block_time: from_timestamp(latest_block_time),
                syncing: self.client.sync_status.is_syncing(),
            },
            validator_account_id,
        })
    }
}

impl Handler<GetNetworkInfo> for ClientActor {
    type Result = Result<NetworkInfoResponse, String>;

    #[perf]
    fn handle(&mut self, msg: GetNetworkInfo, ctx: &mut Context<Self>) -> Self::Result {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("client get network info".into());
        self.check_triggers(ctx);

        Ok(NetworkInfoResponse {
            active_peers: self
                .network_info
                .active_peers
                .clone()
                .into_iter()
                .map(|a| a.peer_info)
                .collect::<Vec<_>>(),
            num_active_peers: self.network_info.num_active_peers,
            peer_max_count: self.network_info.peer_max_count,
            sent_bytes_per_sec: self.network_info.sent_bytes_per_sec,
            received_bytes_per_sec: self.network_info.received_bytes_per_sec,
            known_producers: self.network_info.known_producers.clone(),
            #[cfg(feature = "metric_recorder")]
            metric_recorder: self.network_info.metric_recorder.clone(),
        })
    }
}

impl ClientActor {
    fn sign_announce_account(&self, epoch_id: &EpochId) -> Result<Signature, ()> {
        if let Some(validator_signer) = self.client.validator_signer.as_ref() {
            Ok(validator_signer.sign_account_announce(
                &validator_signer.validator_id(),
                &self.node_id,
                epoch_id,
            ))
        } else {
            Err(())
        }
    }

    /// Check if client Account Id should be sent and send it.
    /// Account Id is sent when is not current a validator but are becoming a validator soon.
    fn check_send_announce_account(&mut self, prev_block_hash: CryptoHash) {
        // If no peers, there is no one to announce to.
        if self.network_info.num_active_peers == 0 {
            debug!(target: "client", "No peers: skip account announce");
            return;
        }

        // First check that we currently have an AccountId
        let validator_signer = match self.client.validator_signer.as_ref() {
            None => return,
            Some(signer) => signer,
        };

        let now = Instant::now();
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
            let signature = self.sign_announce_account(&next_epoch_id).unwrap();

            self.network_adapter.do_send(NetworkRequests::AnnounceAccount(AnnounceAccount {
                account_id: validator_signer.validator_id().clone(),
                peer_id: self.node_id.clone(),
                epoch_id: next_epoch_id,
                signature,
            }));
        }
    }

    /// Retrieves latest height, and checks if must produce next block.
    /// Otherwise wait for block arrival or suggest to skip after timeout.
    fn handle_block_production(&mut self) -> Result<(), Error> {
        // If syncing, don't try to produce blocks.
        if self.client.sync_status.is_syncing() {
            return Ok(());
        }

        let _ = self.client.check_and_update_doomslug_tip();

        let head = self.client.chain.head()?;
        let latest_known = self.client.chain.mut_store().get_latest_known()?;
        assert!(
            head.height <= latest_known.height,
            "Latest known height is invalid {} vs {}",
            head.height,
            latest_known.height
        );

        let epoch_id =
            self.client.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;

        for height in
            latest_known.height + 1..=self.client.doomslug.get_largest_height_crossing_threshold()
        {
            let next_block_producer_account =
                self.client.runtime_adapter.get_block_producer(&epoch_id, height)?;

            if self.client.validator_signer.as_ref().map(|bp| bp.validator_id())
                == Some(&next_block_producer_account)
            {
                let num_chunks = self.client.shards_mgr.num_chunks_for_block(&head.last_block_hash);
                let have_all_chunks =
                    head.height == 0 || num_chunks == self.client.runtime_adapter.num_shards();

                if self.client.doomslug.ready_to_produce_block(
                    Instant::now(),
                    height,
                    have_all_chunks,
                ) {
                    if let Err(err) = self.produce_block(height) {
                        // If there is an error, report it and let it retry on the next loop step.
                        error!(target: "client", "Block production failed: {}", err);
                    }
                }
            }
        }

        Ok(())
    }

    fn schedule_triggers(&mut self, ctx: &mut Context<Self>) {
        let wait = self.check_triggers(ctx);

        near_performance_metrics::actix::run_later(ctx, file!(), line!(), wait, move |act, ctx| {
            act.schedule_triggers(ctx);
        });
    }

    fn check_triggers(&mut self, ctx: &mut Context<ClientActor>) -> Duration {
        // There is a bug in Actix library. While there are messages in mailbox, Actix
        // will prioritize processing messages until mailbox is empty. Execution of any other task
        // scheduled with run_later will be delayed.

        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("client triggers".into());

        let mut delay = Duration::from_secs(1);
        let now = Utc::now();

        if self.sync_started {
            self.doomslug_timer_next_attempt = self.run_timer(
                self.client.config.doosmslug_step_period,
                self.doomslug_timer_next_attempt,
                ctx,
                |act, ctx| act.try_doomslug_timer(ctx),
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
        self.chunk_request_retry_next_attempt = self.run_timer(
            self.client.config.chunk_request_retry_period,
            self.chunk_request_retry_next_attempt,
            ctx,
            |act, _ctx| {
                if let Ok(header_head) = act.client.chain.header_head() {
                    act.client.shards_mgr.resend_chunk_requests(&header_head)
                }
            },
        );
        core::cmp::min(
            delay,
            self.chunk_request_retry_next_attempt
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(delay),
        )
    }

    fn try_handle_block_production(&mut self) {
        match self.handle_block_production() {
            Ok(()) => {}
            Err(err) => {
                error!(target: "client", "Handle block production failed: {:?}", err);
            }
        }
    }

    fn try_doomslug_timer(&mut self, _: &mut Context<ClientActor>) {
        let _ = self.client.check_and_update_doomslug_tip();

        let approvals = self.client.doomslug.process_timer(Instant::now());

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
        match self.client.produce_block(next_height) {
            Ok(Some(block)) => {
                let block_hash = *block.hash();
                let peer_id = self.node_id.clone();
                let prev_hash = *block.header().prev_hash();
                let block_protocol_version = block.header().latest_protocol_version();
                let res = self.process_block(block, Provenance::PRODUCED, &peer_id);
                match &res {
                    Ok(_) => Ok(()),
                    Err(e) => match e.kind() {
                        near_chain::ErrorKind::ChunksMissing(missing_chunks) => {
                            debug!(
                                "Chunks were missing for newly produced block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                                block_hash,
                                self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
                                missing_chunks,
                                missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                            );
                            let protocol_version = self
                                .client
                                .runtime_adapter
                                .get_epoch_id_from_prev_block(&prev_hash)
                                .and_then(|epoch| {
                                    self.client.runtime_adapter.get_epoch_protocol_version(&epoch)
                                })
                                .unwrap_or(block_protocol_version);
                            self.client.shards_mgr.request_chunks(
                                missing_chunks,
                                &self.client.chain.header_head().expect("header_head must be available when processing newly produced block"),
                                protocol_version,
                            );
                            Ok(())
                        }
                        _ => {
                            error!(target: "client", "Failed to process freshly produced block: {:?}", res);
                            byzantine_assert!(false);
                            res.map_err(|err| err.into())
                        }
                    },
                }
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Process all blocks that were accepted by calling other relevant services.
    fn process_accepted_blocks(&mut self, accepted_blocks: Vec<AcceptedBlock>) {
        for accepted_block in accepted_blocks {
            self.client.on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
            let block = self.client.chain.get_block(&accepted_block.hash).unwrap();
            let gas_used = Block::compute_gas_used(block.chunks().iter(), block.header().height());

            let last_final_hash = *block.header().last_final_block();

            self.info_helper.block_processed(gas_used);
            self.check_send_announce_account(last_final_hash);
        }
    }

    /// Process block and execute callbacks.
    fn process_block(
        &mut self,
        block: Block,
        provenance: Provenance,
        peer_id: &PeerId,
    ) -> Result<(), near_chain::Error> {
        // If we produced the block, send it out before we apply the block.
        // If we didn't produce the block and didn't request it, do basic validation
        // before sending it out.
        if provenance == Provenance::PRODUCED {
            self.network_adapter.do_send(NetworkRequests::Block { block: block.clone() });
        } else {
            match self.client.chain.validate_block(&block) {
                Ok(_) => {
                    let head = self.client.chain.head()?;
                    // do not broadcast blocks that are too far back.
                    if (head.height < block.header().height()
                        || &head.epoch_id == block.header().epoch_id())
                        && provenance == Provenance::NONE
                        && !self.client.sync_status.is_syncing()
                    {
                        self.client.rebroadcast_block(block.clone());
                    }
                }
                Err(e) => {
                    if e.is_bad_data() {
                        self.network_adapter.do_send(NetworkRequests::BanPeer {
                            peer_id: peer_id.clone(),
                            ban_reason: ReasonForBan::BadBlockHeader,
                        });
                        return Err(e);
                    }
                }
            }
        }
        let (accepted_blocks, result) = self.client.process_block(block, provenance);
        self.process_accepted_blocks(accepted_blocks);
        result.map(|_| ())
    }

    /// Processes received block. Ban peer if the block header is invalid or the block is ill-formed.
    fn receive_block(&mut self, block: Block, peer_id: PeerId, was_requested: bool) {
        let hash = *block.hash();
        debug!(target: "client", "{:?} Received block {} <- {} at {} from {}, requested: {}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), hash, block.header().prev_hash(), block.header().height(), peer_id, was_requested);
        let head = unwrap_or_return!(self.client.chain.head());
        let is_syncing = self.client.sync_status.is_syncing();
        if block.header().height() >= head.height + BLOCK_HORIZON && is_syncing && !was_requested {
            debug!(target: "client", "dropping block {} that is too far ahead. Block height {} current head height {}", block.hash(), block.header().height(), head.height);
            return;
        }
        let tail = unwrap_or_return!(self.client.chain.tail());
        if block.header().height() < tail {
            debug!(target: "client", "dropping block {} that is too far behind. Block height {} current tail height {}", block.hash(), block.header().height(), tail);
            return;
        }
        let prev_hash = *block.header().prev_hash();
        let block_protocol_version = block.header().latest_protocol_version();
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(block, provenance, &peer_id) {
            Ok(_) => {}
            Err(ref err) if err.is_bad_data() => {
                warn!(target: "client", "receive bad block: {}", err);
            }
            Err(ref err) if err.is_error() => {
                if let near_chain::ErrorKind::DBNotFoundErr(msg) = err.kind() {
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
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if !self.client.chain.is_orphan(&prev_hash) {
                        self.request_block_by_hash(prev_hash, peer_id)
                    }
                }
                near_chain::ErrorKind::ChunksMissing(missing_chunks) => {
                    debug!(
                        target: "client",
                        "Chunks were missing for block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                        hash.clone(),
                        self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
                        missing_chunks,
                        missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                    );
                    let protocol_version = self
                        .client
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&prev_hash)
                        .and_then(|epoch| {
                            self.client.runtime_adapter.get_epoch_protocol_version(&epoch)
                        })
                        .unwrap_or(block_protocol_version);
                    self.client.shards_mgr.request_chunks(
                        missing_chunks,
                        &self.client.chain.header_head().expect(
                            "header_head should always be available when block is received",
                        ),
                        protocol_version,
                    );
                }
                _ => {
                    debug!(target: "client", "Process block: block {} refused by chain: {}", hash, e.kind());
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

    fn request_block_by_hash(&mut self, hash: CryptoHash, peer_id: PeerId) {
        match self.client.chain.block_exists(&hash) {
            Ok(false) => {
                self.network_adapter.do_send(NetworkRequests::BlockRequest { hash, peer_id });
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
            highest_height_peer(&self.network_info.highest_height_peers)
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
        #[cfg(feature = "adversarial")]
        {
            if self.adv.read().unwrap().adv_disable_header_sync {
                return false;
            }
        }

        needs_syncing
    }

    /// Starts syncing and then switches to either syncing or regular mode.
    fn start_sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Wait for connections reach at least minimum peers unless skipping sync.
        if self.network_info.num_active_peers < self.client.config.min_num_peers
            && !self.client.config.skip_sync_wait
        {
            near_performance_metrics::actix::run_later(
                ctx,
                file!(),
                line!(),
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
    fn catchup(&mut self, ctx: &mut Context<ClientActor>) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("client catchup".into());
        match self.client.run_catchup(&self.network_info.highest_height_peers) {
            Ok(accepted_blocks) => {
                self.process_accepted_blocks(accepted_blocks);
            }
            Err(err) => {
                error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), err)
            }
        }

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
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
    ) -> DateTime<Utc>
    where
        F: FnOnce(&mut Self, &mut <Self as Actor>::Context) + 'static,
    {
        let now = Utc::now();
        if now < next_attempt {
            return next_attempt;
        }

        f(self, ctx);

        return now.checked_add_signed(OldDuration::from_std(duration).unwrap()).unwrap();
    }

    /// Main syncing job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        #[cfg(feature = "delay_detector")]
        let _d = DelayDetector::new("client sync".into());
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later(($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);

                near_performance_metrics::actix::run_later(
                    ctx,
                    file!(),
                    line!(),
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
                            (sync_hash.clone(), shard_sync.clone(), false)
                        }
                        _ => {
                            let sync_hash = unwrap_or_run_later!(self.find_sync_hash());
                            (sync_hash, HashMap::default(), true)
                        }
                    };

                let me = self.client.validator_signer.as_ref().map(|x| x.validator_id().clone());
                let shards_to_sync = (0..self.client.runtime_adapter.num_shards())
                    .filter(|x| {
                        self.client.shards_mgr.cares_about_shard_this_or_next_epoch(
                            me.as_ref(),
                            &sync_hash,
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
                )) {
                    StateSyncResult::Unchanged => (),
                    StateSyncResult::Changed(fetch_block) => {
                        self.client.sync_status = SyncStatus::StateSync(sync_hash, new_shard_sync);
                        if fetch_block {
                            if let Some(peer_info) =
                                highest_height_peer(&self.network_info.highest_height_peers)
                            {
                                if let Ok(header) = self.client.chain.get_block_header(&sync_hash) {
                                    for hash in
                                        vec![*header.prev_hash(), *header.hash()].into_iter()
                                    {
                                        self.request_block_by_hash(
                                            hash,
                                            peer_info.peer_info.id.clone(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    StateSyncResult::Completed => {
                        info!(target: "sync", "State sync: all shards are done");

                        let accepted_blocks = Arc::new(RwLock::new(vec![]));
                        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
                        let challenges = Arc::new(RwLock::new(vec![]));

                        unwrap_or_run_later!(self.client.chain.reset_heads_post_state_sync(
                            &me,
                            sync_hash,
                            |accepted_block| {
                                accepted_blocks.write().unwrap().push(accepted_block);
                            },
                            |missing_chunks| {
                                blocks_missing_chunks.write().unwrap().push(missing_chunks)
                            },
                            |challenge| challenges.write().unwrap().push(challenge)
                        ));

                        self.client.send_challenges(challenges);

                        self.process_accepted_blocks(
                            accepted_blocks.write().unwrap().drain(..).collect(),
                        );

                        self.client.shards_mgr.request_chunks(
                            blocks_missing_chunks.write().unwrap().drain(..).flatten(),
                            &self
                                .client
                                .chain
                                .header_head()
                                .expect("header_head must be available during sync"),
                            // It is ok to pass the latest protocol version here since we are likely
                            // syncing old blocks, which means the protocol version will not change
                            // the logic.
                            PROTOCOL_VERSION,
                        );

                        self.client.sync_status =
                            SyncStatus::BodySync { current_height: 0, highest_height: 0 };
                    }
                }
            }
        }

        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            wait_period,
            move |act, ctx| {
                act.sync(ctx);
            },
        );
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        near_performance_metrics::actix::run_later(
            ctx,
            file!(),
            line!(),
            self.client.config.log_summary_period,
            move |act, ctx| {
                #[cfg(feature = "delay_detector")]
                let _d = DelayDetector::new("client log summary".into());
                let is_syncing = act.client.sync_status.is_syncing();
                let head = unwrap_or_return!(act.client.chain.head(), act.log_summary(ctx));
                let validator_info = if !is_syncing {
                    let validators = unwrap_or_return!(
                        act.client.runtime_adapter.get_epoch_block_producers_ordered(
                            &head.epoch_id,
                            &head.last_block_hash
                        ),
                        act.log_summary(ctx)
                    );
                    let num_validators = validators.len();
                    let account_id = act.client.validator_signer.as_ref().map(|x| x.validator_id());
                    let is_validator = if let Some(ref account_id) = account_id {
                        match act.client.runtime_adapter.get_validator_by_account_id(
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

                act.info_helper.info(
                    act.client.chain.store().get_genesis_height(),
                    &head,
                    &act.client.sync_status,
                    &act.node_id,
                    &act.network_info,
                    validator_info,
                );

                act.log_summary(ctx);
            },
        );
    }
}

/// Starts client in a separate Arbiter (thread).
pub fn start_client(
    client_config: ClientConfig,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    node_id: PeerId,
    network_adapter: Arc<dyn NetworkAdapter>,
    validator_signer: Option<Arc<dyn ValidatorSigner>>,
    telemetry_actor: Addr<TelemetryActor>,
    #[cfg(feature = "adversarial")] adv: Arc<RwLock<AdversarialControls>>,
) -> (Addr<ClientActor>, ArbiterHandle) {
    let client_arbiter_handle = Arbiter::current();
    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |_ctx| {
        ClientActor::new(
            client_config,
            chain_genesis,
            runtime_adapter,
            node_id,
            network_adapter,
            validator_signer,
            telemetry_actor,
            true,
            #[cfg(feature = "adversarial")]
            adv,
        )
        .unwrap()
    });
    (client_addr, client_arbiter_handle)
}
