//! Client actor orchestrates Client and facilitates network connection.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use actix::{Actor, Addr, AsyncContext, Context, Handler};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};

#[cfg(feature = "adversarial")]
use near_chain::check_refcount_map;
use near_chain::test_utils::format_hash;
use near_chain::types::AcceptedBlock;
use near_chain::{
    byzantine_assert, Block, BlockHeader, ChainGenesis, ChainStoreAccess, Provenance,
    RuntimeAdapter,
};
use near_chain_configs::ClientConfig;
use near_crypto::Signature;
#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
#[cfg(feature = "adversarial")]
use near_network::types::NetworkAdversarialMessage;
use near_network::types::{NetworkInfo, ReasonForBan, StateResponseInfo};
use near_network::{
    NetworkAdapter, NetworkClientMessages, NetworkClientResponses, NetworkRequests,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::from_timestamp;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::views::ValidatorInfo;
#[cfg(feature = "adversarial")]
use near_store::ColBlock;
use near_telemetry::TelemetryActor;

use crate::client::Client;
use crate::info::InfoHelper;
use crate::sync::{highest_height_peer, StateSync, StateSyncResult};
use crate::types::{
    Error, GetNetworkInfo, NetworkInfoResponse, ShardSyncDownload, ShardSyncStatus, Status,
    StatusSyncInfo, SyncStatus,
};
use crate::StatusResponse;

/// Multiplier on `max_block_time` to wait until deciding that chain stalled.
const STATUS_WAIT_TIME_MULTIPLIER: u64 = 10;
/// Drop blocks whose height are beyond head + horizon.
const BLOCK_HORIZON: u64 = 500;

pub struct ClientActor {
    /// Adversarial controls
    #[cfg(feature = "adversarial")]
    pub adv_sync_info: Option<(u64, u64)>,
    #[cfg(feature = "adversarial")]
    pub adv_disable_header_sync: bool,
    #[cfg(feature = "adversarial")]
    pub adv_disable_doomslug: bool,

    client: Client,
    network_adapter: Arc<dyn NetworkAdapter>,
    network_info: NetworkInfo,
    /// Identity that represents this Client at the network level.
    /// It is used as part of the messages that identify this client.
    node_id: PeerId,
    /// Last height we announced our accounts as validators.
    last_validator_announce_height: Option<BlockHeight>,
    /// Last time we announced our accounts as validators.
    last_validator_announce_time: Option<Instant>,
    /// Info helper.
    info_helper: InfoHelper,
}

fn wait_until_genesis(genesis_time: &DateTime<Utc>) {
    let now = Utc::now();
    //get chrono::Duration::num_seconds() by deducting genesis_time from now
    let chrono_seconds = genesis_time.signed_duration_since(now).num_seconds();
    //check if number of seconds in chrono::Duration larger than zero
    if chrono_seconds > 0 {
        info!(target: "near", "Waiting until genesis: {}", chrono_seconds);
        let seconds = Duration::from_secs(chrono_seconds as u64);
        thread::sleep(seconds);
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

        Ok(ClientActor {
            #[cfg(feature = "adversarial")]
            adv_sync_info: None,
            #[cfg(feature = "adversarial")]
            adv_disable_header_sync: false,
            #[cfg(feature = "adversarial")]
            adv_disable_doomslug: false,
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
            },
            last_validator_announce_height: None,
            last_validator_announce_time: None,
            info_helper,
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
            self.block_production_tracking(ctx);
        }

        // Start chunk request retry job.
        self.chunk_request_retry(ctx);

        // Start catchup job.
        self.catchup(ctx);

        // Start periodic logging of current state of the client.
        self.log_summary(ctx);
    }
}

impl Handler<NetworkClientMessages> for ClientActor {
    type Result = NetworkClientResponses;

    fn handle(&mut self, msg: NetworkClientMessages, _: &mut Context<Self>) -> Self::Result {
        match msg {
            #[cfg(feature = "adversarial")]
            NetworkClientMessages::Adversarial(adversarial_msg) => {
                return match adversarial_msg {
                    NetworkAdversarialMessage::AdvDisableDoomslug => {
                        info!(target: "adversary", "Turning Doomslug off");
                        self.adv_disable_doomslug = true;
                        self.client.doomslug.adv_disable();
                        self.client.chain.adv_disable_doomslug();
                        NetworkClientResponses::NoResponse
                    }
                    NetworkAdversarialMessage::AdvDisableHeaderSync => {
                        info!(target: "adversary", "Blocking header sync");
                        self.adv_disable_header_sync = true;
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
                        chain_store_update.save_largest_skipped_height(&height);
                        chain_store_update.save_largest_approved_height(&height);
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
                    NetworkAdversarialMessage::AdvCheckRefMap => {
                        info!(target: "adversary", "Check Block Reference Map");
                        match check_refcount_map(&mut self.client.chain) {
                            Ok(_) => NetworkClientResponses::AdvResult(1 /* true */),
                            Err(e) => {
                                error!(target: "client", "Block Reference Map is inconsistent: {:?}", e);
                                NetworkClientResponses::AdvResult(0 /* false */)
                            }
                        }
                    }
                    _ => panic!("invalid adversary message"),
                };
            }
            NetworkClientMessages::Transaction { transaction, is_forwarded } => {
                self.client.process_tx(transaction, is_forwarded)
            }
            NetworkClientMessages::Block(block, peer_id, was_requested) => {
                let blocks_at_height = self
                    .client
                    .chain
                    .mut_store()
                    .get_all_block_hashes_by_height(block.header.inner_lite.height);
                if was_requested || !blocks_at_height.is_ok() {
                    if let SyncStatus::StateSync(sync_hash, _) = &mut self.client.sync_status {
                        if let Ok(header) = self.client.chain.get_block_header(sync_hash) {
                            if block.hash() == header.prev_hash {
                                if let Err(_) = self.client.chain.save_block(&block) {
                                    error!(target: "client", "Failed to save a block during state sync");
                                }
                                return NetworkClientResponses::NoResponse;
                            } else if &block.hash() == sync_hash {
                                self.client.chain.save_orphan(&block);
                                return NetworkClientResponses::NoResponse;
                            }
                        }
                    }
                    self.receive_block(block, peer_id, was_requested)
                } else {
                    match self
                        .client
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&block.header.prev_hash)
                    {
                        Ok(epoch_id) => {
                            if let Some(hashes) = blocks_at_height.unwrap().get(&epoch_id) {
                                if !hashes.contains(&block.header.hash) {
                                    warn!(target: "client", "Rejecting unrequested block {}, height {}", block.header.hash, block.header.inner_lite.height);
                                }
                            }
                        }
                        _ => {}
                    }
                    return NetworkClientResponses::NoResponse;
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
            NetworkClientMessages::BlockApproval(approval, _) => {
                self.client.collect_block_approval(&approval, false);
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::StateResponse(StateResponseInfo {
                shard_id,
                sync_hash: hash,
                state_response,
            }) => {
                // Get the download that matches the shard_id and hash
                let download = {
                    let mut download: Option<&mut ShardSyncDownload> = None;

                    // ... It could be that the state was requested by the state sync
                    if let SyncStatus::StateSync(sync_hash, shards_to_download) =
                        &mut self.client.sync_status
                    {
                        if hash == *sync_hash {
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
                            if let Some(header) = state_response.header {
                                if !shard_sync_download.downloads[0].done {
                                    match self.client.chain.set_state_header(
                                        shard_id,
                                        hash,
                                        header.clone(),
                                    ) {
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
                            if let Some(part) = state_response.part {
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
            NetworkClientMessages::PartialEncodedChunkRequest(part_request_msg, route_back) => {
                let _ = self.client.shards_mgr.process_partial_encoded_chunk_request(
                    part_request_msg,
                    route_back,
                    self.client.chain.mut_store(),
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::PartialEncodedChunk(partial_encoded_chunk) => {
                if let Ok(accepted_blocks) =
                    self.client.process_partial_encoded_chunk(partial_encoded_chunk)
                {
                    self.process_accepted_blocks(accepted_blocks);
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
    type Result = Result<StatusResponse, String>;

    fn handle(&mut self, msg: Status, _: &mut Context<Self>) -> Self::Result {
        let head = self.client.chain.head().map_err(|err| err.to_string())?;
        let header = self
            .client
            .chain
            .get_block_header(&head.last_block_hash)
            .map_err(|err| err.to_string())?;
        let latest_block_time = header.inner_lite.timestamp.clone();
        if msg.is_health_check {
            let elapsed = (Utc::now() - from_timestamp(latest_block_time)).to_std().unwrap();
            if elapsed
                > Duration::from_millis(
                    self.client.config.max_block_production_delay.as_millis() as u64
                        * STATUS_WAIT_TIME_MULTIPLIER,
                )
            {
                return Err(format!("No blocks for {:?}.", elapsed));
            }
        }
        let validators = self
            .client
            .runtime_adapter
            .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(validator_stake, is_slashed)| ValidatorInfo {
                account_id: validator_stake.account_id,
                is_slashed,
            })
            .collect();
        Ok(StatusResponse {
            version: self.client.config.version.clone(),
            chain_id: self.client.config.chain_id.clone(),
            rpc_addr: self.client.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash.into(),
                latest_block_height: head.height,
                latest_state_root: header.inner_lite.prev_state_root.clone().into(),
                latest_block_time: from_timestamp(latest_block_time),
                syncing: self.client.sync_status.is_syncing(),
            },
        })
    }
}

impl Handler<GetNetworkInfo> for ClientActor {
    type Result = Result<NetworkInfoResponse, String>;

    fn handle(&mut self, _: GetNetworkInfo, _: &mut Context<Self>) -> Self::Result {
        Ok(NetworkInfoResponse {
            active_peers: self
                .network_info
                .active_peers
                .clone()
                .into_iter()
                .map(|a| a.peer_info.clone())
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
        if self.client.validator_signer.is_none() {
            // There is no account id associated with this client
            return;
        }
        let validator_signer = self.client.validator_signer.as_ref().unwrap();

        let now = Instant::now();
        // Check that we haven't announced it too recently
        if let Some(last_validator_announce_time) = self.last_validator_announce_time {
            // Don't make announcement if have passed less than half of the time in which other peers
            // should remove our Account Id from their Routing Tables.
            if 2 * (now - last_validator_announce_time) < self.client.config.ttl_account_id_router {
                return;
            }
        }

        let epoch_start_height = unwrap_or_return!(
            self.client.runtime_adapter.get_epoch_start_height(&prev_block_hash),
            ()
        );

        debug!(target: "client", "Check announce account for {}, epoch start height: {}, {:?}", validator_signer.validator_id(), epoch_start_height, self.last_validator_announce_height);

        if let Some(last_validator_announce_height) = self.last_validator_announce_height {
            if last_validator_announce_height >= epoch_start_height {
                // This announcement was already done!
                return;
            }
        }

        // Announce AccountId if client is becoming a validator soon.
        let next_epoch_id = unwrap_or_return!(self
            .client
            .runtime_adapter
            .get_next_epoch_id_from_prev_block(&prev_block_hash));

        // Check client is part of the futures validators
        if let Ok(validators) = self
            .client
            .runtime_adapter
            .get_epoch_block_producers_ordered(&next_epoch_id, &prev_block_hash)
        {
            if validators.iter().any(|(validator_stake, _)| {
                &validator_stake.account_id == validator_signer.validator_id()
            }) {
                debug!(target: "client", "Sending announce account for {}", validator_signer.validator_id());
                self.last_validator_announce_height = Some(epoch_start_height);
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
            format!("Latest known height is invalid {} vs {}", head.height, latest_known.height)
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
                        error!(target: "client", "Block production failed: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }

    /// Runs a loop to keep track of the latest known block, produces if it's this validators turn.
    fn block_production_tracking(&mut self, ctx: &mut Context<Self>) {
        match self.handle_block_production() {
            Ok(()) => {}
            Err(err) => {
                error!(target: "client", "Handle block production failed: {:?}", err);
            }
        }
        let wait = self.client.config.block_production_tracking_delay;
        ctx.run_later(wait, move |act, ctx| {
            act.block_production_tracking(ctx);
        });
    }

    /// Produce block if we are block producer for given `next_height` height.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block(&mut self, next_height: BlockHeight) -> Result<(), Error> {
        match self.client.produce_block(next_height) {
            Ok(Some(block)) => {
                let block_hash = block.hash();
                let res = self.process_block(block, Provenance::PRODUCED);
                match &res {
                    Ok(_) => Ok(()),
                    Err(e) => match e.kind() {
                        near_chain::ErrorKind::ChunksMissing(missing_chunks) => {
                            debug!(
                                "Chunks were missing for newly produced block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                                block_hash,
                                self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
                                missing_chunks.clone(),
                                missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                            );
                            self.client.shards_mgr.request_chunks(missing_chunks).unwrap();
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
            let gas_used = Block::compute_gas_used(&block.chunks, block.header.inner_lite.height);
            let gas_limit = Block::compute_gas_limit(&block.chunks, block.header.inner_lite.height);

            self.info_helper.block_processed(gas_used, gas_limit);
            self.check_send_announce_account(accepted_block.hash);
        }
    }

    /// Process block and execute callbacks.
    fn process_block(
        &mut self,
        block: Block,
        provenance: Provenance,
    ) -> Result<(), near_chain::Error> {
        // If we produced the block, send it out before we apply the block.
        // If we didn't produce the block and didn't request it, do basic validation
        // before sending it out.
        if provenance == Provenance::PRODUCED {
            self.network_adapter.do_send(NetworkRequests::Block { block: block.clone() });
        } else if provenance == Provenance::NONE {
            // Don't care about challenge here since it will be handled when we actually process
            // the block.
            if self.client.chain.process_block_header(&block.header, |_| {}).is_ok() {
                let head = self.client.chain.head()?;
                // do not broadcast blocks that are too far back.
                if head.height < block.header.inner_lite.height
                    || head.epoch_id == block.header.inner_lite.epoch_id
                {
                    self.client.rebroadcast_block(block.clone());
                }
            }
        }
        let (accepted_blocks, result) = self.client.process_block(block, provenance);
        self.process_accepted_blocks(accepted_blocks);
        result.map(|_| ())
    }

    /// Processes received block, returns boolean if block was reasonable or malicious.
    fn receive_block(
        &mut self,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
    ) -> NetworkClientResponses {
        let hash = block.hash();
        debug!(target: "client", "{:?} Received block {} <- {} at {} from {}, requested: {}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), hash, block.header.prev_hash, block.header.inner_lite.height, peer_id, was_requested);
        // drop the block if it is too far ahead
        let head = unwrap_or_return!(self.client.chain.head(), NetworkClientResponses::NoResponse);
        if block.header.inner_lite.height >= head.height + BLOCK_HORIZON {
            debug!(target: "client", "dropping block {} that is too far ahead. Block height {} current head height {}", block.hash(), block.header.inner_lite.height, head.height);
            return NetworkClientResponses::NoResponse;
        }
        let prev_hash = block.header.prev_hash;
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(block, provenance) {
            Ok(_) => NetworkClientResponses::NoResponse,
            Err(ref err) if err.is_bad_data() => {
                NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlock }
            }
            Err(ref err) if err.is_error() => {
                if self.client.sync_status.is_syncing() {
                    // While syncing, we may receive blocks that are older or from next epochs.
                    // This leads to Old Block or EpochOutOfBounds errors.
                    debug!(target: "client", "Error on receival of block: {}", err);
                } else {
                    error!(target: "client", "Error on receival of block: {}", err);
                }
                NetworkClientResponses::NoResponse
            }
            Err(e) => match e.kind() {
                near_chain::ErrorKind::Orphan => {
                    if !self.client.chain.is_orphan(&prev_hash) {
                        self.request_block_by_hash(prev_hash, peer_id)
                    }
                    NetworkClientResponses::NoResponse
                }
                near_chain::ErrorKind::ChunksMissing(missing_chunks) => {
                    debug!(
                        target: "client",
                        "Chunks were missing for block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                        hash.clone(),
                        self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
                        missing_chunks.clone(),
                        missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                    );
                    self.client.shards_mgr.request_chunks(missing_chunks).unwrap();
                    NetworkClientResponses::NoResponse
                }
                _ => {
                    debug!(target: "client", "Process block: block {} refused by chain: {}", hash, e.kind());
                    NetworkClientResponses::NoResponse
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
            if full_peer_info.chain_info.score_and_height() <= head.score_and_height() {
                info!(target: "client", "Sync: synced at {} @ {:?} [{}], {}, highest height peer: {} @ {:?}",
                      head.height, head.score, format_hash(head.last_block_hash),
                    full_peer_info.peer_info.id,
                    full_peer_info.chain_info.height, full_peer_info.chain_info.score
                );
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.score_and_height().beyond_threshold(
                &head.score_and_height(),
                self.client.config.sync_height_threshold,
            ) {
                info!(
                    target: "client",
                    "Sync: height/score: {}/{}, peer id/height//score: {}/{}/{}, enabling sync",
                    head.height,
                    head.score,
                    full_peer_info.peer_info.id,
                    full_peer_info.chain_info.height,
                    full_peer_info.chain_info.score,
                );
                is_syncing = true;
            }
        }
        Ok((is_syncing, full_peer_info.chain_info.height))
    }

    fn needs_syncing(&self, needs_syncing: bool) -> bool {
        #[cfg(feature = "adversarial")]
        {
            if self.adv_disable_header_sync {
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
            ctx.run_later(self.client.config.sync_step_period, move |act, ctx| {
                act.start_sync(ctx);
            });
            return;
        }

        // Start doomslug timer
        self.doomslug_timer(ctx);

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
            sync_hash = self.client.chain.get_block_header(&sync_hash)?.prev_hash;
        }
        let epoch_start_sync_hash =
            StateSync::get_epoch_start_sync_hash(&mut self.client.chain, &sync_hash)?;
        Ok(epoch_start_sync_hash)
    }

    /// Runs catchup on repeat, if this client is a validator.
    fn catchup(&mut self, ctx: &mut Context<ClientActor>) {
        match self.client.run_catchup(&self.network_info.highest_height_peers) {
            Ok(accepted_blocks) => {
                self.process_accepted_blocks(accepted_blocks);
            }
            Err(err) => {
                error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.client.validator_signer.as_ref().map(|vs| vs.validator_id()), err)
            }
        }

        ctx.run_later(self.client.config.catchup_step_period, move |act, ctx| {
            act.catchup(ctx);
        });
    }

    /// Job to retry chunks that were requested but not received within expected time.
    fn chunk_request_retry(&mut self, ctx: &mut Context<ClientActor>) {
        match self.client.shards_mgr.resend_chunk_requests() {
            Ok(_) => {}
            Err(err) => {
                error!(target: "client", "Failed to resend chunk requests: {}", err);
            }
        };
        ctx.run_later(self.client.config.chunk_request_retry_period, move |act, ctx| {
            act.chunk_request_retry(ctx);
        });
    }

    /// An actix recursive function that processes doomslug timer
    fn doomslug_timer(&mut self, ctx: &mut Context<ClientActor>) {
        let _ = self.client.check_and_update_doomslug_tip();

        let approvals = self.client.doomslug.process_timer(Instant::now());

        // Important to save the largest skipped and endorsed heights before sending approvals, so
        // that if the node crashes in the meantime, we cannot get slashed on recovery
        let mut chain_store_update = self.client.chain.mut_store().store_update();
        chain_store_update
            .save_largest_skipped_height(&self.client.doomslug.get_largest_skipped_height());
        chain_store_update
            .save_largest_endorsed_height(&self.client.doomslug.get_largest_endorsed_height());

        match chain_store_update.commit() {
            Ok(_) => {
                for approval in approvals {
                    // `Chain::process_approval` updates metrics related to the finality gadget.
                    // Don't send the approval if such an update failed
                    if let Ok(_) = self.client.chain.process_approval(
                        &self.client.validator_signer.as_ref().map(|x| x.validator_id().clone()),
                        &approval,
                    ) {
                        if let Err(e) = self.client.send_approval(approval) {
                            error!("Error while sending an approval {:?}", e);
                        }
                    }
                }
            }
            Err(e) => error!("Error while committing largest skipped height {:?}", e),
        };

        ctx.run_later(Duration::from_millis(50), move |act, ctx| {
            act.doomslug_timer(ctx);
        });
    }

    /// Main syncing job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later(($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);
                ctx.run_later(self.client.config.sync_step_period, move |act, ctx| {
                    act.sync(ctx);
                });
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
                let (sync_hash, mut new_shard_sync) = match &self.client.sync_status {
                    SyncStatus::StateSync(sync_hash, shard_sync) => {
                        (sync_hash.clone(), shard_sync.clone())
                    }
                    _ => {
                        let sync_hash = unwrap_or_run_later!(self.find_sync_hash());
                        (sync_hash, HashMap::default())
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

                unwrap_or_run_later!(self.client.chain.reset_data_pre_state_sync(sync_hash));

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
                                    for hash in vec![header.prev_hash, header.hash].into_iter() {
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
                        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
                            self.client.shards_mgr.request_chunks(missing_chunks).unwrap();
                        }

                        self.client.sync_status =
                            SyncStatus::BodySync { current_height: 0, highest_height: 0 };
                    }
                }
            }
        }

        ctx.run_later(wait_period, move |act, ctx| {
            act.sync(ctx);
        });
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        ctx.run_later(self.client.config.log_summary_period, move |act, ctx| {
            let head = unwrap_or_return!(act.client.chain.head());
            let validators = unwrap_or_return!(act
                .client
                .runtime_adapter
                .get_epoch_block_producers_ordered(&head.epoch_id, &head.last_block_hash));
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
            let is_fishermen = if let Some(ref account_id) = account_id {
                match act.client.runtime_adapter.get_fisherman_by_account_id(
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

            act.info_helper.info(
                act.client.chain.store().get_genesis_height(),
                &head,
                &act.client.sync_status,
                &act.node_id,
                &act.network_info,
                is_validator,
                is_fishermen,
                num_validators,
            );

            act.log_summary(ctx);
        });
    }
}
