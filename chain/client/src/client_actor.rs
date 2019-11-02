//! Client actor is orchestrates Client and facilitates network connection.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient,
    WrapFuture,
};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};

use near_chain::types::{
    AcceptedBlock, ShardStateSyncResponse, ValidatorSignatureVerificationResult,
};
use near_chain::{
    byzantine_assert, Block, BlockHeader, ChainGenesis, ChainStoreAccess, Provenance,
    RuntimeAdapter,
};
use near_chunks::{NetworkAdapter, NetworkRecipient};
use near_crypto::Signature;
use near_network::types::{AnnounceAccount, NetworkInfo, PeerId, ReasonForBan, StateResponseInfo};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockIndex, EpochId, Range};
use near_primitives::unwrap_or_return;
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::ValidatorInfo;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::client::Client;
use crate::info::InfoHelper;
use crate::sync::{most_weight_peer, StateSyncResult};
use crate::types::{
    BlockProducer, ClientConfig, Error, GetNetworkInfo, NetworkInfoResponse, ShardSyncStatus,
    Status, StatusSyncInfo, SyncStatus,
};
use crate::{sync, StatusResponse};
use cached::Cached;
use near_primitives::block::GenesisId;

enum AccountAnnounceVerificationResult {
    Valid,
    UnknownEpoch,
    Invalid(ReasonForBan),
}

pub struct ClientActor {
    client: Client,
    network_actor: Recipient<NetworkRequests>,
    network_adapter: Arc<dyn NetworkAdapter>,
    network_info: NetworkInfo,
    /// Identity that represents this Client at the network level.
    /// It is used as part of the messages that identify this client.
    node_id: PeerId,
    /// Last height we announced our accounts as validators.
    last_validator_announce_height: Option<BlockIndex>,
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
        info!(target: "chain", "Waiting until genesis: {}", chrono_seconds);
        let seconds = Duration::from_secs(chrono_seconds as u64);
        thread::sleep(seconds);
    }
}

impl ClientActor {
    pub fn new(
        config: ClientConfig,
        store: Arc<Store>,
        chain_genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        node_id: PeerId,
        network_actor: Recipient<NetworkRequests>,
        block_producer: Option<BlockProducer>,
        telemetry_actor: Addr<TelemetryActor>,
    ) -> Result<Self, Error> {
        wait_until_genesis(&chain_genesis.time);
        if let Some(bp) = &block_producer {
            info!(target: "client", "Starting validator node: {}", bp.account_id);
        }
        let info_helper = InfoHelper::new(telemetry_actor, block_producer.clone());
        let network_adapter = Arc::new(NetworkRecipient::new(network_actor.clone()));
        let client = Client::new(
            config,
            store,
            chain_genesis,
            runtime_adapter,
            network_adapter.clone(),
            block_producer,
        )?;

        Ok(ClientActor {
            client,
            network_actor,
            network_adapter,
            node_id,
            network_info: NetworkInfo {
                active_peers: vec![],
                num_active_peers: 0,
                peer_max_count: 0,
                most_weight_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
            },
            last_validator_announce_height: None,
            last_validator_announce_time: None,
            info_helper,
        })
    }
    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> AccountAnnounceVerificationResult {
        let announce_hash = announce_account.hash();
        let head = unwrap_or_return!(
            self.client.chain.head(),
            AccountAnnounceVerificationResult::UnknownEpoch
        );

        // If we are currently not at the epoch that this announcement is in, skip it.
        if announce_account.epoch_id != head.epoch_id {
            return AccountAnnounceVerificationResult::UnknownEpoch;
        }

        match self.client.runtime_adapter.verify_validator_signature(
            &announce_account.epoch_id,
            &head.last_block_hash,
            &announce_account.account_id,
            announce_hash.as_ref(),
            &announce_account.signature,
        ) {
            ValidatorSignatureVerificationResult::Valid => AccountAnnounceVerificationResult::Valid,
            ValidatorSignatureVerificationResult::Invalid => {
                AccountAnnounceVerificationResult::Invalid(ReasonForBan::InvalidSignature)
            }
            ValidatorSignatureVerificationResult::UnknownEpoch => {
                AccountAnnounceVerificationResult::UnknownEpoch
            }
        }
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.start_sync(ctx);

        // Start block production tracking if have block producer info.
        if let Some(_) = self.client.block_producer {
            self.block_production_tracking(ctx);
        }

        // Start chunk request retry job.
        self.chunk_request_retry(ctx);

        // Start catchup job.
        self.catchup(ctx);

        // Start fetching information from network.
        self.fetch_network_info(ctx);

        // Start periodic logging of current state of the client.
        self.log_summary(ctx);
    }
}

impl Handler<NetworkClientMessages> for ClientActor {
    type Result = NetworkClientResponses;

    fn handle(&mut self, msg: NetworkClientMessages, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkClientMessages::Transaction(tx) => self.client.process_tx(tx),
            NetworkClientMessages::TxStatus { tx_hash, signer_account_id } => {
                self.client.get_tx_status(tx_hash, signer_account_id)
            }
            NetworkClientMessages::TxStatusResponse(tx_result) => {
                let tx_hash = tx_result.transaction.id;
                if self.client.tx_status_requests.cache_remove(&tx_hash).is_some() {
                    self.client.tx_status_response.cache_set(tx_hash, tx_result);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::BlockHeader(header, peer_id) => {
                self.receive_header(header, peer_id)
            }
            NetworkClientMessages::Block(block, peer_id, was_requested) => {
                if let SyncStatus::StateSync(sync_hash, _) = &mut self.client.sync_status {
                    if let Ok(header) = self.client.chain.get_block_header(sync_hash) {
                        if block.hash() == header.inner.prev_hash {
                            if let Err(_) = self.client.chain.save_block(&block) {
                                error!(target: "client", "Failed to save a block during state sync");
                            }
                            return NetworkClientResponses::NoResponse;
                        }
                    }
                }
                self.receive_block(block, peer_id, was_requested)
            }
            NetworkClientMessages::BlockRequest(hash) => {
                if let Ok(block) = self.client.chain.get_block(&hash) {
                    NetworkClientResponses::Block(block.clone())
                } else {
                    NetworkClientResponses::NoResponse
                }
            }
            NetworkClientMessages::BlockHeadersRequest(hashes) => {
                if let Ok(headers) = self.retrieve_headers(hashes) {
                    NetworkClientResponses::BlockHeaders(headers)
                } else {
                    NetworkClientResponses::NoResponse
                }
            }
            NetworkClientMessages::GetChainInfo => match self.client.chain.head() {
                Ok(head) => NetworkClientResponses::ChainInfo {
                    genesis_id: GenesisId {
                        chain_id: self.client.chain.chain_id().clone(),
                        hash: self.client.chain.genesis().hash(),
                    },
                    height: head.height,
                    total_weight: head.total_weight,
                },
                Err(err) => {
                    error!(target: "client", "{}", err);
                    NetworkClientResponses::NoResponse
                }
            },
            NetworkClientMessages::BlockHeaders(headers, peer_id) => {
                if self.receive_headers(headers, peer_id) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block headers");
                    NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockHeader }
                }
            }
            NetworkClientMessages::BlockApproval(account_id, hash, signature, peer_id) => {
                if self.client.collect_block_approval(&account_id, &hash, &signature, &peer_id) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block approval: {} {} {}", account_id, hash, signature);
                    NetworkClientResponses::NoResponse

                    // TODO(1259): The originator of this message is not the immediate sender so we should not ban him.
                    // NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockApproval }
                }
            }
            NetworkClientMessages::StateRequest(shard_id, hash, need_header, parts_ranges) => {
                let mut parts = vec![];
                for Range(from, to) in parts_ranges {
                    for part_id in from..to {
                        if let Ok(part) =
                            self.client.chain.get_state_response_part(shard_id, part_id, hash)
                        {
                            parts.push(part);
                        } else {
                            return NetworkClientResponses::NoResponse;
                        }
                    }
                }
                if need_header {
                    match self.client.chain.get_state_response_header(shard_id, hash) {
                        Ok(header) => {
                            return NetworkClientResponses::StateResponse(StateResponseInfo {
                                shard_id,
                                hash,
                                shard_state: ShardStateSyncResponse { header: Some(header), parts },
                            });
                        }
                        Err(_) => {
                            return NetworkClientResponses::NoResponse;
                        }
                    }
                } else {
                    return NetworkClientResponses::StateResponse(StateResponseInfo {
                        shard_id,
                        hash,
                        shard_state: ShardStateSyncResponse { header: None, parts },
                    });
                }
            }
            NetworkClientMessages::StateResponse(StateResponseInfo {
                shard_id,
                hash,
                shard_state,
            }) => {
                // Populate the hashmaps with shard statuses that might be interested in this state
                let mut shards_to_download_vec = vec![];

                // ... It could be that the state was requested by the state sync
                if let SyncStatus::StateSync(sync_hash, shards_to_download) =
                    &mut self.client.sync_status
                {
                    if hash == *sync_hash {
                        shards_to_download_vec.push(shards_to_download);
                    }
                }

                // ... Or one of the catchups
                for (sync_hash, state_sync_info) in
                    self.client.chain.store().iterate_state_sync_infos()
                {
                    if hash == sync_hash {
                        assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
                        if let Some((_, shards_to_download)) =
                            self.client.catchup_state_syncs.get_mut(&sync_hash)
                        {
                            shards_to_download_vec.push(shards_to_download);
                        }
                        // We should not be requesting the same state twice.
                        break;
                    }
                }

                for shards_to_download in shards_to_download_vec {
                    let shard_sync_download = if shards_to_download.contains_key(&shard_id) {
                        &shards_to_download[&shard_id]
                    } else {
                        // TODO is this correct behavior?
                        continue;
                    };
                    let mut new_shard_download = shard_sync_download.clone();

                    match shard_sync_download.status {
                        ShardSyncStatus::StateDownloadHeader => {
                            if let Some(header) = &shard_state.header {
                                if !shard_sync_download.downloads[0].done {
                                    match self.client.chain.set_state_header(
                                        shard_id,
                                        hash,
                                        header.clone(),
                                    ) {
                                        Ok(()) => {
                                            new_shard_download.downloads[0].done = true;
                                        }
                                        Err(_err) => {
                                            new_shard_download.downloads[0].error = true;
                                        }
                                    }
                                }
                            }
                        }
                        ShardSyncStatus::StateDownloadParts => {
                            for part in shard_state.parts.iter() {
                                let part_id = part.state_part.part_id as usize;
                                if part_id >= shard_sync_download.downloads.len() {
                                    // TODO ???
                                    continue;
                                }
                                if !shard_sync_download.downloads[part_id].done {
                                    match self.client.chain.set_state_part(
                                        shard_id,
                                        hash,
                                        part.clone(),
                                    ) {
                                        Ok(()) => {
                                            new_shard_download.downloads[part_id].done = true;
                                        }
                                        Err(_err) => {
                                            new_shard_download.downloads[part_id].error = true;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            continue;
                        }
                    }

                    shards_to_download.insert(shard_id, new_shard_download);
                }

                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkPartRequest(part_request_msg, peer_id) => {
                let _ =
                    self.client.shards_mgr.process_chunk_part_request(part_request_msg, peer_id);
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkOnePartRequest(part_request_msg, peer_id) => {
                let _ = self.client.shards_mgr.process_chunk_one_part_request(
                    part_request_msg,
                    peer_id,
                    self.client.chain.mut_store(),
                );
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkPart(part_msg) => {
                if let Ok(accepted_blocks) = self.client.process_chunk_part(part_msg) {
                    self.process_accepted_blocks(accepted_blocks);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkOnePart(one_part_msg) => {
                if let Ok(accepted_blocks) = self.client.process_chunk_one_part(one_part_msg) {
                    self.process_accepted_blocks(accepted_blocks);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::AnnounceAccount(announce_accounts) => {
                let mut filtered_announce_accounts = Vec::new();

                for announce_account in announce_accounts.into_iter() {
                    match self.check_signature_account_announce(&announce_account) {
                        AccountAnnounceVerificationResult::Invalid(ban_reason) => {
                            return NetworkClientResponses::Ban { ban_reason };
                        }
                        AccountAnnounceVerificationResult::Valid => {
                            filtered_announce_accounts.push(announce_account);
                        }
                        // Filter this account
                        AccountAnnounceVerificationResult::UnknownEpoch => {}
                    }
                }

                NetworkClientResponses::AnnounceAccount(filtered_announce_accounts)
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
        }
    }
}

impl Handler<Status> for ClientActor {
    type Result = Result<StatusResponse, String>;

    fn handle(&mut self, _: Status, _: &mut Context<Self>) -> Self::Result {
        let head = self.client.chain.head().map_err(|err| err.to_string())?;
        let prev_header = self
            .client
            .chain
            .get_block_header(&head.last_block_hash)
            .map_err(|err| err.to_string())?;
        let latest_block_time = prev_header.inner.timestamp.clone();
        let validators = self
            .client
            .runtime_adapter
            .get_epoch_block_producers(&head.epoch_id, &head.last_block_hash)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(account_id, is_slashed)| ValidatorInfo { account_id, is_slashed })
            .collect();
        Ok(StatusResponse {
            version: self.client.config.version.clone(),
            chain_id: self.client.config.chain_id.clone(),
            rpc_addr: self.client.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash.into(),
                latest_block_height: head.height,
                latest_state_root: prev_header.inner.prev_state_root.clone().into(),
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
        })
    }
}

impl ClientActor {
    fn sign_announce_account(&self, epoch_id: &EpochId) -> Result<Signature, ()> {
        if let Some(block_producer) = self.client.block_producer.as_ref() {
            let hash = AnnounceAccount::build_header_hash(
                &block_producer.account_id,
                &self.node_id,
                epoch_id,
            );
            let signature = block_producer.signer.sign(hash.as_ref());
            Ok(signature)
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
        if self.client.block_producer.is_none() {
            // There is no account id associated with this client
            return;
        }
        let block_producer = self.client.block_producer.as_ref().unwrap();

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

        debug!(target: "client", "Check announce account for {}, epoch start height: {}, {:?}", block_producer.account_id, epoch_start_height, self.last_validator_announce_height);

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
        if let Ok(validators) =
            self.client.runtime_adapter.get_epoch_block_producers(&next_epoch_id, &prev_block_hash)
        {
            if validators.iter().any(|(account_id, _)| (account_id == &block_producer.account_id)) {
                debug!(target: "client", "Sending announce account for {}", block_producer.account_id);
                self.last_validator_announce_height = Some(epoch_start_height);
                self.last_validator_announce_time = Some(now);
                let signature = self.sign_announce_account(&next_epoch_id).unwrap();

                self.network_adapter.send(NetworkRequests::AnnounceAccount(AnnounceAccount {
                    account_id: block_producer.account_id.clone(),
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
        if self.client.sync_status != SyncStatus::NoSync {
            return Ok(());
        }
        let head = self.client.chain.head()?;
        let mut latest_known = self.client.chain.mut_store().get_latest_known()?;
        assert!(
            head.height <= latest_known.height,
            format!("Latest known height is invalid {} vs {}", head.height, latest_known.height)
        );
        let epoch_id =
            self.client.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        // Get who is the block producer for the upcoming `latest_known.height + 1` block.
        let next_block_producer_account =
            self.client.runtime_adapter.get_block_producer(&epoch_id, latest_known.height + 1)?;

        let elapsed = (Utc::now() - from_timestamp(latest_known.seen)).to_std().unwrap();
        if self.client.block_producer.as_ref().map(|bp| bp.account_id.clone())
            == Some(next_block_producer_account)
        {
            // Next block producer is this client, try to produce a block if at least min_block_production_delay time passed since last block.
            if elapsed >= self.client.config.min_block_production_delay {
                if let Err(err) = self.produce_block(latest_known.height + 1, elapsed) {
                    // If there is an error, report it and let it retry on the next loop step.
                    error!(target: "client", "Block production failed: {:?}", err);
                }
            }
        } else {
            if elapsed < self.client.config.max_block_wait_delay {
                // Next block producer is not this client, so just go for another loop iteration.
            } else {
                // Upcoming block has not been seen in max block production delay, suggest to skip.
                if !self.client.config.produce_empty_blocks {
                    // If we are not producing empty blocks, we always wait for a block to be produced.
                    // Used exclusively for testing.
                    return Ok(());
                }
                debug!(target: "client", "{:?} Timeout for {}, current head {}, suggesting to skip", self.client.block_producer.as_ref().map(|bp| bp.account_id.clone()), latest_known.height, head.height);
                latest_known.height += 1;
                latest_known.seen = to_timestamp(Utc::now());
                self.client.chain.mut_store().save_latest_known(latest_known)?;
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

    /// Produce block if we are block producer for given `next_height` index.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block(
        &mut self,
        next_height: BlockIndex,
        elapsed_since_last_block: Duration,
    ) -> Result<(), Error> {
        match self.client.produce_block(next_height, elapsed_since_last_block) {
            Ok(Some(block)) => {
                let res = self.process_block(block, Provenance::PRODUCED);
                if res.is_err() {
                    error!(target: "client", "Failed to process freshly produced block: {:?}", res);
                    byzantine_assert!(false);
                }
                res.map_err(|err| err.into())
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Process all blocks that were accepted by calling other relevant services.
    fn process_accepted_blocks(&mut self, accepted_blocks: Vec<AcceptedBlock>) {
        for accepted_block in accepted_blocks.into_iter() {
            self.client.on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );

            self.info_helper.block_processed(accepted_block.gas_used, accepted_block.gas_limit);
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
        if provenance == Provenance::PRODUCED {
            self.network_adapter.send(NetworkRequests::Block { block: block.clone() });
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
        debug!(target: "client", "{:?} Received block {} <- {} at {} from {}", self.client.block_producer.as_ref().map(|bp| bp.account_id.clone()), hash, block.header.inner.prev_hash, block.header.inner.height, peer_id);
        let prev_hash = block.header.inner.prev_hash;
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
                    info!(target: "client", "Error on receival of block: {}", err);
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
                        "Chunks were missing for block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                        hash.clone(),
                        self.client.block_producer.as_ref().map(|bp| bp.account_id.clone()),
                        missing_chunks.clone(),
                        missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                    );
                    self.client.shards_mgr.request_chunks(missing_chunks, false).unwrap();
                    NetworkClientResponses::NoResponse
                }
                _ => {
                    debug!("Process block: block {} refused by chain: {}", hash, e.kind());
                    NetworkClientResponses::NoResponse
                }
            },
        }
    }

    fn receive_header(&mut self, header: BlockHeader, peer_info: PeerId) -> NetworkClientResponses {
        let hash = header.hash();
        debug!(target: "client", "Received block header {} at {} from {}", hash, header.inner.height, peer_info);

        // Process block by chain, if it's valid header ask for the block.
        let result = self.client.process_block_header(&header);

        match result {
            Err(ref e) if e.kind() == near_chain::ErrorKind::EpochOutOfBounds => {
                // Block header is either invalid or arrived too early. We ignore it.
                return NetworkClientResponses::NoResponse;
            }
            Err(ref e) if e.is_bad_data() => {
                return NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockHeader }
            }
            // Some error that worth surfacing.
            Err(ref e) if e.is_error() => {
                error!(target: "client", "Error on receival of header: {}", e);
                return NetworkClientResponses::NoResponse;
            }
            // Got an error when trying to process the block header, but it's not due to
            // invalid data or underlying error. Surface as fine.
            Err(_) => return NetworkClientResponses::NoResponse,
            _ => {}
        }

        // Succesfully processed a block header and can request the full block.
        self.request_block_by_hash(header.hash(), peer_info);
        NetworkClientResponses::NoResponse
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
            Ok(false) => self.network_adapter.send(NetworkRequests::BlockRequest { hash, peer_id }),
            Ok(true) => {
                debug!(target: "client", "send_block_request_to_peer: block {} already known", hash)
            }
            Err(e) => {
                error!(target: "client", "send_block_request_to_peer: failed to check block exists: {:?}", e)
            }
        }
    }

    fn retrieve_headers(
        &mut self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<BlockHeader>, near_chain::Error> {
        let header = match self.client.chain.find_common_header(&hashes) {
            Some(header) => header,
            None => return Ok(vec![]),
        };

        let mut headers = vec![];
        let max_height = self.client.chain.header_head()?.height;
        // TODO: this may be inefficient if there are a lot of skipped blocks.
        for h in header.inner.height + 1..=max_height {
            if let Ok(header) = self.client.chain.get_header_by_height(h) {
                headers.push(header.clone());
                if headers.len() >= sync::MAX_BLOCK_HEADERS as usize {
                    break;
                }
            }
        }
        Ok(headers)
    }

    /// Check whether need to (continue) sync.
    fn needs_syncing(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.client.chain.head()?;
        let mut is_syncing = self.client.sync_status.is_syncing();

        let full_peer_info =
            if let Some(full_peer_info) = most_weight_peer(&self.network_info.most_weight_peers) {
                full_peer_info
            } else {
                if !self.client.config.skip_sync_wait {
                    warn!(target: "client", "Sync: no peers available, disabling sync");
                }
                return Ok((false, 0));
            };

        if is_syncing {
            if full_peer_info.chain_info.total_weight <= head.total_weight {
                info!(target: "client", "Sync: synced at {} @ {} [{}]", head.total_weight.to_num(), head.height, head.last_block_hash);
                is_syncing = false;
            }
        } else {
            if full_peer_info.chain_info.total_weight.to_num()
                > head.total_weight.to_num() + self.client.config.sync_weight_threshold
                && full_peer_info.chain_info.height
                    > head.height + self.client.config.sync_height_threshold
            {
                info!(
                    target: "client",
                    "Sync: height/weight: {}/{}, peer height/weight: {}/{}, enabling sync",
                    head.height,
                    head.total_weight,
                    full_peer_info.chain_info.height,
                    full_peer_info.chain_info.total_weight
                );
                is_syncing = true;
            }
        }
        Ok((is_syncing, full_peer_info.chain_info.height))
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
        // Start main sync loop.
        self.sync(ctx);
    }

    fn find_sync_hash(&mut self) -> Result<CryptoHash, near_chain::Error> {
        let header_head = self.client.chain.header_head()?;
        let mut sync_hash = header_head.prev_block_hash;
        for _ in 0..self.client.config.state_fetch_horizon {
            sync_hash = self.client.chain.get_block_header(&sync_hash)?.inner.prev_hash;
        }
        Ok(sync_hash)
    }

    /// Runs catchup on repeat, if this client is a validator.
    fn catchup(&mut self, ctx: &mut Context<ClientActor>) {
        match self.client.run_catchup() {
            Ok(accepted_blocks) => {
                self.process_accepted_blocks(accepted_blocks);
            }
            Err(err) => {
                error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.client.block_producer.as_ref().map(|bp| bp.account_id.clone()), err)
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
        let (needs_syncing, highest_height) = unwrap_or_run_later!(self.needs_syncing());

        if !needs_syncing {
            if currently_syncing {
                debug!(
                    "{:?} moo transitions to no sync",
                    self.client.block_producer.as_ref().map(|x| x.account_id.clone()),
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
                &self.network_info.most_weight_peers
            ));
            // Only body / state sync if header height is close to the latest.
            let header_head = unwrap_or_run_later!(self.client.chain.header_head());

            // Sync state if already running sync state or if block sync is too far.
            let sync_state = match self.client.sync_status {
                SyncStatus::StateSync(_, _) => true,
                _ if highest_height <= self.client.config.block_header_fetch_horizon
                    || header_head.height
                        >= highest_height - self.client.config.block_header_fetch_horizon =>
                {
                    unwrap_or_run_later!(self.client.block_sync.run(
                        &mut self.client.sync_status,
                        &mut self.client.chain,
                        highest_height,
                        &self.network_info.most_weight_peers
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

                let me = &self.client.block_producer.as_ref().map(|x| x.account_id.clone());
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
                match unwrap_or_run_later!(self.client.state_sync.run(
                    sync_hash,
                    &mut new_shard_sync,
                    &mut self.client.chain,
                    &self.client.runtime_adapter,
                    shards_to_sync
                )) {
                    StateSyncResult::Unchanged => (),
                    StateSyncResult::Changed(fetch_block) => {
                        self.client.sync_status = SyncStatus::StateSync(sync_hash, new_shard_sync);
                        if let Some(peer_info) =
                            most_weight_peer(&self.network_info.most_weight_peers)
                        {
                            if fetch_block {
                                if let Ok(header) = self.client.chain.get_block_header(&sync_hash) {
                                    let prev_hash = header.inner.prev_hash;
                                    self.request_block_by_hash(prev_hash, peer_info.peer_info.id);
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
                            me,
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
                            self.client.shards_mgr.request_chunks(missing_chunks, false).unwrap();
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

    /// Periodically fetch network info.
    fn fetch_network_info(&mut self, ctx: &mut Context<Self>) {
        // TODO: replace with push from network?
        self.network_actor
            .send(NetworkRequests::FetchInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkResponses::Info(network_info)) => {
                    act.network_info = network_info;
                    actix::fut::ok(())
                }
                Err(_)
                | Ok(NetworkResponses::BanPeer(_))
                | Ok(NetworkResponses::RoutingTableInfo(_))
                | Ok(NetworkResponses::PingPongInfo { .. })
                | Ok(NetworkResponses::NoResponse)
                | Ok(NetworkResponses::EdgeUpdate(_)) => {
                    error!(target: "client", "Sync: received error or incorrect result.");
                    actix::fut::err(())
                }
            })
            .wait(ctx);

        ctx.run_later(self.client.config.fetch_info_period, move |act, ctx| {
            act.fetch_network_info(ctx);
        });
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        ctx.run_later(self.client.config.log_summary_period, move |act, ctx| {
            let head = unwrap_or_return!(act.client.chain.head());
            let validators = unwrap_or_return!(act
                .client
                .runtime_adapter
                .get_epoch_block_producers(&head.epoch_id, &head.last_block_hash));
            let num_validators = validators.len();
            let is_validator = if let Some(block_producer) = &act.client.block_producer {
                if let Some((_, is_slashed)) =
                    validators.into_iter().find(|x| x.0 == block_producer.account_id)
                {
                    !is_slashed
                } else {
                    false
                }
            } else {
                false
            };

            act.info_helper.info(
                &head,
                &act.client.sync_status,
                &act.node_id,
                &act.network_info,
                is_validator,
                num_validators,
            );

            act.log_summary(ctx);
        });
    }
}
