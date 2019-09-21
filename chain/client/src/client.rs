//! Client is responsible for tracking the chain and related pieces of infrastructure.
//! Block production is done in done in this actor as well (at the moment).

use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient,
    WrapFuture,
};
use borsh::BorshSerialize;
use cached::{Cached, SizedCache};
use chrono::{DateTime, Utc};
use futures::Future;
use log::{debug, error, info, warn};

use near_chain::types::{LatestKnown, ReceiptResponse, ValidatorSignatureVerificationResult};
use near_chain::{
    byzantine_assert, Block, BlockApproval, BlockHeader, BlockStatus, Chain, ChainGenesis,
    ChainStoreAccess, Provenance, RuntimeAdapter,
};
use near_chunks::ShardsManager;
use near_crypto::Signature;
use near_network::types::{
    AnnounceAccount, AnnounceAccountRoute, NetworkInfo, PeerId, ReasonForBan, StateResponseInfo,
};
use near_network::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::{check_tx_history, SignedTransaction};
use near_primitives::types::{AccountId, BlockIndex, EpochId, ShardId};
use near_primitives::unwrap_or_return;
use near_primitives::utils::{from_timestamp, to_timestamp};
use near_primitives::views::ValidatorInfo;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::info::InfoHelper;
use crate::sync::{
    most_weight_peer, BlockSync, HeaderSync, StateSync, StateSyncResult, SyncNetworkRecipient,
};
use crate::types::{
    BlockProducer, ClientConfig, Error, ShardSyncStatus, Status, StatusSyncInfo, SyncStatus,
};
use crate::{sync, StatusResponse};

/// Economics config taken from genesis config
struct EconConfig {
    gas_price_adjustment_rate: u8,
}

enum AccountAnnounceVerificationResult {
    Valid,
    UnknownEpoch,
    Invalid(ReasonForBan),
}

pub struct ClientActor {
    config: ClientConfig,
    sync_status: SyncStatus,
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    shards_mgr: ShardsManager,
    /// A mapping from a block for which a state sync is underway for the next epoch, and the object
    /// storing the current status of the state sync
    catchup_state_syncs: HashMap<CryptoHash, (StateSync, HashMap<u64, ShardSyncStatus>)>,
    block_producer: Option<BlockProducer>,
    network_actor: Recipient<NetworkRequests>,
    network_info: NetworkInfo,
    /// Identity that represents this Client at the network level.
    /// It is used as part of the messages that identify this client.
    node_id: PeerId,
    /// Approvals for which we do not have the block yet
    pending_approvals: SizedCache<CryptoHash, HashMap<AccountId, (Signature, PeerId)>>,
    /// Set of approvals for the next block.
    approvals: HashMap<usize, Signature>,
    /// Keeps track of syncing headers.
    header_sync: HeaderSync,
    /// Keeps track of syncing block.
    block_sync: BlockSync,
    /// Keeps track of syncing state.
    state_sync: StateSync,
    /// Last height we announced our accounts as validators.
    last_validator_announce_height: Option<BlockIndex>,
    /// Last time we announced our accounts as validators.
    last_validator_announce_time: Option<Instant>,
    /// Info helper.
    info_helper: InfoHelper,
    /// economics constants
    econ_config: EconConfig,
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
        let chain = Chain::new(store.clone(), runtime_adapter.clone(), &chain_genesis)?;
        let shards_mgr = ShardsManager::new(
            block_producer.as_ref().map(|x| x.account_id.clone()),
            runtime_adapter.clone(),
            network_actor.clone(),
            store.clone(),
        );
        let sync_status = SyncStatus::AwaitingPeers;
        let header_sync = HeaderSync::new(SyncNetworkRecipient::new(network_actor.clone()));
        let block_sync = BlockSync::new(
            SyncNetworkRecipient::new(network_actor.clone()),
            config.block_fetch_horizon,
        );
        let state_sync = StateSync::new(SyncNetworkRecipient::new(network_actor.clone()));
        if let Some(bp) = &block_producer {
            info!(target: "client", "Starting validator node: {}", bp.account_id);
        }

        let info_helper = InfoHelper::new(telemetry_actor, block_producer.clone());
        let num_block_producers = config.num_block_producers;

        Ok(ClientActor {
            config,
            sync_status,
            chain,
            runtime_adapter,
            shards_mgr,
            catchup_state_syncs: HashMap::new(),
            network_actor,
            node_id,
            block_producer,
            network_info: NetworkInfo {
                num_active_peers: 0,
                peer_max_count: 0,
                most_weight_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
            },
            pending_approvals: SizedCache::with_size(num_block_producers),
            approvals: HashMap::default(),
            header_sync,
            block_sync,
            state_sync,
            last_validator_announce_height: None,
            last_validator_announce_time: None,
            info_helper,
            econ_config: EconConfig {
                gas_price_adjustment_rate: chain_genesis.gas_price_adjustment_rate,
            },
        })
    }

    fn check_signature_account_announce(
        &self,
        announce_account: &AnnounceAccount,
    ) -> AccountAnnounceVerificationResult {
        // Check header is correct.
        let header_hash = announce_account.header_hash();
        let header = announce_account.header();

        // hash must match announcement hash ...
        if header_hash != header.hash {
            return AccountAnnounceVerificationResult::Invalid(ReasonForBan::InvalidHash);
        }

        // ... and signature should be valid.
        match self.runtime_adapter.verify_validator_signature(
            &announce_account.epoch_id,
            &announce_account.account_id,
            header_hash.as_ref(),
            &header.signature,
        ) {
            ValidatorSignatureVerificationResult::Valid => {}
            ValidatorSignatureVerificationResult::Invalid => {
                return AccountAnnounceVerificationResult::Invalid(ReasonForBan::InvalidSignature);
            }
            ValidatorSignatureVerificationResult::UnknownEpoch => {
                return AccountAnnounceVerificationResult::UnknownEpoch;
            }
        }

        // Check intermediates hops are correct.
        // Skip first element (header)
        match announce_account.route.iter().skip(1).fold(Ok(header_hash), |previous_hash, hop| {
            // Folding function will return None if at least one hop checking fail,
            // otherwise it will return hash from last hop.
            if let Ok(previous_hash) = previous_hash {
                let AnnounceAccountRoute { peer_id, hash: current_hash, signature } = hop;

                let real_current_hash = &hash(
                    [
                        previous_hash.as_ref(),
                        peer_id.try_to_vec().expect("Failed to serialize").as_ref(),
                    ]
                    .concat()
                    .as_slice(),
                );

                if real_current_hash != current_hash {
                    return Err(ReasonForBan::InvalidHash);
                }

                if signature.verify(current_hash.as_ref(), &peer_id.public_key()) {
                    Ok(current_hash.clone())
                } else {
                    return Err(ReasonForBan::InvalidSignature);
                }
            } else {
                previous_hash
            }
        }) {
            Ok(_) => AccountAnnounceVerificationResult::Valid,
            Err(reason_for_ban) => AccountAnnounceVerificationResult::Invalid(reason_for_ban),
        }
    }

    /// Determine if I am a validator in current epoch for specified shard.
    fn active_validator(&self) -> Result<bool, Error> {
        let head = self.chain.head()?;

        let account_id = if let Some(bp) = self.block_producer.as_ref() {
            &bp.account_id
        } else {
            return Ok(false);
        };

        let block_proposers = self
            .runtime_adapter
            .get_epoch_block_producers(&head.epoch_id, &head.last_block_hash)
            .map_err(|e| Error::Other(e.to_string()))?;

        // I am a validator if I am in the assignment for current epoch and I'm not slashed.
        Ok(block_proposers
            .into_iter()
            .find_map(
                |(validator, slashed)| if &validator == account_id { Some(!slashed) } else { None },
            )
            .unwrap_or(false))
    }
}

impl Actor for ClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.start_sync(ctx);

        // Start block production tracking if have block producer info.
        if let Some(_) = self.block_producer {
            self.block_production_tracking(ctx);
        }

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

    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkClientMessages::Transaction(tx) => self.process_tx(tx),
            NetworkClientMessages::BlockHeader(header, peer_id) => {
                self.receive_header(header, peer_id)
            }
            NetworkClientMessages::Block(block, peer_id, was_requested) => {
                if let SyncStatus::StateSync(sync_hash, _) = &mut self.sync_status {
                    if let Ok(header) = self.chain.get_block_header(sync_hash) {
                        if block.hash() == header.inner.prev_hash {
                            if let Err(_) = self.chain.save_block(&block) {
                                error!(target: "client", "Failed to save a block during state sync");
                            }
                            return NetworkClientResponses::NoResponse;
                        }
                    }
                }
                self.receive_block(ctx, block, peer_id, was_requested)
            }
            NetworkClientMessages::BlockRequest(hash) => {
                if let Ok(block) = self.chain.get_block(&hash) {
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
            NetworkClientMessages::GetChainInfo => match self.chain.head() {
                Ok(head) => NetworkClientResponses::ChainInfo {
                    genesis: self.chain.genesis().hash(),
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
                if self.collect_block_approval(&account_id, &hash, &signature, &peer_id) {
                    NetworkClientResponses::NoResponse
                } else {
                    warn!(target: "client", "Banning node for sending invalid block approval: {} {} {}", account_id, hash, signature);
                    NetworkClientResponses::NoResponse

                    // TODO(1259): The originator of this message is not the immediate sender so we should not ban him.
                    // NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlockApproval }
                }
            }
            NetworkClientMessages::StateRequest(shard_id, hash) => {
                if let Ok((prev_chunk_extra, payload, outgoing_receipts, incoming_receipts)) =
                    self.chain.state_request(shard_id, hash)
                {
                    return NetworkClientResponses::StateResponse(StateResponseInfo {
                        shard_id,
                        hash,
                        prev_chunk_extra,
                        payload,
                        outgoing_receipts,
                        incoming_receipts,
                    });
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::StateResponse(StateResponseInfo {
                shard_id,
                hash,
                prev_chunk_extra,
                payload,
                outgoing_receipts,
                incoming_receipts,
            }) => {
                // Populate the hashmaps with shard statuses that might be interested in this state
                //     (naturally, the plural of statuses is statuseses)
                let mut shard_statuseses = vec![];

                // ... It could be that the state was requested by the state sync
                if let SyncStatus::StateSync(sync_hash, shard_statuses) = &mut self.sync_status {
                    if hash == *sync_hash {
                        shard_statuseses.push(shard_statuses);
                    }
                }

                // ... Or one of the catchups
                for (sync_hash, state_sync_info) in self.chain.store().iterate_state_sync_infos() {
                    if hash == sync_hash {
                        assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
                        if let Some((_, shard_statuses)) =
                            self.catchup_state_syncs.get_mut(&sync_hash)
                        {
                            shard_statuseses.push(shard_statuses);
                        }
                        // We should not be requesting the same state twice.
                        break;
                    }
                }

                if !shard_statuseses.is_empty() {
                    match self.chain.set_shard_state(
                        &self.block_producer.as_ref().map(|bp| bp.account_id.clone()),
                        shard_id,
                        hash,
                        prev_chunk_extra,
                        payload,
                        outgoing_receipts,
                        incoming_receipts,
                    ) {
                        Ok(()) => {
                            for shard_statuses in shard_statuseses {
                                shard_statuses.insert(shard_id, ShardSyncStatus::StateDone);
                            }
                        }
                        Err(err) => {
                            for shard_statuses in shard_statuseses {
                                shard_statuses.insert(
                                    shard_id,
                                    ShardSyncStatus::Error(format!(
                                        "Failed to set state for {} @ {}: {}",
                                        shard_id, hash, err
                                    )),
                                );
                            }
                        }
                    }
                }

                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkPartRequest(part_request_msg, peer_id) => {
                let _ = self.shards_mgr.process_chunk_part_request(part_request_msg, peer_id);
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkOnePartRequest(part_request_msg, peer_id) => {
                let _ = self.shards_mgr.process_chunk_one_part_request(part_request_msg, peer_id);
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkPart(part_msg) => {
                if let Ok(Some(block_hash)) = self.shards_mgr.process_chunk_part(part_msg) {
                    self.process_blocks_with_missing_chunks(ctx, block_hash);
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::ChunkOnePart(one_part_msg) => {
                let prev_block_hash = one_part_msg.header.inner.prev_block_hash;
                if let Ok(ret) = self.shards_mgr.process_chunk_one_part(one_part_msg.clone()) {
                    if ret {
                        // If the chunk builds on top of the current head, get all the remaining parts
                        // TODO: if the bp receives the chunk before they receive the block, they will
                        //     not collect the parts currently. It will result in chunk not included
                        //     in the next block.
                        if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                            self.block_producer.as_ref().map(|x| &x.account_id),
                            &prev_block_hash,
                            one_part_msg.shard_id,
                            true,
                        ) && self
                            .chain
                            .head()
                            .map(|head| {
                                head.last_block_hash == one_part_msg.header.inner.prev_block_hash
                            })
                            .unwrap_or(false)
                        {
                            self.shards_mgr.request_chunks(vec![one_part_msg.header]).unwrap();
                        } else {
                            // We are getting here either because we don't care about the shard, or
                            //    because we see the one part before we see the block.
                            // In the latter case we will request parts once the block is received
                        }
                        self.process_blocks_with_missing_chunks(ctx, prev_block_hash);
                    }
                }
                NetworkClientResponses::NoResponse
            }
            NetworkClientMessages::AnnounceAccount(announce_account) => {
                match self.check_signature_account_announce(&announce_account) {
                    AccountAnnounceVerificationResult::Valid => {
                        actix::spawn(
                            self.network_actor
                                .send(NetworkRequests::AnnounceAccount(announce_account))
                                .map_err(|e| error!(target: "client", "{}", e))
                                .map(|_| ()),
                        );
                        NetworkClientResponses::NoResponse
                    }
                    AccountAnnounceVerificationResult::Invalid(ban_reason) => {
                        NetworkClientResponses::Ban { ban_reason }
                    }
                    AccountAnnounceVerificationResult::UnknownEpoch => {
                        NetworkClientResponses::NoResponse
                    }
                }
            }
        }
    }
}

impl Handler<Status> for ClientActor {
    type Result = Result<StatusResponse, String>;

    fn handle(&mut self, _: Status, _: &mut Context<Self>) -> Self::Result {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let prev_header =
            self.chain.get_block_header(&head.last_block_hash).map_err(|err| err.to_string())?;
        let latest_block_time = prev_header.inner.timestamp.clone();
        let validators = self
            .runtime_adapter
            .get_epoch_block_producers(&head.epoch_id, &head.last_block_hash)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(account_id, is_slashed)| ValidatorInfo { account_id, is_slashed })
            .collect();
        Ok(StatusResponse {
            version: self.config.version.clone(),
            chain_id: self.config.chain_id.clone(),
            rpc_addr: self.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash.into(),
                latest_block_height: head.height,
                latest_state_root: prev_header.inner.prev_state_root.clone().into(),
                latest_block_time: from_timestamp(latest_block_time),
                syncing: self.sync_status.is_syncing(),
            },
        })
    }
}

impl ClientActor {
    /// Gets called when block got accepted.
    /// Send updates over network, update tx pool and notify ourselves if it's time to produce next block.
    fn on_block_accepted(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block_hash: CryptoHash,
        status: BlockStatus,
        provenance: Provenance,
    ) {
        let block = match self.chain.get_block(&block_hash) {
            Ok(block) => block.clone(),
            Err(err) => {
                error!(target: "client", "Failed to find block {} that was just accepted: {}", block_hash, err);
                return;
            }
        };

        // Count blocks and gas processed both in SYNC and regular modes.
        self.info_helper.block_processed(block.header.inner.gas_used, block.header.inner.gas_limit);

        // Process orphaned chunk_one_parts
        if self.shards_mgr.process_orphaned_one_parts(block_hash) {
            self.process_blocks_with_missing_chunks(ctx, block_hash);
        }

        if provenance != Provenance::SYNC {
            // If we produced the block, then we want to broadcast it.
            // If received the block from another node then broadcast "header first" to minimise network traffic.
            if provenance == Provenance::PRODUCED {
                let _ = self.network_actor.do_send(NetworkRequests::Block { block: block.clone() });
            } else {
                let approval = self.pending_approvals.cache_get(&block_hash).cloned();
                if let Some(approval) = approval {
                    for (account_id, (sig, peer_id)) in approval {
                        if !self.collect_block_approval(&account_id, &block_hash, &sig, &peer_id) {
                            let _ = self.network_actor.do_send(NetworkRequests::BanPeer {
                                peer_id,
                                ban_reason: ReasonForBan::BadBlockApproval,
                            });
                        }
                    }
                }
                let approval = self.get_block_approval(&block);
                let _ = self.network_actor.do_send(NetworkRequests::BlockHeaderAnnounce {
                    header: block.header.clone(),
                    approval,
                });
            }
        }

        if let Some(bp) = self.block_producer.clone() {
            // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
            // This may be slow and we do not want to delay block propagation.
            match status {
                BlockStatus::Next => {
                    // If this block immediately follows the current tip, remove transactions
                    //    from the txpool
                    self.remove_transactions_for_block(bp.account_id.clone(), &block);
                }
                BlockStatus::Fork => {
                    // If it's a fork, no need to reconcile transactions or produce chunks
                    return;
                }
                BlockStatus::Reorg(prev_head) => {
                    // If a reorg happened, reintroduce transactions from the previous chain and
                    //    remove transactions from the new chain
                    let mut reintroduce_head =
                        self.chain.get_block_header(&prev_head).unwrap().clone();
                    let mut remove_head = block.header.clone();
                    assert_ne!(remove_head.hash(), reintroduce_head.hash());

                    let mut to_remove = vec![];
                    let mut to_reintroduce = vec![];

                    while remove_head.hash() != reintroduce_head.hash() {
                        while remove_head.inner.height > reintroduce_head.inner.height {
                            to_remove.push(remove_head.hash());
                            remove_head = self
                                .chain
                                .get_block_header(&remove_head.inner.prev_hash)
                                .unwrap()
                                .clone();
                        }
                        while reintroduce_head.inner.height > remove_head.inner.height
                            || reintroduce_head.inner.height == remove_head.inner.height
                                && reintroduce_head.hash() != remove_head.hash()
                        {
                            to_reintroduce.push(reintroduce_head.hash());
                            reintroduce_head = self
                                .chain
                                .get_block_header(&reintroduce_head.inner.prev_hash)
                                .unwrap()
                                .clone();
                        }
                    }

                    for to_reintroduce_hash in to_reintroduce {
                        let block = self.chain.get_block(&to_reintroduce_hash).unwrap().clone();
                        self.reintroduce_transactions_for_block(bp.account_id.clone(), &block);
                    }

                    for to_remove_hash in to_remove {
                        let block = self.chain.get_block(&to_remove_hash).unwrap().clone();
                        self.remove_transactions_for_block(bp.account_id.clone(), &block);
                    }
                }
            };

            if provenance != Provenance::SYNC {
                // Produce new chunks
                for shard_id in 0..self.runtime_adapter.num_shards() {
                    let epoch_id = self
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&block.header.hash())
                        .unwrap();
                    let chunk_proposer = self
                        .runtime_adapter
                        .get_chunk_producer(&epoch_id, block.header.inner.height + 1, shard_id)
                        .unwrap();

                    if chunk_proposer == *bp.account_id {
                        if let Err(err) = self.produce_chunk(
                            ctx,
                            block.hash(),
                            &epoch_id,
                            block.chunks[shard_id as usize].clone(),
                            block.header.inner.height + 1,
                            shard_id,
                        ) {
                            error!(target: "client", "Error producing chunk {:?}", err);
                        }
                    }
                }
            }
        }

        self.check_send_announce_account(block.hash());
    }

    fn remove_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.inner.prev_hash,
                    shard_id,
                    true,
                ) {
                    self.shards_mgr.remove_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        &self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions,
                    );
                }
            }
        }
    }

    fn reintroduce_transactions_for_block(&mut self, me: AccountId, block: &Block) {
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if block.header.inner.height == chunk_header.height_included {
                if self.shards_mgr.cares_about_shard_this_or_next_epoch(
                    Some(&me),
                    &block.header.inner.prev_hash,
                    shard_id,
                    false,
                ) {
                    self.shards_mgr.reintroduce_transactions(
                        shard_id,
                        // By now the chunk must be in store, otherwise the block would have been orphaned
                        &self.chain.get_chunk(&chunk_header.chunk_hash()).unwrap().transactions,
                    );
                }
            }
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
        if self.block_producer.is_none() {
            // There is no account id associated with this client
            return;
        }
        let block_producer = self.block_producer.as_ref().unwrap();

        let now = Instant::now();
        // Check that we haven't announced it too recently
        if let Some(last_validator_announce_time) = self.last_validator_announce_time {
            // Don't make announcement if have passed less than half of the time in which other peers
            // should remove our Account Id from their Routing Tables.
            if 2 * (now - last_validator_announce_time) < self.config.ttl_account_id_router {
                return;
            }
        }

        let epoch_start_height =
            unwrap_or_return!(self.runtime_adapter.get_epoch_start_height(&prev_block_hash));

        debug!(target: "client", "Check announce account for {}, epoch start height: {}, {:?}", block_producer.account_id, epoch_start_height, self.last_validator_announce_height);

        if let Some(last_validator_announce_height) = self.last_validator_announce_height {
            if last_validator_announce_height >= epoch_start_height {
                // This announcement was already done!
                return;
            }
        }

        // Announce AccountId if client is becoming a validator soon.
        let next_epoch_id = unwrap_or_return!(self
            .runtime_adapter
            .get_next_epoch_id_from_prev_block(&prev_block_hash));

        // Check client is part of the futures validators
        if let Ok(validators) =
            self.runtime_adapter.get_epoch_block_producers(&next_epoch_id, &prev_block_hash)
        {
            if validators.iter().any(|(account_id, _)| (account_id == &block_producer.account_id)) {
                debug!(target: "client", "Sending announce account for {}", block_producer.account_id);
                self.last_validator_announce_height = Some(epoch_start_height);
                self.last_validator_announce_time = Some(now);
                let (hash, signature) = self.sign_announce_account(&next_epoch_id).unwrap();

                actix::spawn(
                    self.network_actor
                        .send(NetworkRequests::AnnounceAccount(AnnounceAccount::new(
                            block_producer.account_id.clone(),
                            next_epoch_id,
                            self.node_id.clone(),
                            hash,
                            signature,
                        )))
                        .map_err(|e| error!(target: "client", "{:?}", e))
                        .map(|_| ()),
                );
            }
        }
    }

    fn sign_announce_account(&self, epoch_id: &EpochId) -> Result<(CryptoHash, Signature), ()> {
        if let Some(block_producer) = self.block_producer.as_ref() {
            let hash = AnnounceAccount::build_header_hash(
                &block_producer.account_id,
                &self.node_id,
                epoch_id,
            );
            let signature = block_producer.signer.sign(hash.as_ref());
            Ok((hash, signature))
        } else {
            Err(())
        }
    }

    fn get_block_proposer(
        &self,
        epoch_id: &EpochId,
        height: BlockIndex,
    ) -> Result<AccountId, Error> {
        self.runtime_adapter.get_block_producer(epoch_id, height).map_err(|err| err.into())
    }

    fn get_epoch_block_proposers(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, Error> {
        self.runtime_adapter
            .get_epoch_block_producers(epoch_id, last_block_hash)
            .map_err(|err| err.into())
    }

    /// Create approval for given block or return none if not a block producer.
    fn get_block_approval(&mut self, block: &Block) -> Option<BlockApproval> {
        let mut epoch_hash =
            self.runtime_adapter.get_epoch_id_from_prev_block(&block.hash()).ok()?;
        let next_block_producer_account =
            self.get_block_proposer(&epoch_hash, block.header.inner.height + 1);
        if let (Some(block_producer), Ok(next_block_producer_account)) =
            (&self.block_producer, &next_block_producer_account)
        {
            if &block_producer.account_id != next_block_producer_account {
                epoch_hash = block.header.inner.epoch_id.clone();
                if let Ok(validators) =
                    self.runtime_adapter.get_epoch_block_producers(&epoch_hash, &block.hash())
                {
                    if let Some((_, is_slashed)) =
                        validators.into_iter().find(|v| v.0 == block_producer.account_id)
                    {
                        if !is_slashed {
                            return Some(BlockApproval::new(
                                block.hash(),
                                &*block_producer.signer,
                                next_block_producer_account.clone(),
                            ));
                        }
                    }
                }
            }
        }
        None
    }

    /// Retrieves latest height, and checks if must produce next block.
    /// Otherwise wait for block arrival or suggest to skip after timeout.
    fn handle_block_production(&mut self, ctx: &mut Context<Self>) -> Result<(), Error> {
        // If syncing, don't try to produce blocks.
        if self.sync_status != SyncStatus::NoSync {
            return Ok(());
        }
        let head = self.chain.head()?;
        let mut latest_known = self.chain.mut_store().get_latest_known()?;
        assert!(
            head.height <= latest_known.height,
            format!("Latest known height is invalid {} vs {}", head.height, latest_known.height)
        );
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        // Get who is the block producer for the upcoming `latest_known.height + 1` block.
        let next_block_producer_account =
            self.runtime_adapter.get_block_producer(&epoch_id, latest_known.height + 1)?;

        let elapsed = (Utc::now() - from_timestamp(latest_known.seen)).to_std().unwrap();
        if self.block_producer.as_ref().map(|bp| bp.account_id.clone())
            == Some(next_block_producer_account)
        {
            // Next block producer is this client, try to produce a block if at least min_block_production_delay time passed since last block.
            if elapsed >= self.config.min_block_production_delay {
                if let Err(err) = self.produce_block(ctx, latest_known.height + 1, elapsed) {
                    // If there is an error, report it and let it retry on the next loop step.
                    error!(target: "client", "Block production failed: {:?}", err);
                }
            }
        } else {
            if elapsed < self.config.max_block_wait_delay {
                // Next block producer is not this client, so just go for another loop iteration.
            } else {
                // Upcoming block has not been seen in max block production delay, suggest to skip.
                if !self.config.produce_empty_blocks {
                    // If we are not producing empty blocks, we always wait for a block to be produced.
                    // Used exclusively for testing.
                    return Ok(());
                }
                debug!(target: "client", "{:?} Timeout for {}, current head {}, suggesting to skip", self.block_producer.as_ref().map(|bp| bp.account_id.clone()), latest_known.height, head.height);
                latest_known.height += 1;
                latest_known.seen = to_timestamp(Utc::now());
                self.chain.mut_store().save_latest_known(latest_known)?;
            }
        }
        Ok(())
    }

    /// Runs a loop to keep track of the latest known block, produces if it's this validators turn.
    fn block_production_tracking(&mut self, ctx: &mut Context<Self>) {
        match self.handle_block_production(ctx) {
            Ok(()) => {}
            Err(err) => {
                error!(target: "client", "Handle block production failed: {:?}", err);
            }
        }
        let wait = self.config.block_production_tracking_delay;
        ctx.run_later(wait, move |act, ctx| {
            act.block_production_tracking(ctx);
        });
    }

    fn produce_chunk(
        &mut self,
        _ctx: &mut Context<ClientActor>, // TODO: remove?
        prev_block_hash: CryptoHash,
        epoch_id: &EpochId,
        last_header: ShardChunkHeader,
        next_height: BlockIndex,
        shard_id: ShardId,
    ) -> Result<(), Error> {
        let block_producer = self
            .block_producer
            .as_ref()
            .ok_or_else(|| Error::ChunkProducer("Called without block producer info.".to_string()))?
            .clone();

        let chunk_proposer =
            self.runtime_adapter.get_chunk_producer(epoch_id, next_height, shard_id).unwrap();
        if block_producer.account_id != chunk_proposer {
            debug!(target: "client", "Not producing chunk for shard {}: chain at {}, not block producer for next block. Me: {}, proposer: {}", shard_id, next_height, block_producer.account_id, chunk_proposer);
            return Ok(());
        }

        debug!(
            target: "client",
            "Producing chunk at height {} for shard {}, I'm {}",
            next_height,
            shard_id,
            block_producer.account_id
        );

        let chunk_extra = self
            .chain
            .get_latest_chunk_extra(shard_id)
            .map_err(|err| Error::ChunkProducer(format!("No chunk extra available: {}", err)))?
            .clone();

        let transactions: Vec<_> = self
            .shards_mgr
            .prepare_transactions(shard_id, self.config.block_expected_weight)?
            .into_iter()
            .filter(|t| {
                check_tx_history(
                    self.chain.get_block_header(&t.transaction.block_hash).ok(),
                    next_height,
                    self.config.transaction_validity_period,
                )
            })
            .collect();
        let block_header = self.chain.get_block_header(&prev_block_hash)?;
        let transactions_len = transactions.len();
        let filtered_transactions = self.runtime_adapter.filter_transactions(
            next_height,
            block_header.inner.timestamp,
            block_header.inner.gas_price,
            chunk_extra.state_root,
            transactions,
        );
        debug!(
            "Creating a chunk with {} filtered transactions from {} total transactions for shard {}",
            filtered_transactions.len(),
            transactions_len,
            shard_id
        );

        let ReceiptResponse(_, receipts) = self.chain.get_outgoing_receipts_for_shard(
            prev_block_hash,
            shard_id,
            last_header.height_included,
        )?;

        // Receipts proofs root is calculating here
        //
        // For each subset of incoming_receipts_into_shard_i_from_the_current_one
        // we calculate hash here and save it
        // and then hash all of them into a single receipts root
        //
        // We check validity in two ways:
        // 1. someone who cares about shard will download all the receipts
        // and checks that receipts_root equals to all receipts hashed
        // 2. anyone who just asks for one's incoming receipts
        // will receive a piece of incoming receipts only
        // with merkle receipts proofs which can be checked locally
        let receipts_hashes = self.runtime_adapter.build_receipts_hashes(&receipts)?;
        let (receipts_root, _) = merklize(&receipts_hashes);

        let encoded_chunk = self
            .shards_mgr
            .create_encoded_shard_chunk(
                prev_block_hash,
                chunk_extra.state_root,
                next_height,
                shard_id,
                chunk_extra.gas_used,
                chunk_extra.gas_limit,
                chunk_extra.validator_proposals.clone(),
                &filtered_transactions,
                &receipts,
                receipts_root,
                block_producer.signer.clone(),
            )
            .map_err(|_e| {
                Error::ChunkProducer("Can't create encoded chunk, serialization error.".to_string())
            })?;

        debug!(
            target: "client",
            "Produced chunk at height {} for shard {} with {} txs and {} receipts, I'm {}, chunk_hash: {}",
            next_height,
            shard_id,
            filtered_transactions.len(),
            receipts.len(),
            block_producer.account_id,
            encoded_chunk.chunk_hash().0,
        );

        self.shards_mgr.distribute_encoded_chunk(encoded_chunk, receipts);

        Ok(())
    }

    /// Produce block if we are block producer for given `next_height` index.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        next_height: BlockIndex,
        elapsed_since_last_block: Duration,
    ) -> Result<(), Error> {
        let block_producer = self.block_producer.as_ref().ok_or_else(|| {
            Error::BlockProducer("Called without block producer info.".to_string())
        })?;
        let head = self.chain.head()?;
        assert_eq!(
            head.epoch_id,
            self.runtime_adapter.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap()
        );

        // Check that we are were called at the block that we are producer for.
        let next_block_proposer = self.get_block_proposer(
            &self.runtime_adapter.get_epoch_id_from_prev_block(&head.last_block_hash).unwrap(),
            next_height,
        )?;
        if block_producer.account_id != next_block_proposer {
            info!(target: "client", "Produce block: chain at {}, not block producer for next block.", next_height);
            return Ok(());
        }
        let prev = self.chain.get_block_header(&head.last_block_hash)?;
        let prev_hash = head.last_block_hash;
        let prev_prev_hash = prev.inner.prev_hash;

        debug!(target: "client", "{:?} Producing block at height {}", block_producer.account_id, next_height);

        if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            if !self.chain.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)? {
                // Currently state for the chunks we are interested in this epoch
                // are not yet caught up (e.g. still state syncing).
                // We reschedule block production.
                // Alex's comment:
                // The previous block is not caught up for the next epoch relative to the previous
                // block, which is the current epoch for this block, so this block cannot be applied
                // at all yet, block production must to be rescheduled
                debug!(target: "client", "Produce block: prev block is not caught up");
                return Ok(());
            }
        }

        // Wait until we have all approvals or timeouts per max block production delay.
        let validators =
            self.runtime_adapter.get_epoch_block_producers(&head.epoch_id, &prev_hash)?;
        let total_validators = validators.len();
        let prev_same_bp = self.runtime_adapter.get_block_producer(&head.epoch_id, head.height)?
            == block_producer.account_id.clone();
        // If epoch changed, and before there was 2 validators and now there is 1 - prev_same_bp is false, but total validators right now is 1.
        let total_approvals =
            total_validators - min(if prev_same_bp { 1 } else { 2 }, total_validators);
        if head.height > 0
            && self.approvals.len() < total_approvals
            && elapsed_since_last_block < self.config.max_block_production_delay
        {
            // Will retry after a `block_production_tracking_delay`.
            debug!(target: "client", "Produce block: approvals {}, expected: {}", self.approvals.len(), total_approvals);
            return Ok(());
        }

        // If we are not producing empty blocks, skip this and call handle scheduling for the next block.
        let new_chunks = self.shards_mgr.prepare_chunks(prev_hash);

        // If we are producing empty blocks and there are not transactions.
        if !self.config.produce_empty_blocks && new_chunks.is_empty() {
            debug!(target: "client", "Empty blocks, skipping block production");
            return Ok(());
        }

        let prev_block = self.chain.get_block(&head.last_block_hash)?;
        let mut chunks = prev_block.chunks.clone();

        // Collect new chunks.
        for (shard_id, mut chunk_header) in new_chunks {
            chunk_header.height_included = next_height;
            chunks[shard_id as usize] = chunk_header;
        }

        let prev_header = self.chain.get_block_header(&head.last_block_hash)?;

        // This is kept here in case we want to revive txs on the block level.
        let transactions = vec![];

        // At this point, the previous epoch hash must be available
        let epoch_id = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .expect("Epoch hash should exist at this point");

        let inflation = if self.runtime_adapter.is_next_block_epoch_start(&head.last_block_hash)? {
            let next_epoch_id =
                self.runtime_adapter.get_next_epoch_id_from_prev_block(&head.last_block_hash)?;
            Some(self.runtime_adapter.get_epoch_inflation(&next_epoch_id)?)
        } else {
            None
        };

        let block = Block::produce(
            &prev_header,
            next_height,
            chunks,
            epoch_id,
            transactions,
            self.approvals.drain().collect(),
            self.econ_config.gas_price_adjustment_rate,
            inflation,
            block_producer.signer.clone(),
        );

        // Update latest known even before sending block out, to prevent race conditions.
        self.chain.mut_store().save_latest_known(LatestKnown {
            height: next_height,
            seen: to_timestamp(Utc::now()),
        })?;

        let res = self.process_block(ctx, block, Provenance::PRODUCED);
        byzantine_assert!(res.is_ok());
        res.map_err(|err| err.into())
    }

    /// Check if any block with missing chunks is ready to be processed
    fn process_blocks_with_missing_chunks(
        &mut self,
        ctx: &mut Context<ClientActor>,
        last_accepted_block_hash: CryptoHash,
    ) {
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let me =
            self.block_producer.as_ref().map(|block_producer| block_producer.account_id.clone());
        self.chain.check_blocks_with_missing_chunks(&me, last_accepted_block_hash, |block, status, provenance| {
            debug!(target: "client", "Block {} was missing chunks but now is ready to be processed", block.hash());
            accepted_blocks.write().unwrap().push((block.hash(), status, provenance));
        }, |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks));
        for (hash, status, provenance) in accepted_blocks.write().unwrap().drain(..) {
            self.on_block_accepted(ctx, hash, status, provenance);
        }
        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
    }

    /// Process block and execute callbacks.
    fn process_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block: Block,
        provenance: Provenance,
    ) -> Result<(), near_chain::Error> {
        // XXX: this is bad, there is no multithreading here, what is the better way to handle this callback?
        // TODO: replace to channels or cross beams here?
        let accepted_blocks = Arc::new(RwLock::new(vec![]));
        let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));
        let result = {
            let me = self
                .block_producer
                .as_ref()
                .map(|block_producer| block_producer.account_id.clone());
            self.chain.process_block(
                &me,
                block,
                provenance,
                |block, status, provenance| {
                    accepted_blocks.write().unwrap().push((block.hash(), status, provenance));
                },
                |missing_chunks| blocks_missing_chunks.write().unwrap().push(missing_chunks),
            )
        };
        // Process all blocks that were accepted.
        for (hash, status, provenance) in accepted_blocks.write().unwrap().drain(..) {
            self.on_block_accepted(ctx, hash, status, provenance);
        }
        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
            self.shards_mgr.request_chunks(missing_chunks).unwrap();
        }
        result.map(|_| ())
    }

    /// Processes received block, returns boolean if block was reasonable or malicious.
    fn receive_block(
        &mut self,
        ctx: &mut Context<ClientActor>,
        block: Block,
        peer_id: PeerId,
        was_requested: bool,
    ) -> NetworkClientResponses {
        let hash = block.hash();
        debug!(target: "client", "{:?} Received block {} <- {} at {} from {}", self.block_producer.as_ref().map(|bp| bp.account_id.clone()), hash, block.header.inner.prev_hash, block.header.inner.height, peer_id);
        let prev_hash = block.header.inner.prev_hash;
        let provenance =
            if was_requested { near_chain::Provenance::SYNC } else { near_chain::Provenance::NONE };
        match self.process_block(ctx, block, provenance) {
            Ok(_) => NetworkClientResponses::NoResponse,
            Err(ref err) if err.is_bad_data() => {
                NetworkClientResponses::Ban { ban_reason: ReasonForBan::BadBlock }
            }
            Err(ref err) if err.is_error() => {
                if self.sync_status.is_syncing() {
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
                    if !self.chain.is_orphan(&prev_hash) && !self.sync_status.is_syncing() {
                        self.request_block_by_hash(prev_hash, peer_id)
                    }
                    NetworkClientResponses::NoResponse
                }
                near_chain::ErrorKind::ChunksMissing(missing_chunks) => {
                    debug!(
                        "Chunks were missing for block {}, I'm {:?}, requesting. Missing: {:?}, ({:?})",
                        hash.clone(),
                        self.block_producer.as_ref().map(|bp| bp.account_id.clone()),
                        missing_chunks.clone(),
                        missing_chunks.iter().map(|header| header.chunk_hash()).collect::<Vec<_>>()
                    );
                    self.shards_mgr.request_chunks(missing_chunks).unwrap();
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
        let result = self.chain.process_block_header(&header);

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
        match self.chain.sync_block_headers(headers) {
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
        match self.chain.block_exists(&hash) {
            Ok(false) => {
                // TODO: ?? should we add a wait for response here?
                let _ = self.network_actor.do_send(NetworkRequests::BlockRequest { hash, peer_id });
            }
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
        let header = match self.chain.find_common_header(&hashes) {
            Some(header) => header,
            None => return Ok(vec![]),
        };

        let mut headers = vec![];
        let max_height = self.chain.header_head()?.height;
        // TODO: this may be inefficient if there are a lot of skipped blocks.
        for h in header.inner.height + 1..=max_height {
            if let Ok(header) = self.chain.get_header_by_height(h) {
                headers.push(header.clone());
                if headers.len() >= sync::MAX_BLOCK_HEADERS as usize {
                    break;
                }
            }
        }
        Ok(headers)
    }

    /// Validate transaction and return transaction information relevant to ordering it in the mempool.
    fn process_tx(&mut self, tx: SignedTransaction) -> NetworkClientResponses {
        let head = unwrap_or_return!(self.chain.head(), NetworkClientResponses::NoResponse);
        let me = self.block_producer.as_ref().map(|bp| &bp.account_id);
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id);
        if !check_tx_history(
            self.chain.get_block_header(&tx.transaction.block_hash).ok(),
            head.height,
            self.config.transaction_validity_period,
        ) {
            debug!(target: "client", "Invalid tx: expired or from a different fork -- {:?}", tx);
            return NetworkClientResponses::InvalidTx(
                "Transaction has either expired or from a different fork".to_string(),
            );
        }
        let block_header = unwrap_or_return!(
            self.chain.get_block_header(&head.last_block_hash),
            NetworkClientResponses::NoResponse
        );
        let gas_price = block_header.inner.gas_price;
        let timestamp = block_header.inner.timestamp;
        let state_root = unwrap_or_return!(
            self.chain.get_chunk_extra(&head.last_block_hash, shard_id),
            NetworkClientResponses::NoResponse
        )
        .state_root
        .clone();

        match self.runtime_adapter.validate_tx(head.height + 1, timestamp, gas_price, state_root, tx) {
            Ok(valid_transaction) => {
                let active_validator = unwrap_or_return!(self.active_validator(), {
                    warn!(target: "client", "Me: {:?} Dropping tx: {:?}", me, valid_transaction);
                    NetworkClientResponses::NoResponse
                });

                // If I'm not an active validator I should forward tx to next validators.
                if active_validator {
                    debug!(
                        "Recording a transaction. I'm {:?}, {}",
                        self.block_producer.as_ref().map(|bp| bp.account_id.clone()),
                        shard_id
                    );
                    self.shards_mgr.insert_transaction(shard_id, valid_transaction);
                    NetworkClientResponses::ValidTx
                } else {
                    // TODO(MarX): Forward tx even if I am a validator.
                    // TODO(MarX): How many validators ahead of current time should we forward tx?
                    let target_height = head.height + 2;

                    debug!(target: "client",
                           "{:?} Routing a transaction. {}",
                            self.block_producer.as_ref().map(|bp| bp.account_id.clone()),
                            shard_id
                    );

                    let validator = unwrap_or_return!(
                        self.runtime_adapter.get_chunk_producer(
                            &head.epoch_id,
                            target_height,
                            shard_id
                        ),
                        {
                            warn!(target: "client", "Me: {:?} Dropping tx: {:?}", me, valid_transaction);
                            NetworkClientResponses::NoResponse
                        }
                    );

                    NetworkClientResponses::ForwardTx(validator, valid_transaction.transaction)
                }
            }
            Err(err) => {
                debug!(target: "client", "Invalid transaction: {:?}", err);
                NetworkClientResponses::InvalidTx(err.to_string())
            }
        }
    }

    /// Check whether need to (continue) sync.
    fn needs_syncing(&self) -> Result<(bool, u64), near_chain::Error> {
        let head = self.chain.head()?;
        let mut is_syncing = self.sync_status.is_syncing();

        let full_peer_info =
            if let Some(full_peer_info) = most_weight_peer(&self.network_info.most_weight_peers) {
                full_peer_info
            } else {
                if !self.config.skip_sync_wait {
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
                > head.total_weight.to_num() + self.config.sync_weight_threshold
                && full_peer_info.chain_info.height
                    > head.height + self.config.sync_height_threshold
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
        if self.network_info.num_active_peers < self.config.min_num_peers
            && !self.config.skip_sync_wait
        {
            ctx.run_later(self.config.sync_step_period, move |act, ctx| {
                act.start_sync(ctx);
            });
            return;
        }
        // Start main sync loop.
        self.sync(ctx);
    }

    fn find_sync_hash(&mut self) -> Result<CryptoHash, near_chain::Error> {
        let header_head = self.chain.header_head()?;
        let mut sync_hash = header_head.prev_block_hash;
        for _ in 0..self.config.state_fetch_horizon {
            sync_hash = self.chain.get_block_header(&sync_hash)?.inner.prev_hash;
        }
        Ok(sync_hash)
    }

    /// Walks through all the ongoing state syncs for future epochs and processes them
    fn run_catchup(&mut self, ctx: &mut Context<ClientActor>) -> Result<(), Error> {
        let me = &self.block_producer.as_ref().map(|x| x.account_id.clone());
        for (sync_hash, state_sync_info) in self.chain.store().iterate_state_sync_infos() {
            assert_eq!(sync_hash, state_sync_info.epoch_tail_hash);
            let network_actor1 = self.network_actor.clone();

            let (state_sync, new_shard_sync) =
                self.catchup_state_syncs.entry(sync_hash).or_insert_with(|| {
                    (StateSync::new(SyncNetworkRecipient::new(network_actor1)), HashMap::new())
                });

            debug!(
                target: "client",
                "Catchup me: {:?}: sync_hash: {:?}, sync_info: {:?}", me, sync_hash, new_shard_sync
            );

            match state_sync.run(
                sync_hash,
                new_shard_sync,
                &mut self.chain,
                &self.runtime_adapter,
                state_sync_info.shards.iter().map(|tuple| tuple.0).collect(),
            )? {
                StateSyncResult::Unchanged => {}
                StateSyncResult::Changed(fetch_block) => {
                    assert!(!fetch_block);
                }
                StateSyncResult::Completed => {
                    let accepted_blocks = Arc::new(RwLock::new(vec![]));
                    let blocks_missing_chunks = Arc::new(RwLock::new(vec![]));

                    self.chain.catchup_blocks(
                        me,
                        &sync_hash,
                        |block, status, provenance| {
                            accepted_blocks.write().unwrap().push((
                                block.hash(),
                                status,
                                provenance,
                            ));
                        },
                        |missing_chunks| {
                            blocks_missing_chunks.write().unwrap().push(missing_chunks)
                        },
                    )?;

                    for (hash, status, provenance) in accepted_blocks.write().unwrap().drain(..) {
                        self.on_block_accepted(ctx, hash, status, provenance);
                    }
                    for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
                        self.shards_mgr.request_chunks(missing_chunks).unwrap();
                    }
                }
            }
        }

        Ok(())
    }

    /// Runs catchup on repeat, if this client is a validator.
    fn catchup(&mut self, ctx: &mut Context<ClientActor>) {
        if let Some(_) = self.block_producer {
            match self.run_catchup(ctx) {
                Ok(_) => {}
                Err(err) => {
                    error!(target: "client", "{:?} Error occurred during catchup for the next epoch: {:?}", self.block_producer.as_ref().map(|bp| bp.account_id.clone()), err)
                }
            }

            ctx.run_later(self.config.catchup_step_period, move |act, ctx| {
                act.catchup(ctx);
            });
        }
    }

    /// Main syncing job responsible for syncing client with other peers.
    fn sync(&mut self, ctx: &mut Context<ClientActor>) {
        // Macro to schedule to call this function later if error occurred.
        macro_rules! unwrap_or_run_later(($obj: expr) => (match $obj {
            Ok(v) => v,
            Err(err) => {
                error!(target: "sync", "Sync: Unexpected error: {}", err);
                ctx.run_later(self.config.sync_step_period, move |act, ctx| {
                    act.sync(ctx);
                });
                return;
            }
        }));

        let mut wait_period = self.config.sync_step_period;

        let currently_syncing = self.sync_status.is_syncing();
        let (needs_syncing, highest_height) = unwrap_or_run_later!(self.needs_syncing());

        if !needs_syncing {
            if currently_syncing {
                self.sync_status = SyncStatus::NoSync;

                // Initial transition out of "syncing" state.
                // Announce this client's account id if their epoch is coming up.
                let head = unwrap_or_run_later!(self.chain.head());
                self.check_send_announce_account(head.prev_block_hash);
            }
            wait_period = self.config.sync_check_period;
        } else {
            // Run each step of syncing separately.
            unwrap_or_run_later!(self.header_sync.run(
                &mut self.sync_status,
                &mut self.chain,
                highest_height,
                &self.network_info.most_weight_peers
            ));
            // Only body / state sync if header height is close to the latest.
            let header_head = unwrap_or_run_later!(self.chain.header_head());

            // Sync state if already running sync state or if block sync is too far.
            let sync_state = match self.sync_status {
                SyncStatus::StateSync(_, _) => true,
                _ if highest_height <= self.config.block_header_fetch_horizon
                    || header_head.height
                        >= highest_height - self.config.block_header_fetch_horizon =>
                {
                    unwrap_or_run_later!(self.block_sync.run(
                        &mut self.sync_status,
                        &mut self.chain,
                        highest_height,
                        &self.network_info.most_weight_peers
                    ))
                }
                _ => false,
            };
            if sync_state {
                let (sync_hash, mut new_shard_sync) = match &self.sync_status {
                    SyncStatus::StateSync(sync_hash, shard_sync) => {
                        let mut need_to_restart = false;

                        if let Ok(sync_block_header) = self.chain.get_block_header(&sync_hash) {
                            let prev_hash = sync_block_header.inner.prev_hash;

                            if let Ok(current_epoch) =
                                self.runtime_adapter.get_epoch_id_from_prev_block(&prev_hash)
                            {
                                if let Ok(next_epoch) = self
                                    .runtime_adapter
                                    .get_next_epoch_id_from_prev_block(&prev_hash)
                                {
                                    if let Ok(header_head) = self.chain.header_head() {
                                        let header_head_epoch = header_head.epoch_id;

                                        if current_epoch != header_head_epoch
                                            && next_epoch != header_head_epoch
                                        {
                                            error!(target: "client", "Header head is not within two epochs of state sync hash, restarting state sync");
                                            debug!(target: "client", "Current epoch: {:?}, Next epoch: {:?}, Header head epoch: {:?}", current_epoch, next_epoch, header_head_epoch);
                                            need_to_restart = true;
                                        }
                                    }
                                }
                            }
                        }

                        if need_to_restart {
                            (unwrap_or_run_later!(self.find_sync_hash()), HashMap::default())
                        } else {
                            (sync_hash.clone(), shard_sync.clone())
                        }
                    }
                    _ => (unwrap_or_run_later!(self.find_sync_hash()), HashMap::default()),
                };

                let me = &self.block_producer.as_ref().map(|x| x.account_id.clone());
                let shards_to_sync = (0..self.runtime_adapter.num_shards())
                    .filter(|x| {
                        self.shards_mgr.cares_about_shard_this_or_next_epoch(
                            me.as_ref(),
                            &sync_hash,
                            *x,
                            true,
                        )
                    })
                    .collect();
                match unwrap_or_run_later!(self.state_sync.run(
                    sync_hash,
                    &mut new_shard_sync,
                    &mut self.chain,
                    &self.runtime_adapter,
                    shards_to_sync
                )) {
                    StateSyncResult::Unchanged => (),
                    StateSyncResult::Changed(fetch_block) => {
                        self.sync_status = SyncStatus::StateSync(sync_hash, new_shard_sync);
                        if let Some(peer_info) =
                            most_weight_peer(&self.network_info.most_weight_peers)
                        {
                            if fetch_block {
                                if let Ok(header) = self.chain.get_block_header(&sync_hash) {
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

                        unwrap_or_run_later!(self.chain.reset_heads_post_state_sync(
                            me,
                            sync_hash,
                            |block, status, provenance| {
                                accepted_blocks.write().unwrap().push((
                                    block.hash(),
                                    status,
                                    provenance,
                                ));
                            },
                            |missing_chunks| {
                                blocks_missing_chunks.write().unwrap().push(missing_chunks)
                            },
                        ));

                        for (hash, status, provenance) in accepted_blocks.write().unwrap().drain(..)
                        {
                            self.on_block_accepted(ctx, hash, status, provenance);
                        }
                        for missing_chunks in blocks_missing_chunks.write().unwrap().drain(..) {
                            self.shards_mgr.request_chunks(missing_chunks).unwrap();
                        }

                        self.sync_status =
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
                Ok(NetworkResponses::NoResponse) => actix::fut::ok(()),
                Err(e) => {
                    error!(target: "client", "Sync: recieved error or incorrect result: {}", e);
                    actix::fut::err(())
                }
            })
            .wait(ctx);

        ctx.run_later(self.config.fetch_info_period, move |act, ctx| {
            act.fetch_network_info(ctx);
        });
    }

    /// Periodically log summary.
    fn log_summary(&self, ctx: &mut Context<Self>) {
        ctx.run_later(self.config.log_summary_period, move |act, ctx| {
            let head = unwrap_or_return!(act.chain.head());
            let validators = unwrap_or_return!(
                act.get_epoch_block_proposers(&head.epoch_id, &head.last_block_hash)
            );
            let num_validators = validators.len();
            let is_validator = if let Some(block_producer) = &act.block_producer {
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
                &act.sync_status,
                &act.node_id,
                &act.network_info,
                is_validator,
                num_validators,
            );

            act.log_summary(ctx);
        });
    }

    /// Collects block approvals. Returns false if block approval is invalid.
    fn collect_block_approval(
        &mut self,
        account_id: &AccountId,
        hash: &CryptoHash,
        signature: &Signature,
        peer_id: &PeerId,
    ) -> bool {
        // TODO: figure out how to validate better before hitting the disk? For example validator and account cache to validate signature first.
        // TODO: This header is missing, should collect for later? should have better way to verify then.

        let header = match self.chain.get_block_header(&hash) {
            Ok(h) => h.clone(),
            Err(e) => {
                if e.is_bad_data() {
                    return false;
                }
                let mut entry =
                    self.pending_approvals.cache_remove(hash).unwrap_or_else(|| HashMap::new());
                entry.insert(account_id.clone(), (signature.clone(), peer_id.clone()));
                self.pending_approvals.cache_set(*hash, entry);
                return true;
            }
        };

        // TODO: Access runtime adapter only once to find the position and public key.

        // If given account is not current block proposer.
        let position = match self.get_epoch_block_proposers(&header.inner.epoch_id, &hash) {
            Ok(validators) => {
                let position = validators.iter().position(|x| &(x.0) == account_id);
                if let Some(idx) = position {
                    if !validators[idx].1 {
                        idx
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            Err(err) => {
                error!(target: "client", "Block approval error: {}", err);
                return false;
            }
        };
        // Check signature is correct for given validator.
        if let ValidatorSignatureVerificationResult::Invalid =
            self.runtime_adapter.verify_validator_signature(
                &header.inner.epoch_id,
                account_id,
                hash.as_ref(),
                signature,
            )
        {
            return false;
        }
        debug!(target: "client", "Received approval for {} from {}", hash, account_id);
        self.approvals.insert(position, signature.clone());
        true
    }
}
