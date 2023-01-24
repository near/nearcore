//! State sync is trying to fetch the 'full state' from the peers (which can be multiple GB).
//! It happens after HeaderSync and before Body Sync (but only if the node sees that it is 'too much behind').
//! See https://near.github.io/nearcore/architecture/how/sync.html for more detailed information.
//! Such state can be downloaded only at special heights (currently - at the beginning of the current and previous
//! epochs).
//!
//! You can do the state sync for each shard independently.
//! It starts by fetching a 'header' - that contains basic information about the state (for example its size, how
//! many parts it consists of, hash of the root etc).
//! Then it tries downloading the rest of the data in 'parts' (usually the part is around 1MB in size).
//!
//! For downloading - the code is picking the potential target nodes (all direct peers that are tracking the shard
//! (and are high enough) + validators from that epoch that were tracking the shard)
//! Then for each part that we're missing, we're 'randomly' picking a target from whom we'll request it - but we make
//! sure to not request more than MAX_STATE_PART_REQUESTS from each.
//!
//! WARNING: with the current design, we're putting quite a load on the validators - as we request a lot of data from
//!         them (if you assume that we have 100 validators and 30 peers - we send 100/130 of requests to validators).
//!         Currently validators defend against it, by having a rate limiters - but we should improve the algorithm
//!         here to depend more on local peers instead.
//!

use near_chain::{near_chain_primitives, Error};
use near_primitives::state_part::PartId;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as TimeDuration;

use ansi_term::Color::{Purple, Yellow};
use chrono::{DateTime, Duration};
use futures::{future, FutureExt};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use tracing::{debug, error, info, warn};

use near_chain::{Chain, RuntimeWithEpochManagerAdapter};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_primitives::hash::CryptoHash;
use near_primitives::syncing::{get_num_state_parts, ShardStateSyncResponse};
use near_primitives::time::{Clock, Utc};
use near_primitives::types::{AccountId, ShardId, StateRoot};

use near_chain::chain::{ApplyStatePartsRequest, StateSplitRequest};
use near_client_primitives::types::{
    DownloadStatus, ShardSyncDownload, ShardSyncStatus, StateSplitApplyingStatus,
};
use near_network::types::AccountOrPeerIdOrHash;
use near_network::types::PeerManagerMessageRequest;
use near_o11y::WithSpanContextExt;
use near_primitives::shard_layout::ShardUId;

/// Maximum number of state parts to request per peer on each round when node is trying to download the state.
pub const MAX_STATE_PART_REQUEST: u64 = 16;
/// Number of state parts already requested stored as pending.
/// This number should not exceed MAX_STATE_PART_REQUEST times (number of peers in the network).
pub const MAX_PENDING_PART: u64 = MAX_STATE_PART_REQUEST * 10000;

pub enum StateSyncResult {
    /// No shard has changed its status
    Unchanged,
    /// At least one shard has changed its status
    /// Boolean parameter specifies whether the client needs to start fetching the block
    Changed(bool),
    /// The state for all shards was downloaded.
    Completed,
}

struct PendingRequestStatus {
    /// Number of parts that are in progress (we requested them from a given peer but didn't get the answer yet).
    missing_parts: usize,
    wait_until: DateTime<Utc>,
}

impl PendingRequestStatus {
    fn new(timeout: Duration) -> Self {
        Self { missing_parts: 1, wait_until: Clock::utc().add(timeout) }
    }
    fn expired(&self) -> bool {
        Clock::utc() > self.wait_until
    }
}

/// Private to public API conversion.
fn make_account_or_peer_id_or_hash(
    from: near_network::types::AccountOrPeerIdOrHash,
) -> near_client_primitives::types::AccountOrPeerIdOrHash {
    type From = near_network::types::AccountOrPeerIdOrHash;
    type To = near_client_primitives::types::AccountOrPeerIdOrHash;
    match from {
        From::AccountId(a) => To::AccountId(a),
        From::PeerId(p) => To::PeerId(p),
        From::Hash(h) => To::Hash(h),
    }
}

/// Helper to track state sync.
pub struct StateSync {
    network_adapter: Arc<dyn PeerManagerAdapter>,

    last_time_block_requested: Option<DateTime<Utc>>,

    last_part_id_requested: HashMap<(AccountOrPeerIdOrHash, ShardId), PendingRequestStatus>,
    /// Map from which part we requested to whom.
    requested_target: lru::LruCache<(u64, CryptoHash), AccountOrPeerIdOrHash>,

    /// Timeout (set in config - by default to 60 seconds) is used to figure out how long we should wait
    /// for the answer from the other node before giving up.
    timeout: Duration,

    /// Maps shard_id to result of applying downloaded state
    state_parts_apply_results: HashMap<ShardId, Result<(), near_chain_primitives::error::Error>>,

    /// Maps shard_id to result of splitting state for resharding
    split_state_roots: HashMap<ShardId, Result<HashMap<ShardUId, StateRoot>, Error>>,
}

impl StateSync {
    pub fn new(network_adapter: Arc<dyn PeerManagerAdapter>, timeout: TimeDuration) -> Self {
        StateSync {
            network_adapter,
            last_time_block_requested: None,
            last_part_id_requested: Default::default(),
            requested_target: lru::LruCache::new(MAX_PENDING_PART as usize),
            timeout: Duration::from_std(timeout).unwrap(),
            state_parts_apply_results: HashMap::new(),
            split_state_roots: HashMap::new(),
        }
    }

    fn sync_block_status(
        &mut self,
        prev_hash: &CryptoHash,
        chain: &Chain,
        now: DateTime<Utc>,
    ) -> Result<(bool, bool), near_chain::Error> {
        let (request_block, have_block) = if !chain.block_exists(prev_hash)? {
            match self.last_time_block_requested {
                None => (true, false),
                Some(last_time) => {
                    if now - last_time >= self.timeout {
                        error!(target: "sync", "State sync: block request for {} timed out in {} seconds", prev_hash, self.timeout.num_seconds());
                        (true, false)
                    } else {
                        (false, false)
                    }
                }
            }
        } else {
            self.last_time_block_requested = None;
            (false, true)
        };
        if request_block {
            self.last_time_block_requested = Some(now);
        };
        Ok((request_block, have_block))
    }

    // In the tuple of bools returned by this function, the first one
    // indicates whether something has changed in `new_shard_sync`,
    // and therefore whether the client needs to update its
    // `sync_status`. The second indicates whether state sync is
    // finished, in which case the client will transition to block sync
    fn sync_shards_status(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeWithEpochManagerAdapter>,
        highest_height_peers: &[HighestHeightPeerInfo],
        tracking_shards: Vec<ShardId>,
        now: DateTime<Utc>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<(bool, bool), near_chain::Error> {
        let mut all_done = true;
        let mut update_sync_status = false;

        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let prev_epoch_id = chain.get_block_header(&prev_hash)?.epoch_id().clone();
        let epoch_id = chain.get_block_header(&sync_hash)?.epoch_id().clone();
        if runtime_adapter.get_shard_layout(&prev_epoch_id)?
            != runtime_adapter.get_shard_layout(&epoch_id)?
        {
            error!("cannot sync to the first epoch after sharding upgrade");
            panic!("cannot sync to the first epoch after sharding upgrade. Please wait for the next epoch or find peers that are more up to date");
        }
        let split_states = runtime_adapter.will_shard_layout_change_next_epoch(&prev_hash)?;

        for shard_id in tracking_shards {
            let mut download_timeout = false;
            let mut need_shard = false;
            let shard_sync_download = new_shard_sync.entry(shard_id).or_insert_with(|| {
                need_shard = true;
                update_sync_status = true;
                ShardSyncDownload::new(now)
            });

            let old_status = shard_sync_download.status.clone();
            let mut this_done = false;
            match &shard_sync_download.status {
                ShardSyncStatus::StateDownloadHeader => {
                    // StateDownloadHeader is the first step. We want to fetch the basic information about the state (its size, hash etc).
                    if shard_sync_download.downloads[0].done {
                        let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                        let state_num_parts =
                            get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                        // If the header was downloaded succesfully - move to phase 2 (downloading parts).
                        // Create the vector with entry for each part.
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![
                                DownloadStatus {
                                    start_time: now,
                                    prev_update_time: now,
                                    run_me: Arc::new(AtomicBool::new(true)),
                                    error: false,
                                    done: false,
                                    state_requests_count: 0,
                                    last_target: None,
                                };
                                state_num_parts as usize
                            ],
                            status: ShardSyncStatus::StateDownloadParts,
                        };
                        need_shard = true;
                    } else {
                        let prev = shard_sync_download.downloads[0].prev_update_time;
                        let error = shard_sync_download.downloads[0].error;
                        download_timeout = now - prev > self.timeout;
                        // Retry in case of timeout or failure.
                        if download_timeout || error {
                            shard_sync_download.downloads[0].run_me.store(true, Ordering::SeqCst);
                            shard_sync_download.downloads[0].error = false;
                            shard_sync_download.downloads[0].prev_update_time = now;
                        }
                        if shard_sync_download.downloads[0].run_me.load(Ordering::SeqCst) {
                            need_shard = true;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadParts => {
                    // Step 2 - download all the parts (each part is usually around 1MB).
                    let mut parts_done = true;
                    for part_download in shard_sync_download.downloads.iter_mut() {
                        if !part_download.done {
                            parts_done = false;
                            let prev = part_download.prev_update_time;
                            let error = part_download.error;
                            let part_timeout = now - prev > self.timeout;
                            // Retry parts that failed.
                            if part_timeout || error {
                                download_timeout |= part_timeout;
                                part_download.run_me.store(true, Ordering::SeqCst);
                                part_download.error = false;
                                part_download.prev_update_time = now;
                            }
                            if part_download.run_me.load(Ordering::SeqCst) {
                                need_shard = true;
                            }
                        }
                    }
                    // If all parts are done - we can move towards scheduling.
                    if parts_done {
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateDownloadScheduling,
                        };
                    }
                }
                ShardSyncStatus::StateDownloadScheduling => {
                    let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                    let state_num_parts =
                        get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                    // Now apply all the parts to the chain / runtime.
                    // TODO: not sure why this has to happen only after all the parts were downloaded -
                    //       as we could have done this in parallel after getting each part.
                    match chain.schedule_apply_state_parts(
                        shard_id,
                        sync_hash,
                        state_num_parts,
                        state_parts_task_scheduler,
                    ) {
                        Ok(()) => {
                            *shard_sync_download = ShardSyncDownload {
                                downloads: vec![],
                                status: ShardSyncStatus::StateDownloadApplying,
                            }
                        }
                        Err(e) => {
                            // Cannot finalize the downloaded state.
                            // The reasonable behavior here is to start from the very beginning.
                            error!(target: "sync", "State sync finalizing error, shard = {}, hash = {}: {:?}", shard_id, sync_hash, e);
                            *shard_sync_download = ShardSyncDownload::new(now);
                            chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadApplying => {
                    // Keep waiting until our shard is on the list of results
                    // (these are set via callback from ClientActor - both for sync and catchup).
                    let result = self.state_parts_apply_results.remove(&shard_id);
                    if let Some(result) = result {
                        match chain.set_state_finalize(shard_id, sync_hash, result) {
                            Ok(()) => {
                                *shard_sync_download = ShardSyncDownload {
                                    downloads: vec![],
                                    status: ShardSyncStatus::StateDownloadComplete,
                                }
                            }
                            Err(e) => {
                                // Cannot finalize the downloaded state.
                                // The reasonable behavior here is to start from the very beginning.
                                error!(target: "sync", "State sync finalizing error, shard = {}, hash = {}: {:?}", shard_id, sync_hash, e);
                                *shard_sync_download = ShardSyncDownload::new(now);
                                let shard_state_header =
                                    chain.get_state_header(shard_id, sync_hash)?;
                                let state_num_parts = get_num_state_parts(
                                    shard_state_header.state_root_node().memory_usage,
                                );
                                chain.clear_downloaded_parts(
                                    shard_id,
                                    sync_hash,
                                    state_num_parts,
                                )?;
                            }
                        }
                    }
                }
                ShardSyncStatus::StateDownloadComplete => {
                    let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                    let state_num_parts =
                        get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                    chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                    // If the shard layout is changing in this epoch - we have to apply it right now.
                    if split_states {
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateSplitScheduling,
                        }
                    } else {
                        // If there is no layout change - we're done.
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateSyncDone,
                        };
                        this_done = true;
                    }
                }
                ShardSyncStatus::StateSplitScheduling => {
                    debug_assert!(split_states);
                    let status = Arc::new(StateSplitApplyingStatus::new());
                    chain.build_state_for_split_shards_preprocessing(
                        &sync_hash,
                        shard_id,
                        state_split_scheduler,
                        status.clone(),
                    )?;
                    debug!(target: "sync", "State sync split scheduled: me {:?}, shard = {}, hash = {}", me, shard_id, sync_hash);
                    *shard_sync_download = ShardSyncDownload {
                        downloads: vec![],
                        status: ShardSyncStatus::StateSplitApplying(status),
                    };
                }
                ShardSyncStatus::StateSplitApplying(_status) => {
                    debug_assert!(split_states);
                    let result = self.split_state_roots.remove(&shard_id);
                    if let Some(state_roots) = result {
                        chain
                            .build_state_for_split_shards_postprocessing(&sync_hash, state_roots)?;
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateSyncDone,
                        };
                        this_done = true;
                    }
                }
                ShardSyncStatus::StateSyncDone => {
                    this_done = true;
                }
            }
            all_done &= this_done;

            if download_timeout {
                warn!(target: "sync", "State sync didn't download the state for shard {} in {} seconds, sending StateRequest again", shard_id, self.timeout.num_seconds());
                info!(target: "sync", "State sync status: me {:?}, sync_hash {}, phase {}",
                      me,
                      sync_hash,
                      match shard_sync_download.status {
                          ShardSyncStatus::StateDownloadHeader => format!("{} requests sent {}, last target {:?}",
                                                                          Purple.bold().paint(format!("HEADER")),
                                                                          shard_sync_download.downloads[0].state_requests_count,
                                                                          shard_sync_download.downloads[0].last_target),
                          ShardSyncStatus::StateDownloadParts => { let mut text = "".to_string();
                              for (i, download) in shard_sync_download.downloads.iter().enumerate() {
                                  text.push_str(&format!("[{}: {}, {}, {:?}] ",
                                                         Yellow.bold().paint(i.to_string()),
                                                         download.done,
                                                         download.state_requests_count,
                                                         download.last_target));
                              }
                              format!("{} [{}: is_done, requests sent, last target] {}",
                                      Purple.bold().paint("PARTS"),
                                      Yellow.bold().paint("part_id"),
                                      text)
                          }
                          _ => unreachable!("timeout cannot happen when all state is downloaded"),
                      },
                );
            }

            // Execute syncing for shard `shard_id`
            if need_shard {
                update_sync_status = true;
                *shard_sync_download = self.request_shard(
                    me,
                    shard_id,
                    chain,
                    runtime_adapter,
                    sync_hash,
                    shard_sync_download.clone(),
                    highest_height_peers,
                )?;
            }
            update_sync_status |= shard_sync_download.status != old_status;
        }

        Ok((update_sync_status, all_done))
    }

    // Called by the client actor, when it finished applying all the downloaded parts.
    pub fn set_apply_result(&mut self, shard_id: ShardId, apply_result: Result<(), Error>) {
        self.state_parts_apply_results.insert(shard_id, apply_result);
    }

    // Called by the client actor, when it finished splitting the state.
    pub fn set_split_result(
        &mut self,
        shard_id: ShardId,
        result: Result<HashMap<ShardUId, StateRoot>, Error>,
    ) {
        self.split_state_roots.insert(shard_id, result);
    }

    /// Find the hash of the first block on the same epoch (and chain) of block with hash `sync_hash`.
    pub fn get_epoch_start_sync_hash(
        chain: &Chain,
        sync_hash: &CryptoHash,
    ) -> Result<CryptoHash, near_chain::Error> {
        let mut header = chain.get_block_header(sync_hash)?;
        let mut epoch_id = header.epoch_id().clone();
        let mut hash = *header.hash();
        let mut prev_hash = *header.prev_hash();
        loop {
            if prev_hash == CryptoHash::default() {
                return Ok(hash);
            }
            header = chain.get_block_header(&prev_hash)?;
            if &epoch_id != header.epoch_id() {
                return Ok(hash);
            }
            epoch_id = header.epoch_id().clone();
            hash = *header.hash();
            prev_hash = *header.prev_hash();
        }
    }

    fn sent_request_part(
        &mut self,
        target: AccountOrPeerIdOrHash,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        // FIXME: something is wrong - the index should have a shard_id too.
        self.requested_target.put((part_id, sync_hash), target.clone());

        let timeout = self.timeout;
        self.last_part_id_requested
            .entry((target, shard_id))
            .and_modify(|pending_request| {
                pending_request.missing_parts += 1;
            })
            .or_insert_with(|| PendingRequestStatus::new(timeout));
    }

    // Function called when our node receives the network response with a part.
    pub fn received_requested_part(
        &mut self,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        let key = (part_id, sync_hash);
        // Check that it came from the target that we requested it from.
        if let Some(target) = self.requested_target.get(&key) {
            if self.last_part_id_requested.get_mut(&(target.clone(), shard_id)).map_or(
                false,
                |request| {
                    request.missing_parts = request.missing_parts.saturating_sub(1);
                    request.missing_parts == 0
                },
            ) {
                self.last_part_id_requested.remove(&(target.clone(), shard_id));
            }
        }
    }

    /// Find possible targets to download state from.
    /// Candidates are validators at current epoch and peers at highest height.
    /// Only select candidates that we have no pending request currently ongoing.
    fn possible_targets(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &Chain,
        runtime_adapter: &Arc<dyn RuntimeWithEpochManagerAdapter>,
        sync_hash: CryptoHash,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<Vec<AccountOrPeerIdOrHash>, Error> {
        // Remove candidates from pending list if request expired due to timeout
        self.last_part_id_requested.retain(|_, request| !request.expired());

        let prev_block_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let epoch_hash = runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash)?;

        Ok(runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_hash, &sync_hash)?
            .iter()
            .filter_map(|(validator_stake, _slashed)| {
                let account_id = validator_stake.account_id();
                if runtime_adapter.cares_about_shard(
                    Some(account_id),
                    &prev_block_hash,
                    shard_id,
                    false,
                ) {
                    // If we are one of the validators (me is not None) - then make sure that we don't try to send request to ourselves.
                    if me.as_ref().map(|me| me != account_id).unwrap_or(true) {
                        Some(AccountOrPeerIdOrHash::AccountId(account_id.clone()))
                    } else {
                        None
                    }
                } else {
                    // This validator doesn't track the shard - ignore.
                    None
                }
            })
            .chain(highest_height_peers.iter().filter_map(|peer| {
                // Select peers that are high enough (if they are syncing themselves, they might not have the data that we want)
                //  and that are tracking the shard.
                // TODO: possible optimization - simply select peers that have height greater than the epoch start that we're asking for.
                if peer.tracked_shards.contains(&shard_id) {
                    Some(AccountOrPeerIdOrHash::PeerId(peer.peer_info.id.clone()))
                } else {
                    None
                }
            }))
            .filter(|candidate| {
                // If we still have a pending request from this node - don't add another one.
                !self.last_part_id_requested.contains_key(&(candidate.clone(), shard_id))
            })
            .collect::<Vec<_>>())
    }

    /// Returns new ShardSyncDownload if successful, otherwise returns given shard_sync_download
    fn request_shard(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &Chain,
        runtime_adapter: &Arc<dyn RuntimeWithEpochManagerAdapter>,
        sync_hash: CryptoHash,
        shard_sync_download: ShardSyncDownload,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<ShardSyncDownload, near_chain::Error> {
        let possible_targets = self.possible_targets(
            me,
            shard_id,
            chain,
            runtime_adapter,
            sync_hash,
            highest_height_peers,
        )?;

        if possible_targets.is_empty() {
            // In most cases it means that all the targets are currently busy (that we have a pending request with them).
            return Ok(shard_sync_download);
        }

        // Downloading strategy starts here
        let mut new_shard_sync_download = shard_sync_download.clone();

        match shard_sync_download.status {
            ShardSyncStatus::StateDownloadHeader => {
                let target = possible_targets.choose(&mut thread_rng()).cloned().unwrap();
                assert!(new_shard_sync_download.downloads[0].run_me.load(Ordering::SeqCst));
                new_shard_sync_download.downloads[0].run_me.store(false, Ordering::SeqCst);
                new_shard_sync_download.downloads[0].state_requests_count += 1;
                new_shard_sync_download.downloads[0].last_target =
                    Some(make_account_or_peer_id_or_hash(target.clone()));
                let run_me = new_shard_sync_download.downloads[0].run_me.clone();
                near_performance_metrics::actix::spawn(
                    std::any::type_name::<Self>(),
                    self.network_adapter
                        .send(
                            PeerManagerMessageRequest::NetworkRequests(
                                NetworkRequests::StateRequestHeader { shard_id, sync_hash, target },
                            )
                            .with_span_context(),
                        )
                        .then(move |result| {
                            if let Ok(NetworkResponses::RouteNotFound) =
                                result.map(|f| f.as_network_response())
                            {
                                // Send a StateRequestHeader on the next iteration
                                run_me.store(true, Ordering::SeqCst);
                            }
                            future::ready(())
                        }),
                );
            }
            ShardSyncStatus::StateDownloadParts => {
                // We'll select all the 'highest' peers + validators as candidates (exluding those that gave us timeout in the past).
                // And for each one of them, we'll ask for up to 16 (MAX_STATE_PART_REQUEST) parts.
                let possible_targets_sampler =
                    SamplerLimited::new(possible_targets, MAX_STATE_PART_REQUEST);

                // Iterate over all parts that needs to be requested (i.e. download.run_me is true).
                // Parts are ordered such that its index match its part_id.
                // Finally, for every part that needs to be requested it is selected one peer (target) randomly
                // to request the part from.
                // IMPORTANT: here we use 'zip' with possible_target_sampler - which is limited. So at any moment we'll not request more than
                // possible_targets.len() * MAX_STATE_PART_REQUEST parts.
                for ((part_id, download), target) in new_shard_sync_download
                    .downloads
                    .iter_mut()
                    .enumerate()
                    .filter(|(_, download)| download.run_me.load(Ordering::SeqCst))
                    .zip(possible_targets_sampler)
                {
                    self.sent_request_part(target.clone(), part_id as u64, shard_id, sync_hash);
                    download.run_me.store(false, Ordering::SeqCst);
                    download.state_requests_count += 1;
                    download.last_target = Some(make_account_or_peer_id_or_hash(target.clone()));
                    let run_me = download.run_me.clone();

                    near_performance_metrics::actix::spawn(
                        std::any::type_name::<Self>(),
                        self.network_adapter
                            .send(
                                PeerManagerMessageRequest::NetworkRequests(
                                    NetworkRequests::StateRequestPart {
                                        shard_id,
                                        sync_hash,
                                        part_id: part_id as u64,
                                        target: target.clone(),
                                    },
                                )
                                .with_span_context(),
                            )
                            .then(move |result| {
                                // TODO: possible optimization - in the current code, even if one of the targets it not present in the network graph
                                //       (so we keep getting RouteNotFound) - we'll still keep trying to assign parts to it.
                                //       Fortunately only once every 60 seconds (timeout value).
                                if let Ok(NetworkResponses::RouteNotFound) =
                                    result.map(|f| f.as_network_response())
                                {
                                    // Send a StateRequestPart on the next iteration
                                    run_me.store(true, Ordering::SeqCst);
                                }
                                future::ready(())
                            }),
                    );
                }
            }
            _ => {}
        }

        Ok(new_shard_sync_download)
    }

    /// The main 'step' function that should be called periodically to check and update the sync process.
    /// The current state/progress information is mostly kept within 'new_shard_sync' object.
    ///
    /// Returns the state of the sync.
    pub fn run(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeWithEpochManagerAdapter>,
        highest_height_peers: &[HighestHeightPeerInfo],
        // Shards to sync.
        tracking_shards: Vec<ShardId>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<StateSyncResult, near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "StateSync").entered();
        debug!(target: "sync", %sync_hash, ?tracking_shards, "syncing state");
        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let now = Clock::utc();

        // FIXME: it checks if the block exists.. but I have no idea why..
        // seems that we don't really use this block in case of catchup - we use it only for state sync.
        // Seems it is related to some bug with block getting orphaned after state sync? but not sure.
        let (request_block, have_block) = self.sync_block_status(&prev_hash, chain, now)?;

        if tracking_shards.is_empty() {
            // This case is possible if a validator cares about the same shards in the new epoch as
            //    in the previous (or about a subset of them), return success right away

            return if !have_block {
                Ok(StateSyncResult::Changed(request_block))
            } else {
                Ok(StateSyncResult::Completed)
            };
        }

        let (update_sync_status, all_done) = self.sync_shards_status(
            me,
            sync_hash,
            new_shard_sync,
            chain,
            runtime_adapter,
            highest_height_peers,
            tracking_shards,
            now,
            state_parts_task_scheduler,
            state_split_scheduler,
        )?;

        if have_block && all_done {
            return Ok(StateSyncResult::Completed);
        }

        Ok(if update_sync_status || request_block {
            StateSyncResult::Changed(request_block)
        } else {
            StateSyncResult::Unchanged
        })
    }

    pub fn update_download_on_state_response_message(
        &mut self,
        shard_sync_download: &mut ShardSyncDownload,
        hash: CryptoHash,
        shard_id: u64,
        state_response: ShardStateSyncResponse,
        chain: &mut Chain,
    ) {
        if let Some(part_id) = state_response.part_id() {
            // Mark that we have received this part (this will update info on pending parts from peers etc).
            self.received_requested_part(part_id, shard_id, hash);
        }
        match shard_sync_download.status {
            ShardSyncStatus::StateDownloadHeader => {
                if let Some(header) = state_response.take_header() {
                    if !shard_sync_download.downloads[0].done {
                        match chain.set_state_header(shard_id, hash, header) {
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
                        return;
                    }
                    if !shard_sync_download.downloads[part_id as usize].done {
                        match chain.set_state_part(
                            shard_id,
                            hash,
                            PartId::new(part_id, num_parts),
                            &data,
                        ) {
                            Ok(()) => {
                                shard_sync_download.downloads[part_id as usize].done = true;
                            }
                            Err(err) => {
                                error!(target: "sync", "State sync set_state_part error, shard = {}, part = {}, hash = {}: {:?}", shard_id, part_id, hash, err);
                                shard_sync_download.downloads[part_id as usize].error = true;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

/// Create an abstract collection of elements to be shuffled.
/// Each element will appear in the shuffled output exactly `limit` times.
/// Use it as an iterator to access the shuffled collection.
///
/// ```rust,ignore
/// let sampler = SamplerLimited::new(vec![1, 2, 3], 2);
///
/// let res = sampler.collect::<Vec<_>>();
///
/// assert!(res.len() == 6);
/// assert!(res.iter().filter(|v| v == 1).count() == 2);
/// assert!(res.iter().filter(|v| v == 2).count() == 2);
/// assert!(res.iter().filter(|v| v == 3).count() == 2);
/// ```
///
/// Out of the 90 possible values of `res` in the code above on of them is:
///
/// ```
/// vec![1, 2, 1, 3, 3, 2];
/// ```
struct SamplerLimited<T> {
    data: Vec<T>,
    limit: Vec<u64>,
}

impl<T> SamplerLimited<T> {
    fn new(data: Vec<T>, limit: u64) -> Self {
        if limit == 0 {
            Self { data: vec![], limit: vec![] }
        } else {
            let len = data.len();
            Self { data, limit: vec![limit; len] }
        }
    }
}

impl<T: Clone> Iterator for SamplerLimited<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit.is_empty() {
            None
        } else {
            let len = self.limit.len();
            let ix = thread_rng().gen_range(0..len);
            self.limit[ix] -= 1;

            if self.limit[ix] == 0 {
                if ix + 1 != len {
                    self.limit[ix] = self.limit[len - 1];
                    self.data.swap(ix, len - 1);
                }

                self.limit.pop();
                self.data.pop()
            } else {
                Some(self.data[ix].clone())
            }
        }
    }
}

#[cfg(test)]
mod test {

    use actix::System;
    use near_actix_test_utils::run_actix;
    use near_chain::{test_utils::process_block_sync, BlockProcessingArtifact, Provenance};

    use near_epoch_manager::EpochManagerAdapter;
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_primitives::{
        syncing::{ShardStateSyncResponseHeader, ShardStateSyncResponseV2},
        test_utils::TestBlockBuilder,
        types::EpochId,
    };

    use near_chain::test_utils;

    use super::*;

    #[test]
    // Start a new state sync - and check that it asks for a header.
    fn test_ask_for_header() {
        let mock_peer_manager = Arc::new(MockPeerManagerAdapter::default());
        let mut state_sync = StateSync::new(mock_peer_manager.clone(), TimeDuration::from_secs(1));
        let mut new_shard_sync = HashMap::new();

        let (mut chain, kv, signer) = test_utils::setup();

        // TODO: lower the epoch length
        for _ in 0..(chain.epoch_length + 1) {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            let block = if kv.is_next_block_epoch_start(prev.hash()).unwrap() {
                TestBlockBuilder::new(&prev, signer.clone())
                    .epoch_id(prev.header().next_epoch_id().clone())
                    .next_epoch_id(EpochId { 0: *prev.hash() })
                    .next_bp_hash(*prev.header().next_bp_hash())
                    .build()
            } else {
                TestBlockBuilder::new(&prev, signer.clone()).build()
            };

            process_block_sync(
                &mut chain,
                &None,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }

        let request_hash = &chain.head().unwrap().last_block_hash;
        let state_sync_header = chain.get_state_response_header(0, *request_hash).unwrap();
        let state_sync_header = match state_sync_header {
            ShardStateSyncResponseHeader::V1(_) => panic!("Invalid header"),
            ShardStateSyncResponseHeader::V2(internal) => internal,
        };

        let apply_parts_fn = move |_: ApplyStatePartsRequest| {};
        let state_split_fn = move |_: StateSplitRequest| {};

        run_actix(async {
            state_sync
                .run(
                    &None,
                    *request_hash,
                    &mut new_shard_sync,
                    &mut chain,
                    &(kv as Arc<dyn RuntimeWithEpochManagerAdapter>),
                    &[],
                    vec![0],
                    &apply_parts_fn,
                    &state_split_fn,
                )
                .unwrap();

            // Wait for the message that is sent to peer manager.
            mock_peer_manager.notify.notified().await;
            let request = mock_peer_manager.pop().unwrap();

            assert_eq!(
                NetworkRequests::StateRequestHeader {
                    shard_id: 0,
                    sync_hash: *request_hash,
                    target: AccountOrPeerIdOrHash::AccountId("test".parse().unwrap())
                },
                request.as_network_requests()
            );

            assert_eq!(1, new_shard_sync.len());
            let download = new_shard_sync.get(&0).unwrap();

            assert_eq!(download.status, ShardSyncStatus::StateDownloadHeader);

            assert_eq!(download.downloads.len(), 1);
            let download_status = &download.downloads[0];

            // 'run me' is false - as we've just executed this peer manager request.
            assert_eq!(download_status.run_me.load(Ordering::SeqCst), false);
            assert_eq!(download_status.error, false);
            assert_eq!(download_status.done, false);
            assert_eq!(download_status.state_requests_count, 1);
            assert_eq!(
                download_status.last_target,
                Some(near_client_primitives::types::AccountOrPeerIdOrHash::AccountId(
                    "test".parse().unwrap()
                ))
            );

            // Now let's simulate header return message.

            let state_response = ShardStateSyncResponse::V2(ShardStateSyncResponseV2 {
                header: Some(state_sync_header),
                part: None,
            });

            state_sync.update_download_on_state_response_message(
                &mut new_shard_sync.get_mut(&0).unwrap(),
                *request_hash,
                0,
                state_response,
                &mut chain,
            );

            let download = new_shard_sync.get(&0).unwrap();
            assert_eq!(download.status, ShardSyncStatus::StateDownloadHeader);
            // Download should be marked as done.
            assert_eq!(download.downloads[0].done, true);

            System::current().stop()
        });
    }
}
