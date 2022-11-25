use near_chain::{near_chain_primitives, Error};
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

use near_chain::{Chain, RuntimeAdapter};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_primitives::hash::CryptoHash;
use near_primitives::syncing::get_num_state_parts;
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

    state_sync_time: HashMap<ShardId, DateTime<Utc>>,
    last_time_block_requested: Option<DateTime<Utc>>,

    last_part_id_requested: HashMap<(AccountOrPeerIdOrHash, ShardId), PendingRequestStatus>,
    /// Map from which part we requested to whom.
    requested_target: lru::LruCache<(u64, CryptoHash), AccountOrPeerIdOrHash>,

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
            state_sync_time: Default::default(),
            last_time_block_requested: None,
            last_part_id_requested: Default::default(),
            requested_target: lru::LruCache::new(MAX_PENDING_PART as usize),
            timeout: Duration::from_std(timeout).unwrap(),
            state_parts_apply_results: HashMap::new(),
            split_state_roots: HashMap::new(),
        }
    }

    pub fn sync_block_status(
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
    pub fn sync_shards_status(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        highest_height_peers: &[HighestHeightPeerInfo],
        tracking_shards: Vec<ShardId>,
        now: DateTime<Utc>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<(bool, bool), near_chain::Error> {
        let mut all_done = true;
        let mut update_sync_status = false;
        let init_sync_download = ShardSyncDownload {
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
                1
            ],
            status: ShardSyncStatus::StateDownloadHeader,
        };

        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let prev_epoch_id = chain.get_block_header(&prev_hash)?.epoch_id().clone();
        let epoch_id = chain.get_block_header(&sync_hash)?.epoch_id().clone();
        if chain.runtime_adapter.get_shard_layout(&prev_epoch_id)?
            != chain.runtime_adapter.get_shard_layout(&epoch_id)?
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
                init_sync_download.clone()
            });
            let old_status = shard_sync_download.status.clone();
            let mut this_done = false;
            match &shard_sync_download.status {
                ShardSyncStatus::StateDownloadHeader => {
                    if shard_sync_download.downloads[0].done {
                        let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                        let state_num_parts =
                            get_num_state_parts(shard_state_header.state_root_node().memory_usage);
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
                    let mut parts_done = true;
                    for part_download in shard_sync_download.downloads.iter_mut() {
                        if !part_download.done {
                            parts_done = false;
                            let prev = part_download.prev_update_time;
                            let error = part_download.error;
                            let part_timeout = now - prev > self.timeout;
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
                            *shard_sync_download = init_sync_download.clone();
                            chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadApplying => {
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
                                *shard_sync_download = init_sync_download.clone();
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
                    if split_states {
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateSplitScheduling,
                        }
                    } else {
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

    pub fn set_apply_result(&mut self, shard_id: ShardId, apply_result: Result<(), Error>) {
        self.state_parts_apply_results.insert(shard_id, apply_result);
    }

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
        self.requested_target.put((part_id, sync_hash), target.clone());

        let timeout = self.timeout;
        self.last_part_id_requested
            .entry((target, shard_id))
            .and_modify(|pending_request| {
                pending_request.missing_parts += 1;
            })
            .or_insert_with(|| PendingRequestStatus::new(timeout));
    }

    pub fn received_requested_part(
        &mut self,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        let key = (part_id, sync_hash);
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
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
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
                    if me.as_ref().map(|me| me != account_id).unwrap_or(true) {
                        Some(AccountOrPeerIdOrHash::AccountId(account_id.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .chain(highest_height_peers.iter().filter_map(|peer| {
                if peer.tracked_shards.contains(&shard_id) {
                    Some(AccountOrPeerIdOrHash::PeerId(peer.peer_info.id.clone()))
                } else {
                    None
                }
            }))
            .filter(|candidate| {
                !self.last_part_id_requested.contains_key(&(candidate.clone(), shard_id))
            })
            .collect::<Vec<_>>())
    }

    /// Returns new ShardSyncDownload if successful, otherwise returns given shard_sync_download
    pub fn request_shard(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
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
                let possible_targets_sampler =
                    SamplerLimited::new(possible_targets, MAX_STATE_PART_REQUEST);

                // Iterate over all parts that needs to be requested (i.e. download.run_me is true).
                // Parts are ordered such that its index match its part_id.
                // Finally, for every part that needs to be requested it is selected one peer (target) randomly
                // to request the part from
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

    pub fn run(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        highest_height_peers: &[HighestHeightPeerInfo],
        tracking_shards: Vec<ShardId>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
    ) -> Result<StateSyncResult, near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "StateSync").entered();
        debug!(target: "sync", %sync_hash, ?tracking_shards, "syncing state");
        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let now = Clock::utc();

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
            self.state_sync_time.clear();
            return Ok(StateSyncResult::Completed);
        }

        Ok(if update_sync_status || request_block {
            StateSyncResult::Changed(request_block)
        } else {
            StateSyncResult::Unchanged
        })
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
