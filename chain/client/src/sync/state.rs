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

use crate::metrics;
use ansi_term::Color::{Purple, Yellow};
use ansi_term::Style;
use chrono::{DateTime, Duration, Utc};
use futures::{future, FutureExt};
use near_async::messaging::CanSendAsync;
use near_chain::chain::{ApplyStatePartsRequest, StateSplitRequest};
use near_chain::near_chain_primitives;
use near_chain::Chain;
use near_chain_configs::{ExternalStorageConfig, ExternalStorageLocation, SyncConfig};
use near_client_primitives::types::{
    DownloadStatus, ShardSyncDownload, ShardSyncStatus, StateSplitApplyingStatus,
};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::AccountOrPeerIdOrHash;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_part::PartId;
use near_primitives::static_clock::StaticClock;
use near_primitives::syncing::{get_num_state_parts, ShardStateSyncResponse};
use near_primitives::types::{AccountId, EpochHeight, EpochId, ShardId, StateRoot};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration as TimeDuration;
use std::time::Instant;

/// Maximum number of state parts to request per peer on each round when node is trying to download the state.
pub const MAX_STATE_PART_REQUEST: u64 = 16;
/// Number of state parts already requested stored as pending.
/// This number should not exceed MAX_STATE_PART_REQUEST times (number of peers in the network).
pub const MAX_PENDING_PART: u64 = MAX_STATE_PART_REQUEST * 10000;
/// Time limit per state dump iteration.
/// A node must check external storage for parts to dump again once time is up.
pub const STATE_DUMP_ITERATION_TIME_LIMIT_SECS: u64 = 300;

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
        Self { missing_parts: 1, wait_until: StaticClock::utc().add(timeout) }
    }
    fn expired(&self) -> bool {
        StaticClock::utc() > self.wait_until
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

/// How to retrieve the state data.
enum StateSyncInner {
    /// Request both the state header and state parts from the peers.
    Peers {
        /// Which parts were requested from which peer and when.
        last_part_id_requested: HashMap<(AccountOrPeerIdOrHash, ShardId), PendingRequestStatus>,
        /// Map from which part we requested to whom.
        requested_target: lru::LruCache<(u64, CryptoHash), AccountOrPeerIdOrHash>,
    },
    /// Requests the state header from peers but gets the state parts from an
    /// external storage.
    PartsFromExternal {
        /// Chain ID.
        chain_id: String,
        /// The number of requests for state parts from external storage that are
        /// allowed to be started for this shard.
        requests_remaining: Arc<AtomicI32>,
        /// Connection to the external storage.
        external: ExternalConnection,
    },
}

/// Connection to the external storage.
#[derive(Clone)]
pub enum ExternalConnection {
    S3 { bucket: Arc<s3::Bucket> },
    Filesystem { root_dir: PathBuf },
}

impl ExternalConnection {
    async fn get_part(self, shard_id: ShardId, location: &str) -> Result<Vec<u8>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        match self {
            ExternalConnection::S3 { bucket } => {
                let response = bucket.get_object(location).await?;
                tracing::debug!(target: "sync", %shard_id, location, response_code = response.status_code(), num_bytes = response.bytes().len(), "S3 request finished");
                if response.status_code() == 200 {
                    Ok(response.bytes().to_vec())
                } else {
                    Err(anyhow::anyhow!("Bad response status code: {}", response.status_code()))
                }
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(location);
                tracing::debug!(target: "sync", %shard_id, ?path, "Reading a file");
                let data = std::fs::read(&path)?;
                Ok(data)
            }
        }
    }

    /// Uploads the given state part to external storage.
    // Wrapper for adding is_ok to the metric labels.
    pub async fn put_state_part(
        &self,
        state_part: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        let instant = Instant::now();
        let res = self.put_state_part_impl(state_part, shard_id, location).await;
        let is_ok = if res.is_ok() { "ok" } else { "error" };
        let elapsed = instant.elapsed();
        metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string(), is_ok])
            .observe(elapsed.as_secs_f64());
        res
    }

    // Actual implementation.
    async fn put_state_part_impl(
        &self,
        state_part: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                bucket.put_object(&location, state_part).await?;
                tracing::debug!(target: "state_sync_dump", shard_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(location);
                if let Some(parent_dir) = path.parent() {
                    std::fs::create_dir_all(parent_dir)?;
                }
                let mut file = std::fs::OpenOptions::new().write(true).create(true).open(&path)?;
                file.write_all(state_part)?;
                tracing::debug!(target: "state_sync_dump", shard_id, part_length = state_part.len(), ?location, "Wrote a state part to a file");
                Ok(())
            }
        }
    }

    fn extract_file_name_from_full_path(full_path: String) -> String {
        return Self::extract_file_name_from_path_buf(PathBuf::from(full_path));
    }

    fn extract_file_name_from_path_buf(path_buf: PathBuf) -> String {
        return path_buf.file_name().unwrap().to_str().unwrap().to_string();
    }

    pub async fn list_state_parts(
        &self,
        shard_id: ShardId,
        directory_path: &str,
    ) -> Result<Vec<String>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_DUMP_LIST_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        match self {
            ExternalConnection::S3 { bucket } => {
                let prefix = format!("{}/", directory_path);
                let list_results = bucket.list(prefix.clone(), Some("/".to_string())).await?;
                tracing::debug!(target: "state_sync_dump", shard_id, ?directory_path, "List state parts in s3");
                let mut file_names = vec![];
                for res in list_results {
                    for obj in res.contents {
                        file_names.push(Self::extract_file_name_from_full_path(obj.key))
                    }
                }
                Ok(file_names)
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(directory_path);
                tracing::debug!(target: "state_sync_dump", shard_id, ?path, "List state parts in local directory");
                std::fs::create_dir_all(&path)?;
                let mut file_names = vec![];
                let files = std::fs::read_dir(&path)?;
                for file in files {
                    let file_name = Self::extract_file_name_from_path_buf(file?.path());
                    file_names.push(file_name);
                }
                Ok(file_names)
            }
        }
    }
}

/// Helper to track state sync.
pub struct StateSync {
    /// How to retrieve the state data.
    inner: StateSyncInner,

    /// Is used for communication with the peers.
    network_adapter: PeerManagerAdapter,

    /// When the "sync block" was requested.
    /// The "sync block" is the last block of the previous epoch, i.e. `prev_hash` of the `sync_hash` block.
    last_time_block_requested: Option<DateTime<Utc>>,

    /// Timeout (set in config - by default to 60 seconds) is used to figure out how long we should wait
    /// for the answer from the other node before giving up.
    timeout: Duration,

    /// Maps shard_id to result of applying downloaded state.
    state_parts_apply_results: HashMap<ShardId, Result<(), near_chain_primitives::error::Error>>,

    /// Maps shard_id to result of splitting state for resharding.
    split_state_roots: HashMap<ShardId, Result<HashMap<ShardUId, StateRoot>, near_chain::Error>>,
}

impl StateSync {
    pub fn new(
        network_adapter: PeerManagerAdapter,
        timeout: TimeDuration,
        chain_id: &str,
        sync_config: &SyncConfig,
    ) -> Self {
        let inner = match sync_config {
            SyncConfig::Peers => StateSyncInner::Peers {
                last_part_id_requested: Default::default(),
                requested_target: lru::LruCache::new(MAX_PENDING_PART as usize),
            },
            SyncConfig::ExternalStorage(ExternalStorageConfig {
                location,
                num_concurrent_requests,
            }) => {
                let external = match location {
                    ExternalStorageLocation::S3 { bucket, region } => {
                        let bucket = create_bucket(&bucket, &region, timeout);
                        if let Err(err) = bucket {
                            panic!("Failed to create an S3 bucket: {}", err);
                        }
                        ExternalConnection::S3 { bucket: Arc::new(bucket.unwrap()) }
                    }
                    ExternalStorageLocation::Filesystem { root_dir } => {
                        ExternalConnection::Filesystem { root_dir: root_dir.clone() }
                    }
                };
                StateSyncInner::PartsFromExternal {
                    chain_id: chain_id.to_string(),
                    requests_remaining: Arc::new(AtomicI32::new(*num_concurrent_requests as i32)),
                    external,
                }
            }
        };
        let timeout = Duration::from_std(timeout).unwrap();
        StateSync {
            inner,
            network_adapter,
            last_time_block_requested: None,
            timeout,
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
                        tracing::error!(
                            target: "sync",
                            %prev_hash,
                            timeout_sec = self.timeout.num_seconds(),
                            "State sync: block request timed out");
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
        epoch_manager: &dyn EpochManagerAdapter,
        highest_height_peers: &[HighestHeightPeerInfo],
        tracking_shards: Vec<ShardId>,
        now: DateTime<Utc>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
        use_colour: bool,
    ) -> Result<(bool, bool), near_chain::Error> {
        let mut all_done = true;
        let mut update_sync_status = false;

        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let prev_epoch_id = chain.get_block_header(&prev_hash)?.epoch_id().clone();
        let epoch_id = chain.get_block_header(&sync_hash)?.epoch_id().clone();
        if epoch_manager.get_shard_layout(&prev_epoch_id)?
            != epoch_manager.get_shard_layout(&epoch_id)?
        {
            panic!("cannot sync to the first epoch after sharding upgrade. Please wait for the next epoch or find peers that are more up to date");
        }
        let split_states = epoch_manager.will_shard_layout_change(&prev_hash)?;

        for shard_id in tracking_shards {
            let mut download_timeout = false;
            let mut run_shard_state_download = false;
            let shard_sync_download = new_shard_sync.entry(shard_id).or_insert_with(|| {
                run_shard_state_download = true;
                update_sync_status = true;
                ShardSyncDownload::new_download_state_header(now)
            });

            let old_status = shard_sync_download.status.clone();
            let mut shard_sync_done = false;
            metrics::STATE_SYNC_STAGE
                .with_label_values(&[&shard_id.to_string()])
                .set(shard_sync_download.status.repr() as i64);
            match &shard_sync_download.status {
                ShardSyncStatus::StateDownloadHeader => {
                    (download_timeout, run_shard_state_download) = self
                        .sync_shards_download_header_status(
                            shard_id,
                            shard_sync_download,
                            sync_hash,
                            chain,
                            now,
                        )?;
                }
                ShardSyncStatus::StateDownloadParts => {
                    let res = self.sync_shards_download_parts_status(
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                        now,
                    );
                    download_timeout = res.0;
                    run_shard_state_download = res.1;
                    update_sync_status |= res.2;
                }
                ShardSyncStatus::StateDownloadScheduling => {
                    self.sync_shards_download_scheduling_status(
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                        now,
                        state_parts_task_scheduler,
                    )?;
                }
                ShardSyncStatus::StateDownloadApplying => {
                    self.sync_shards_download_applying_status(
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                        now,
                    )?;
                }
                ShardSyncStatus::StateDownloadComplete => {
                    shard_sync_done = self.sync_shards_download_complete_status(
                        split_states,
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                    )?;
                }
                ShardSyncStatus::StateSplitScheduling => {
                    debug_assert!(split_states);
                    self.sync_shards_state_split_scheduling_status(
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                        state_split_scheduler,
                        me,
                    )?;
                }
                ShardSyncStatus::StateSplitApplying(_status) => {
                    debug_assert!(split_states);
                    shard_sync_done = self.sync_shards_state_split_applying_status(
                        shard_id,
                        shard_sync_download,
                        sync_hash,
                        chain,
                    )?;
                }
                ShardSyncStatus::StateSyncDone => {
                    shard_sync_done = true;
                }
            }
            all_done &= shard_sync_done;

            if download_timeout {
                tracing::warn!(
                    target: "sync",
                    %shard_id,
                    timeout_sec = self.timeout.num_seconds(),
                    "State sync didn't download the state, sending StateRequest again");
                tracing::debug!(
                    target: "sync",
                    %shard_id,
                    %sync_hash,
                    ?me,
                    phase = format_shard_sync_phase(shard_sync_download, use_colour),
                    "State sync status");
            }

            // Execute syncing for shard `shard_id`
            if run_shard_state_download {
                update_sync_status = true;
                self.request_shard(
                    me,
                    shard_id,
                    chain,
                    epoch_manager,
                    sync_hash,
                    shard_sync_download,
                    highest_height_peers,
                )?;
            }
            update_sync_status |= shard_sync_download.status != old_status;
        }

        Ok((update_sync_status, all_done))
    }

    // Called by the client actor, when it finished applying all the downloaded parts.
    pub fn set_apply_result(
        &mut self,
        shard_id: ShardId,
        apply_result: Result<(), near_chain::Error>,
    ) {
        self.state_parts_apply_results.insert(shard_id, apply_result);
    }

    // Called by the client actor, when it finished splitting the state.
    pub fn set_split_result(
        &mut self,
        shard_id: ShardId,
        result: Result<HashMap<ShardUId, StateRoot>, near_chain::Error>,
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

    // Function called when our node receives the network response with a part.
    pub fn received_requested_part(
        &mut self,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        match &mut self.inner {
            StateSyncInner::Peers { last_part_id_requested, requested_target } => {
                let key = (part_id, sync_hash);
                // Check that it came from the target that we requested it from.
                if let Some(target) = requested_target.get(&key) {
                    if last_part_id_requested.get_mut(&(target.clone(), shard_id)).map_or(
                        false,
                        |request| {
                            request.missing_parts = request.missing_parts.saturating_sub(1);
                            request.missing_parts == 0
                        },
                    ) {
                        last_part_id_requested.remove(&(target.clone(), shard_id));
                    }
                }
            }
            StateSyncInner::PartsFromExternal { .. } => {
                // Do nothing.
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
        epoch_manager: &dyn EpochManagerAdapter,
        sync_hash: CryptoHash,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<Vec<AccountOrPeerIdOrHash>, near_chain::Error> {
        let prev_block_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let epoch_hash = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;

        let block_producers =
            epoch_manager.get_epoch_block_producers_ordered(&epoch_hash, &sync_hash)?;
        let peers = block_producers
            .iter()
            .filter_map(|(validator_stake, _slashed)| {
                let account_id = validator_stake.account_id();
                if epoch_manager
                    .cares_about_shard_from_prev_block(&prev_block_hash, account_id, shard_id)
                    .unwrap_or(false)
                {
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
            }));
        Ok(self.select_peers(peers.collect(), shard_id)?)
    }

    /// Avoids peers that already have outstanding requests for parts.
    fn select_peers(
        &mut self,
        peers: Vec<AccountOrPeerIdOrHash>,
        shard_id: ShardId,
    ) -> Result<Vec<AccountOrPeerIdOrHash>, near_chain::Error> {
        let res = match &mut self.inner {
            StateSyncInner::Peers {
                last_part_id_requested,
                requested_target: _requested_target,
            } => {
                last_part_id_requested.retain(|_, request| !request.expired());
                peers
                    .into_iter()
                    .filter(|candidate| {
                        // If we still have a pending request from this node - don't add another one.
                        !last_part_id_requested.contains_key(&(candidate.clone(), shard_id))
                    })
                    .collect::<Vec<_>>()
            }
            StateSyncInner::PartsFromExternal { .. } => peers,
        };
        Ok(res)
    }

    /// Returns new ShardSyncDownload if successful, otherwise returns given shard_sync_download
    fn request_shard(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &Chain,
        epoch_manager: &dyn EpochManagerAdapter,
        sync_hash: CryptoHash,
        shard_sync_download: &mut ShardSyncDownload,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        let possible_targets = self.possible_targets(
            me,
            shard_id,
            chain,
            epoch_manager,
            sync_hash,
            highest_height_peers,
        )?;

        if possible_targets.is_empty() {
            // In most cases it means that all the targets are currently busy (that we have a pending request with them).
            return Ok(());
        }

        // Downloading strategy starts here
        match shard_sync_download.status {
            ShardSyncStatus::StateDownloadHeader => {
                self.request_shard_header(
                    shard_id,
                    sync_hash,
                    &possible_targets,
                    shard_sync_download,
                );
            }
            ShardSyncStatus::StateDownloadParts => {
                self.request_shard_parts(
                    shard_id,
                    sync_hash,
                    possible_targets,
                    shard_sync_download,
                    chain,
                );
            }
            _ => {}
        }

        Ok(())
    }

    /// Makes a StateRequestHeader header to one of the peers.
    fn request_shard_header(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        possible_targets: &[AccountOrPeerIdOrHash],
        new_shard_sync_download: &mut ShardSyncDownload,
    ) {
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
                .send_async(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::StateRequestHeader { shard_id, sync_hash, target },
                ))
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

    /// Makes requests to download state parts for the given epoch of the given shard.
    fn request_shard_parts(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        possible_targets: Vec<AccountOrPeerIdOrHash>,
        new_shard_sync_download: &mut ShardSyncDownload,
        chain: &Chain,
    ) {
        // Iterate over all parts that needs to be requested (i.e. download.run_me is true).
        // Parts are ordered such that its index match its part_id.
        match &mut self.inner {
            StateSyncInner::Peers { last_part_id_requested, requested_target } => {
                // We'll select all the 'highest' peers + validators as candidates (excluding those that gave us timeout in the past).
                // And for each one of them, we'll ask for up to 16 (MAX_STATE_PART_REQUEST) parts.
                let possible_targets_sampler =
                    SamplerLimited::new(possible_targets, MAX_STATE_PART_REQUEST);

                // For every part that needs to be requested it is selected one
                // peer (target) randomly to request the part from.
                // IMPORTANT: here we use 'zip' with possible_target_sampler -
                // which is limited. So at any moment we'll not request more
                // than possible_targets.len() * MAX_STATE_PART_REQUEST parts.
                for ((part_id, download), target) in
                    parts_to_fetch(new_shard_sync_download).zip(possible_targets_sampler)
                {
                    sent_request_part(
                        target.clone(),
                        part_id,
                        shard_id,
                        sync_hash,
                        last_part_id_requested,
                        requested_target,
                        self.timeout,
                    );
                    request_part_from_peers(
                        part_id,
                        target,
                        download,
                        shard_id,
                        sync_hash,
                        &self.network_adapter,
                    );
                }
            }
            StateSyncInner::PartsFromExternal { chain_id, requests_remaining, external } => {
                let sync_block_header = chain.get_block_header(&sync_hash).unwrap();
                let epoch_id = sync_block_header.epoch_id();
                let epoch_info = chain.epoch_manager.get_epoch_info(epoch_id).unwrap();
                let epoch_height = epoch_info.epoch_height();

                let shard_state_header = chain.get_state_header(shard_id, sync_hash).unwrap();
                let state_num_parts =
                    get_num_state_parts(shard_state_header.state_root_node().memory_usage);

                for (part_id, download) in parts_to_fetch(new_shard_sync_download) {
                    request_part_from_external_storage(
                        part_id,
                        download,
                        shard_id,
                        epoch_id,
                        epoch_height,
                        state_num_parts,
                        &chain_id.clone(),
                        requests_remaining.clone(),
                        external.clone(),
                    );
                }
            }
        }
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
        epoch_manager: &dyn EpochManagerAdapter,
        highest_height_peers: &[HighestHeightPeerInfo],
        // Shards to sync.
        tracking_shards: Vec<ShardId>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
        state_split_scheduler: &dyn Fn(StateSplitRequest),
        use_colour: bool,
    ) -> Result<StateSyncResult, near_chain::Error> {
        let _span = tracing::debug_span!(target: "sync", "run", sync = "StateSync").entered();
        tracing::trace!(target: "sync", %sync_hash, ?tracking_shards, "syncing state");
        let prev_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        let now = StaticClock::utc();

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
            epoch_manager,
            highest_height_peers,
            tracking_shards,
            now,
            state_parts_task_scheduler,
            state_split_scheduler,
            use_colour,
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
                                tracing::error!(target: "sync", %shard_id, %hash, ?err, "State sync set_state_header error");
                                shard_sync_download.downloads[0].error = true;
                            }
                        }
                    }
                } else {
                    // No header found.
                    // It may happen because requested node couldn't build state response.
                    if !shard_sync_download.downloads[0].done {
                        tracing::info!(target: "sync", %shard_id, %hash, "state_response doesn't have header, should be re-requested");
                        shard_sync_download.downloads[0].error = true;
                    }
                }
            }
            ShardSyncStatus::StateDownloadParts => {
                if let Some(part) = state_response.take_part() {
                    let num_parts = shard_sync_download.downloads.len() as u64;
                    let (part_id, data) = part;
                    if part_id >= num_parts {
                        tracing::error!(target: "sync", %shard_id, %hash, part_id, "State sync received incorrect part_id, potential malicious peer");
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
                                tracing::error!(target: "sync", %shard_id, %hash, part_id, ?err, "State sync set_state_part error");
                                shard_sync_download.downloads[part_id as usize].error = true;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Checks if the header is downloaded.
    /// If the download is complete, then moves forward to `StateDownloadParts`,
    /// otherwise retries the header request.
    /// Returns `(download_timeout, run_shard_state_download)` where:
    /// * `download_timeout` means that the state header request timed out (and needs to be retried).
    /// * `run_shard_state_download` means that header or part download requests need to run for this shard.
    fn sync_shards_download_header_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
    ) -> Result<(bool, bool), near_chain::Error> {
        let mut download_timeout = false;
        let mut run_shard_state_download = false;
        // StateDownloadHeader is the first step. We want to fetch the basic information about the state (its size, hash etc).
        if shard_sync_download.downloads[0].done {
            let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
            let state_num_parts =
                get_num_state_parts(shard_state_header.state_root_node().memory_usage);
            // If the header was downloaded successfully - move to phase 2 (downloading parts).
            // Create the vector with entry for each part.
            *shard_sync_download =
                ShardSyncDownload::new_download_state_parts(now, state_num_parts);
            run_shard_state_download = true;
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
                run_shard_state_download = true;
            }
        }
        Ok((download_timeout, run_shard_state_download))
    }

    /// Checks if the parts are downloaded.
    /// If download of all parts is complete, then moves forward to `StateDownloadScheduling`.
    /// Returns `(download_timeout, run_shard_state_download, update_sync_status)` where:
    /// * `download_timeout` means that the state header request timed out (and needs to be retried).
    /// * `run_shard_state_download` means that header or part download requests need to run for this shard.
    /// * `update_sync_status` means that something changed in `ShardSyncDownload` and it needs to be persisted.
    fn sync_shards_download_parts_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
    ) -> (bool, bool, bool) {
        // Step 2 - download all the parts (each part is usually around 1MB).
        let mut download_timeout = false;
        let mut run_shard_state_download = false;
        let mut update_sync_status = false;

        let mut parts_done = true;
        let num_parts = shard_sync_download.downloads.len();
        let mut num_parts_done = 0;
        for (part_id, part_download) in shard_sync_download.downloads.iter_mut().enumerate() {
            if !part_download.done {
                // Check if a download from an external storage is finished.
                update_sync_status |= check_external_storage_part_response(
                    part_id as u64,
                    num_parts as u64,
                    shard_id,
                    sync_hash,
                    part_download,
                    chain,
                );
            }
            if !part_download.done {
                parts_done = false;
                let prev = part_download.prev_update_time;
                let part_timeout = now - prev > self.timeout; // Retry parts that failed.
                if part_timeout || part_download.error {
                    download_timeout |= part_timeout;
                    if part_timeout || part_download.last_target.is_some() {
                        // Don't immediately retry failed requests from external
                        // storage. Most often error is a state part not
                        // available. That error doesn't get fixed by retrying,
                        // but rather by waiting.
                        metrics::STATE_SYNC_RETRY_PART
                            .with_label_values(&[&shard_id.to_string()])
                            .inc();
                        part_download.run_me.store(true, Ordering::SeqCst);
                        part_download.error = false;
                        part_download.prev_update_time = now;
                        update_sync_status = true;
                    }
                }
                if part_download.run_me.load(Ordering::SeqCst) {
                    run_shard_state_download = true;
                }
            }
            if part_download.done {
                num_parts_done += 1;
            }
        }
        tracing::debug!(target: "sync", %shard_id, %sync_hash, num_parts_done, parts_done);
        metrics::STATE_SYNC_PARTS_DONE
            .with_label_values(&[&shard_id.to_string()])
            .set(num_parts_done);
        metrics::STATE_SYNC_PARTS_TOTAL
            .with_label_values(&[&shard_id.to_string()])
            .set(num_parts as i64);
        // If all parts are done - we can move towards scheduling.
        if parts_done {
            *shard_sync_download = ShardSyncDownload {
                downloads: vec![],
                status: ShardSyncStatus::StateDownloadScheduling,
            };
            update_sync_status = true;
        }
        (download_timeout, run_shard_state_download, update_sync_status)
    }

    fn sync_shards_download_scheduling_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
        state_parts_task_scheduler: &dyn Fn(ApplyStatePartsRequest),
    ) -> Result<(), near_chain::Error> {
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
            Err(err) => {
                // Cannot finalize the downloaded state.
                // The reasonable behavior here is to start from the very beginning.
                metrics::STATE_SYNC_DISCARD_PARTS.with_label_values(&[&shard_id.to_string()]).inc();
                tracing::error!(target: "sync", %shard_id, %sync_hash, ?err, "State sync finalizing error");
                *shard_sync_download = ShardSyncDownload::new_download_state_header(now);
                chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
            }
        }
        Ok(())
    }

    fn sync_shards_download_applying_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
    ) -> Result<(), near_chain::Error> {
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
                Err(err) => {
                    // Cannot finalize the downloaded state.
                    // The reasonable behavior here is to start from the very beginning.
                    metrics::STATE_SYNC_DISCARD_PARTS
                        .with_label_values(&[&shard_id.to_string()])
                        .inc();
                    tracing::error!(target: "sync", %shard_id, %sync_hash, ?err, "State sync finalizing error");
                    *shard_sync_download = ShardSyncDownload::new_download_state_header(now);
                    let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                    let state_num_parts =
                        get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                    chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                }
            }
        }
        Ok(())
    }

    fn sync_shards_download_complete_status(
        &mut self,
        split_states: bool,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
    ) -> Result<bool, near_chain::Error> {
        let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
        let state_num_parts =
            get_num_state_parts(shard_state_header.state_root_node().memory_usage);
        chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;

        let mut shard_sync_done = false;
        // If the shard layout is changing in this epoch - we have to apply it right now.
        if split_states {
            *shard_sync_download = ShardSyncDownload {
                downloads: vec![],
                status: ShardSyncStatus::StateSplitScheduling,
            }
        } else {
            // If there is no layout change - we're done.
            *shard_sync_download =
                ShardSyncDownload { downloads: vec![], status: ShardSyncStatus::StateSyncDone };
            shard_sync_done = true;
        }
        Ok(shard_sync_done)
    }

    fn sync_shards_state_split_scheduling_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        state_split_scheduler: &dyn Fn(StateSplitRequest),
        me: &Option<AccountId>,
    ) -> Result<(), near_chain::Error> {
        let status = Arc::new(StateSplitApplyingStatus::default());
        chain.build_state_for_split_shards_preprocessing(
            &sync_hash,
            shard_id,
            state_split_scheduler,
            status.clone(),
        )?;
        tracing::debug!(target: "sync", %shard_id, %sync_hash, ?me, "State sync split scheduled");
        *shard_sync_download = ShardSyncDownload {
            downloads: vec![],
            status: ShardSyncStatus::StateSplitApplying(status),
        };
        Ok(())
    }

    /// Returns whether the State Sync for the given shard is complete.
    fn sync_shards_state_split_applying_status(
        &mut self,
        shard_id: ShardId,
        shard_sync_download: &mut ShardSyncDownload,
        sync_hash: CryptoHash,
        chain: &mut Chain,
    ) -> Result<bool, near_chain::Error> {
        let result = self.split_state_roots.remove(&shard_id);
        let mut shard_sync_done = false;
        if let Some(state_roots) = result {
            chain.build_state_for_split_shards_postprocessing(&sync_hash, state_roots)?;
            *shard_sync_download =
                ShardSyncDownload { downloads: vec![], status: ShardSyncStatus::StateSyncDone };
            shard_sync_done = true;
        }
        Ok(shard_sync_done)
    }
}

fn create_bucket(
    bucket: &str,
    region: &str,
    timeout: TimeDuration,
) -> Result<s3::Bucket, near_chain::Error> {
    let mut bucket = s3::Bucket::new(
        bucket,
        region.parse::<s3::Region>().map_err(|err| near_chain::Error::Other(err.to_string()))?,
        s3::creds::Credentials::anonymous()
            .map_err(|err| near_chain::Error::Other(err.to_string()))?,
    )
    .map_err(|err| near_chain::Error::Other(err.to_string()))?;
    // Ensure requests finish in finite amount of time.
    bucket.set_request_timeout(Some(timeout));
    Ok(bucket)
}

/// Returns parts that still need to be fetched.
fn parts_to_fetch(
    new_shard_sync_download: &mut ShardSyncDownload,
) -> impl Iterator<Item = (u64, &mut DownloadStatus)> {
    new_shard_sync_download
        .downloads
        .iter_mut()
        .enumerate()
        .filter(|(_, download)| download.run_me.load(Ordering::SeqCst))
        .map(|(part_id, download)| (part_id as u64, download))
}

/// Starts an asynchronous network request to external storage to fetch the given state part.
fn request_part_from_external_storage(
    part_id: u64,
    download: &mut DownloadStatus,
    shard_id: ShardId,
    epoch_id: &EpochId,
    epoch_height: EpochHeight,
    num_parts: u64,
    chain_id: &str,
    requests_remaining: Arc<AtomicI32>,
    external: ExternalConnection,
) {
    if !allow_request(&requests_remaining) {
        return;
    } else {
        if !download.run_me.swap(false, Ordering::SeqCst) {
            tracing::info!(target: "sync", %shard_id, part_id, "run_me is already false");
            return;
        }
    }
    download.state_requests_count += 1;
    download.last_target = None;

    let location =
        external_storage_location(chain_id, epoch_id, epoch_height, shard_id, part_id, num_parts);
    let download_response = download.response.clone();
    near_performance_metrics::actix::spawn("StateSync", {
        async move {
            let result = external.get_part(shard_id, &location).await;
            finished_request(&requests_remaining);
            let mut lock = download_response.lock().unwrap();
            *lock = Some(result.map_err(|err| err.to_string()));
        }
    });
}

/// Asynchronously requests a state part from a suitable peer.
fn request_part_from_peers(
    part_id: u64,
    target: AccountOrPeerIdOrHash,
    download: &mut DownloadStatus,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    network_adapter: &PeerManagerAdapter,
) {
    download.run_me.store(false, Ordering::SeqCst);
    download.state_requests_count += 1;
    download.last_target = Some(make_account_or_peer_id_or_hash(target.clone()));
    let run_me = download.run_me.clone();

    near_performance_metrics::actix::spawn(
        "StateSync",
        network_adapter
            .send_async(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::StateRequestPart { shard_id, sync_hash, part_id, target },
            ))
            .then(move |result| {
                // TODO: possible optimization - in the current code, even if one of the targets it not present in the network graph
                //       (so we keep getting RouteNotFound) - we'll still keep trying to assign parts to it.
                //       Fortunately only once every 60 seconds (timeout value).
                if let Ok(NetworkResponses::RouteNotFound) = result.map(|f| f.as_network_response())
                {
                    // Send a StateRequestPart on the next iteration
                    run_me.store(true, Ordering::SeqCst);
                }
                future::ready(())
            }),
    );
}

fn sent_request_part(
    target: AccountOrPeerIdOrHash,
    part_id: u64,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    last_part_id_requested: &mut HashMap<(AccountOrPeerIdOrHash, ShardId), PendingRequestStatus>,
    requested_target: &mut lru::LruCache<(u64, CryptoHash), AccountOrPeerIdOrHash>,
    timeout: Duration,
) {
    // FIXME: something is wrong - the index should have a shard_id too.
    requested_target.put((part_id, sync_hash), target.clone());
    last_part_id_requested
        .entry((target, shard_id))
        .and_modify(|pending_request| {
            pending_request.missing_parts += 1;
        })
        .or_insert_with(|| PendingRequestStatus::new(timeout));
}

/// Verifies that one more concurrent request can be started.
fn allow_request(requests_remaining: &AtomicI32) -> bool {
    let remaining = requests_remaining.fetch_sub(1, Ordering::SeqCst);
    if remaining <= 0 {
        requests_remaining.fetch_add(1, Ordering::SeqCst);
        false
    } else {
        true
    }
}

fn finished_request(requests_remaining: &AtomicI32) {
    requests_remaining.fetch_add(1, Ordering::SeqCst);
}

/// Works around how data requests to external storage are done.
/// The response is stored on the DownloadStatus object.
/// This function investigates if the response is available and updates `done` and `error` appropriately.
/// If the response is successful, then also writes the state part to the DB.
///
/// Returns whether something changed in `DownloadStatus` which means it needs to be persisted.
fn check_external_storage_part_response(
    part_id: u64,
    num_parts: u64,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_download: &mut DownloadStatus,
    chain: &mut Chain,
) -> bool {
    let external_storage_response = {
        let mut lock = part_download.response.lock().unwrap();
        if let Some(response) = lock.clone() {
            tracing::debug!(target: "sync", %shard_id, part_id, "Got response from external storage");
            // Remove the response from DownloadStatus, because
            // we're going to write state parts to DB and don't need to keep
            // them in `DownloadStatus`.
            *lock = None;
            response
        } else {
            return false;
        }
    };

    let mut err_to_retry = None;
    match external_storage_response {
        // HTTP status code 200 means success.
        Ok(data) => {
            match chain.set_state_part(
                shard_id,
                sync_hash,
                PartId::new(part_id as u64, num_parts as u64),
                &data,
            ) {
                Ok(_) => {
                    metrics::STATE_SYNC_EXTERNAL_PARTS_DONE
                        .with_label_values(&[&shard_id.to_string()])
                        .inc();
                    metrics::STATE_SYNC_EXTERNAL_PARTS_SIZE_DOWNLOADED
                        .with_label_values(&[&shard_id.to_string()])
                        .inc_by(data.len() as u64);
                    part_download.done = true;
                    tracing::debug!(target: "sync", %shard_id, part_id, ?part_download, "Set state part success");
                }
                Err(err) => {
                    metrics::STATE_SYNC_EXTERNAL_PARTS_FAILED
                        .with_label_values(&[&shard_id.to_string()])
                        .inc();
                    tracing::warn!(target: "sync", %shard_id, %sync_hash, part_id, ?err, "Failed to save a state part");
                    err_to_retry =
                        Some(near_chain::Error::Other("Failed to save a state part".to_string()));
                }
            }
        }
        // The request failed without reaching the external storage.
        Err(err) => {
            err_to_retry = Some(near_chain::Error::Other(err));
        }
    };

    if let Some(err) = err_to_retry {
        tracing::debug!(target: "sync", %shard_id, %sync_hash, part_id, ?err, "Failed to get a part from external storage, will retry");
        part_download.error = true;
    }
    true
}

/// Applies style if `use_colour` is enabled.
fn paint(s: &str, style: Style, use_style: bool) -> String {
    if use_style {
        style.paint(s).to_string()
    } else {
        s.to_string()
    }
}

/// Formats the given ShardSyncDownload for logging.
pub(crate) fn format_shard_sync_phase(
    shard_sync_download: &ShardSyncDownload,
    use_colour: bool,
) -> String {
    match &shard_sync_download.status {
        ShardSyncStatus::StateDownloadHeader => format!(
            "{} requests sent {}, last target {:?}",
            paint("HEADER", Purple.bold(), use_colour),
            shard_sync_download.downloads[0].state_requests_count,
            shard_sync_download.downloads[0].last_target
        ),
        ShardSyncStatus::StateDownloadParts => {
            let mut num_parts_done = 0;
            let mut num_parts_not_done = 0;
            let mut text = "".to_string();
            for (i, download) in shard_sync_download.downloads.iter().enumerate() {
                if download.done {
                    num_parts_done += 1;
                    continue;
                }
                num_parts_not_done += 1;
                text.push_str(&format!(
                    "[{}: {}, {}, {:?}] ",
                    paint(&i.to_string(), Yellow.bold(), use_colour),
                    download.done,
                    download.state_requests_count,
                    download.last_target
                ));
            }
            format!(
                "{} [{}: is_done, requests sent, last target] {} num_parts_done={} num_parts_not_done={}",
                paint("PARTS", Purple.bold(), use_colour),
                paint("part_id", Yellow.bold(), use_colour),
                text,
                num_parts_done,
                num_parts_not_done
            )
        }
        status => format!("{:?}", status),
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

/// Construct a location on the external storage.
pub fn external_storage_location(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: u64,
    part_id: u64,
    num_parts: u64,
) -> String {
    format!(
        "{}/{}",
        location_prefix(chain_id, epoch_height, epoch_id, shard_id),
        part_filename(part_id, num_parts)
    )
}

pub fn external_storage_location_directory(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: u64,
) -> String {
    location_prefix(chain_id, epoch_height, epoch_id, shard_id)
}

pub fn location_prefix(
    chain_id: &str,
    epoch_height: u64,
    epoch_id: &EpochId,
    shard_id: u64,
) -> String {
    format!(
        "chain_id={}/epoch_height={}/epoch_id={}/shard_id={}",
        chain_id, epoch_height, epoch_id.0, shard_id
    )
}

pub fn part_filename(part_id: u64, num_parts: u64) -> String {
    format!("state_part_{:06}_of_{:06}", part_id, num_parts)
}

pub fn match_filename(s: &str) -> Option<regex::Captures> {
    let re = regex::Regex::new(r"^state_part_(\d{6})_of_(\d{6})$").unwrap();
    re.captures(s)
}

pub fn is_part_filename(s: &str) -> bool {
    match_filename(s).is_some()
}

pub fn get_num_parts_from_filename(s: &str) -> Option<u64> {
    if let Some(captures) = match_filename(s) {
        if let Some(num_parts) = captures.get(2) {
            if let Ok(num_parts) = num_parts.as_str().parse::<u64>() {
                return Some(num_parts);
            }
        }
    }
    None
}

pub fn get_part_id_from_filename(s: &str) -> Option<u64> {
    if let Some(captures) = match_filename(s) {
        if let Some(part_id) = captures.get(1) {
            if let Ok(part_id) = part_id.as_str().parse::<u64>() {
                return Some(part_id);
            }
        }
    }
    None
}

#[cfg(test)]
mod test {
    use super::*;
    use actix::System;
    use near_actix_test_utils::run_actix;
    use near_chain::test_utils;
    use near_chain::{test_utils::process_block_sync, BlockProcessingArtifact, Provenance};
    use near_epoch_manager::EpochManagerAdapter;
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_primitives::{
        syncing::{ShardStateSyncResponseHeader, ShardStateSyncResponseV2},
        test_utils::TestBlockBuilder,
        types::EpochId,
    };

    #[test]
    // Start a new state sync - and check that it asks for a header.
    fn test_ask_for_header() {
        let mock_peer_manager = Arc::new(MockPeerManagerAdapter::default());
        let mut state_sync = StateSync::new(
            mock_peer_manager.clone().into(),
            TimeDuration::from_secs(1),
            "chain_id",
            &SyncConfig::Peers,
        );
        let mut new_shard_sync = HashMap::new();

        let (mut chain, kv, _, signer) = test_utils::setup();

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
                    kv.as_ref(),
                    &[],
                    vec![0],
                    &apply_parts_fn,
                    &state_split_fn,
                    false,
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

    #[test]
    fn test_match_filename() {
        let filename = part_filename(5, 15);
        assert!(is_part_filename(&filename));
        assert!(!is_part_filename("123123"));

        assert_eq!(get_num_parts_from_filename(&filename), Some(15));
        assert_eq!(get_num_parts_from_filename("123123"), None);

        assert_eq!(get_part_id_from_filename(&filename), Some(5));
        assert_eq!(get_part_id_from_filename("123123"), None);
    }
}
