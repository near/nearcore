//! This module implements ShardManager, which handles chunks requesting and processing.
//! Since blocks only contain chunk headers, full chunks must be communicated separately.
//! For data availability, information in a chunk is divided into parts by Reed Solomon encoding,
//! and each validator holds a subset of parts of each chunk (a validator is called the owner
//! of the parts that they hold). This way, a chunk can be retrieved from any X validators
//! where X is the threshold for the Reed Solomon encoding for retrieving the full information.
//! Currently, X is set to be 1/3 of total validator seats (num_data_parts).
//!
//! **How chunks are propagated in the network
//! Instead sending the full chunk, chunk content is communicated between nodes
//! through PartialEncodedChunk, which includes the chunk header, some parts and
//! receipts of the chunk.  Full chunk can be reconstructed if a node receives
//! enough chunk parts.
//! A node receives partial encoded chunks in three ways,
//! - by requesting it and receiving a PartialEncodedChunkResponse,
//! - by receiving a PartialEncodedChunk, which is sent from the original chunk
//!   producer to the part owners after the chunk is produced
//! - by receiving a PartialEncodedChunkForward, which is sent from part owners
//!   to validators who track the shard, when a validator first receives a part
//!   it owns.  TODO(#5886): this is actually not the current behavior.
//! Note that last two messages can only be sent from validators to validators,
//! so the only way a non-validator receives a partial encoded chunk is by
//! requesting it.
//!
//! ** Requesting for chunks
//! `ShardManager` keeps a request pool that stores all requests for chunks that are not completed
//! yet. The requests are managed at the chunk level, instead of individual parts and receipts.
//! A new request can be added by calling function `request_chunk_single`. If it is not
//! in the pool yet, `request_partial_encoded_chunk` will be called, which checks which parts or
//! receipts are still needed for the chunk by checking `encoded_chunks` (see the section on
//! ** Storing chunks). This way, the node won't send requests for parts and receipts they already have.
//! It then figures out where to request them, either from the original
//! chunk producer, or a block producer or peer who tracks the shard, and sends out the network
//! requests. Check the logic there for details regarding how targets of requests are chosen.
//!
//! Once a request is added the pool, it can be resent through `resend_chunk_requests`,
//! which is done periodically through client_actor. A request is only removed from the pool when
//! all needed parts and receipts in the requested chunk are received.
//!
//! ** Storing chunks
//! Before a chunk can be reconstructed fully, parts and receipts in the chunk are stored in
//! `encoded_chunks`. Full chunks will be persisted in the database storage after they are
//! reconstructed.
//!
//! ** Forwarding chunks
//! To save messages and time for chunks to propagate among validators, we implemented a feature
//! called ForwardChunkParts. When a validator receives a part it owns, it forwards the part to
//! other validators who are assigned to track the shard through a PartialEncodedChunkForward message.
//! This saves the number of requests validators need to send to get all parts they need. A forwarded
//! part can only be processed after the node has the corresponding chunk header, either from blocks
//! or partial chunk requests. Before that, they are temporarily stored in `chunk_forwards_cache`.
//! After that, they are processed as a PartialEncodedChunk message only containing one part.
//!
//! ** Processing chunks
//! Function `process_partial_encoded_chunk` processes a partial encoded chunk message.
//! 1) validates the parts and receipts in the message
//! 2) merges the parts and receipts are into `encoded_chunks`.
//! 3) forwards newly received owned parts to other validators, if any.
//! 4) checks if there are any forwarded chunk parts in `chunk_forwards_cache` that can be processed.
//! 5) checks if all needed parts and receipts are received and tries to reconstruct the full chunk.
//!    If successful, removes request for the chunk from the request pool.
//! Note that the last step requires the previous block of the chunk has been accepted.
//! If not, the function will return `NeedBlock`. To avoid a chunk getting stuck waiting on
//! the previous block, when a new block is accepted, client must remember to call
//! `get_incomplete_chunks` to get the list of incomplete chunks who are waiting on the block
//! and process them
//!
//! ** Validating chunks
//! Before `process_partial_encoded_chunk` returns HaveAllPartsAndReceipts, it will perform
//! the validation steps and return error if validation fails.
//! 1) validate the chunk header is signed by the correct chunk producer and the chunk producer
//!    is not slashed (see `validate_chunk_header`)
//! 2) validate the merkle proofs of the parts and receipts with regarding to the parts root and
//!    receipts root in the chunk header (see the beginning of `process_partial_encoded_chunk`)
//! 3) after the full chunk is reconstructed, validate chunk's proofs in the header matches the body
//!    (see validate_chunk_proofs)
//!
//! We also guarantee that all entries stored inside ShardsManager::encoded_chunks have the chunk header
//! at least "partially" validated by `validate_chunk_header` (see the comments there for what "partial"
//! validation means).

use std::cmp;
use std::collections::{btree_map, hash_map, BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use adapter::ShardsManagerRequestFromClient;
use chrono::DateTime;
use client::ClientAdapterForShardsManager;
use logic::{
    decode_encoded_chunk, make_outgoing_receipts_proofs,
    make_partial_encoded_chunk_from_owned_parts_and_needed_receipts, need_part, need_receipt,
};
use metrics::{
    PARTIAL_ENCODED_CHUNK_FORWARD_CACHED_WITHOUT_HEADER, PARTIAL_ENCODED_CHUNK_RESPONSE_DELAY,
};
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_primitives::time::Utc;
use rand::seq::{IteratorRandom, SliceRandom};
use tracing::{debug, error, warn};

use near_chain::{byzantine_assert, RuntimeWithEpochManagerAdapter};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{verify_path, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, EncodedShardChunkBody, PartialEncodedChunk,
    PartialEncodedChunkPart, PartialEncodedChunkV2, ReceiptList, ReceiptProof, ReedSolomonWrapper,
    ShardChunk, ShardChunkHeader, ShardProof,
};
use near_primitives::time::Clock;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash, ShardId, StateRoot,
};
use near_primitives::unwrap_or_return;
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolVersion;

use crate::chunk_cache::{EncodedChunksCache, EncodedChunksCacheEntry};
use crate::logic::cares_about_shard_this_or_next_epoch;
use near_chain::near_chain_primitives::error::Error::DBNotFoundErr;
pub use near_chunks_primitives::Error;
use near_network::types::{
    AccountIdOrPeerTrackingShard, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg,
};
use near_o11y::WithSpanContextExt;
use rand::Rng;

pub mod adapter;
mod chunk_cache;
pub mod client;
pub mod logic;
pub mod metrics;
pub mod shards_manager_actor;
pub mod test_utils;

const CHUNK_PRODUCER_BLACKLIST_SIZE: usize = 100;
pub const CHUNK_REQUEST_RETRY_MS: u64 = 100;
pub const CHUNK_REQUEST_SWITCH_TO_OTHERS_MS: u64 = 400;
pub const CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS: u64 = 3_000;
const CHUNK_REQUEST_RETRY_MAX_MS: u64 = 1_000_000;
const CHUNK_FORWARD_CACHE_SIZE: usize = 1000;
const ACCEPTING_SEAL_PERIOD_MS: i64 = 30_000;
const NUM_PARTS_REQUESTED_IN_SEAL: usize = 3;
// TODO(#3180): seals are disabled in single shard setting
// const NUM_PARTS_LEFT_IN_SEAL: usize = 1;
const PAST_SEAL_HEIGHT_HORIZON: BlockHeightDelta = 1024;
// Only request chunks from peers whose latest height >= chunk_height - CHUNK_REQUEST_PEER_HORIZON
const CHUNK_REQUEST_PEER_HORIZON: BlockHeightDelta = 5;

#[derive(PartialEq, Eq)]
pub enum ChunkStatus {
    Complete(Vec<MerklePath>),
    Incomplete,
    Invalid,
}

#[derive(Debug)]
pub enum ProcessPartialEncodedChunkResult {
    /// The information included in the partial encoded chunk is already known, no processing is needed
    Known,
    /// All parts and receipts in the chunk are received and the chunk has been processed
    HaveAllPartsAndReceipts,
    /// More parts and receipts are needed for processing the full chunk
    NeedMorePartsOrReceipts,
    /// PartialEncodedChunkMessage is received earlier than Block for the same height.
    /// Without the block we cannot restore the epoch and save encoded chunk data.
    NeedBlock,
}

#[derive(Clone, Debug)]
struct ChunkRequestInfo {
    height: BlockHeight,
    // hash of the ancestor hash used for the request, i.e., the first block up the
    // parent chain of the block that has missing chunks that is approved
    ancestor_hash: CryptoHash,
    // previous block hash of the chunk
    prev_block_hash: CryptoHash,
    shard_id: ShardId,
    added: Instant,
    last_requested: Instant,
}

struct RequestPool {
    retry_duration: Duration,
    switch_to_others_duration: Duration,
    switch_to_full_fetch_duration: Duration,
    max_duration: Duration,
    requests: HashMap<ChunkHash, ChunkRequestInfo>,
}

impl RequestPool {
    pub fn new(
        retry_duration: Duration,
        switch_to_others_duration: Duration,
        switch_to_full_fetch_duration: Duration,
        max_duration: Duration,
    ) -> Self {
        Self {
            retry_duration,
            switch_to_others_duration,
            switch_to_full_fetch_duration,
            max_duration,
            requests: HashMap::default(),
        }
    }
    pub fn contains_key(&self, chunk_hash: &ChunkHash) -> bool {
        self.requests.contains_key(chunk_hash)
    }
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, chunk_request: ChunkRequestInfo) {
        self.requests.insert(chunk_hash, chunk_request);
    }

    pub fn get_request_info(&self, chunk_hash: &ChunkHash) -> Option<&ChunkRequestInfo> {
        self.requests.get(chunk_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) {
        self.requests.remove(chunk_hash);
    }

    pub fn fetch(&mut self) -> Vec<(ChunkHash, ChunkRequestInfo)> {
        let mut removed_requests = HashSet::<ChunkHash>::default();
        let mut requests = Vec::new();
        for (chunk_hash, mut chunk_request) in self.requests.iter_mut() {
            if chunk_request.added.elapsed() > self.max_duration {
                debug!(target: "chunks", "Evicted chunk requested that was never fetched {} (shard_id: {})", chunk_hash.0, chunk_request.shard_id);
                removed_requests.insert(chunk_hash.clone());
                continue;
            }
            if chunk_request.last_requested.elapsed() > self.retry_duration {
                chunk_request.last_requested = Clock::instant();
                requests.push((chunk_hash.clone(), chunk_request.clone()));
            }
        }
        for chunk_hash in removed_requests {
            self.requests.remove(&chunk_hash);
        }
        requests
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Seal<'a> {
    Past,
    Active(&'a mut ActiveSealDemur),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveSealDemur {
    part_ords: HashSet<u64>,
    chunk_producer: AccountId,
    sent: DateTime<Utc>,
    height: BlockHeight,
}

impl Seal<'_> {
    // TODO(#3180): seals are disabled in single shard setting
    /*fn process(self, chunk_entry: &EncodedChunksCacheEntry) -> bool {
        match self {
            Seal::Past => true,
            Seal::Active(demur) => {
                let mut res = true;
                demur.part_ords.retain(|part_ord| {
                    if !chunk_entry.parts.contains_key(&part_ord) {
                        res = false;
                        true
                    } else {
                        false
                    }
                });
                res
            }
        }
    }*/

    fn contains_part_ord(&self, part_ord: &u64) -> bool {
        match self {
            Seal::Past => false,
            Seal::Active(demur) => demur.part_ords.contains(part_ord),
        }
    }
}

pub struct SealsManager {
    me: Option<AccountId>,
    runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter>,

    active_demurs: HashMap<ChunkHash, ActiveSealDemur>,
    past_seals: BTreeMap<BlockHeight, HashSet<ChunkHash>>,
    dont_include_chunks_from: lru::LruCache<AccountId, ()>,
}

impl SealsManager {
    fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter>,
    ) -> Self {
        Self {
            me,
            runtime_adapter,
            active_demurs: HashMap::new(),
            past_seals: BTreeMap::new(),
            dont_include_chunks_from: lru::LruCache::new(CHUNK_PRODUCER_BLACKLIST_SIZE),
        }
    }

    fn get_seal(
        &mut self,
        chunk_hash: &ChunkHash,
        parent_hash: &CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<Seal, near_chain::Error> {
        match self.past_seals.get(&height) {
            Some(hashes) if hashes.contains(chunk_hash) => Ok(Seal::Past),

            // None | Some(hashes) if !hashes.contains(chunk_hash)
            _ => self
                .get_active_seal(chunk_hash, parent_hash, height, shard_id)
                .map(|demur| Seal::Active(demur)),
        }
    }

    fn get_active_seal(
        &mut self,
        chunk_hash: &ChunkHash,
        parent_hash: &CryptoHash,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<&mut ActiveSealDemur, near_chain::Error> {
        match self.active_demurs.entry(chunk_hash.clone()) {
            hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            hash_map::Entry::Vacant(entry) => {
                let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash)?;
                let chunk_producer =
                    self.runtime_adapter.get_chunk_producer(&epoch_id, height, shard_id)?;
                let candidates = {
                    let n = self.runtime_adapter.num_total_parts();
                    // `n` is an upper bound for elements in the accumulator; declaring with
                    // this capacity up front will mean no further allocations will occur
                    // from `push` calls in the loop.
                    let mut accumulator = Vec::with_capacity(n);

                    for part_ord in 0..n {
                        let part_ord = part_ord as u64;
                        let part_owner =
                            self.runtime_adapter.get_part_owner(&epoch_id, part_ord)?;
                        if part_owner == chunk_producer || Some(&part_owner) == self.me.as_ref() {
                            continue;
                        }
                        accumulator.push(part_ord);
                    }

                    accumulator
                };

                let chosen = Self::get_random_part_ords(candidates);
                let demur =
                    ActiveSealDemur { part_ords: chosen, chunk_producer, sent: Utc::now(), height };

                Ok(entry.insert(demur))
            }
        }
    }

    fn get_random_part_ords(candidates: Vec<u64>) -> HashSet<u64> {
        candidates
            .choose_multiple(
                &mut rand::thread_rng(),
                cmp::min(NUM_PARTS_REQUESTED_IN_SEAL, candidates.len()),
            )
            .cloned()
            .collect()
    }

    fn approve_chunk(&mut self, height: BlockHeight, chunk_hash: &ChunkHash) {
        let maybe_seal = self.active_demurs.remove(chunk_hash);
        match maybe_seal {
            None => match self.past_seals.entry(height) {
                btree_map::Entry::Vacant(vacant) => {
                    // TODO(#3180): seals are disabled in single shard setting
                    /*warn!(
                        target: "chunks",
                        "A chunk at height {} with hash {:?} was approved without an active seal demur and no past seals were found at the same height",
                        height,
                        chunk_hash
                    );*/
                    let mut hashes = HashSet::new();
                    hashes.insert(chunk_hash.clone());
                    vacant.insert(hashes);
                }
                btree_map::Entry::Occupied(mut occupied) => {
                    let hashes = occupied.get_mut();
                    if !hashes.contains(chunk_hash) {
                        // TODO(#3180): seals are disabled in single shard setting
                        /*warn!(
                            target: "chunks",
                            "Approved chunk at height {} with hash {:?} was not an active seal demur or a past seal",
                            height,
                            chunk_hash
                        );*/
                        hashes.insert(chunk_hash.clone());
                    }
                }
            },
            Some(seal) => {
                Self::insert_past_seal(&mut self.past_seals, seal.height, chunk_hash.clone());
            }
        }
    }

    fn insert_past_seal(
        past_seals: &mut BTreeMap<BlockHeight, HashSet<ChunkHash>>,
        height: BlockHeight,
        chunk_hash: ChunkHash,
    ) {
        let hashes_at_height = past_seals.entry(height).or_insert_with(HashSet::new);
        hashes_at_height.insert(chunk_hash);
    }

    // TODO(#3180): seals are disabled in single shard setting
    /*fn prune_past_seals(&mut self) {
        let maybe_height_limits = {
            let mut heights = self.past_seals.keys();
            heights.next().and_then(|least_height| {
                heights.next_back().map(|greatest_height| (*least_height, *greatest_height))
            })
        };

        if let Some((least_height, greatest_height)) = maybe_height_limits {
            let min_keep_height = greatest_height.saturating_sub(PAST_SEAL_HEIGHT_HORIZON);
            if least_height < min_keep_height {
                let remaining_seals = self.past_seals.split_off(&min_keep_height);
                self.past_seals = remaining_seals;
            }
        }
    }*/

    // TODO(#3180): seals are disabled in single shard setting
    /*fn track_seals(&mut self) {
        let now = Utc::now();
        let me = &self.me;
        let dont_include_chunks_from = &mut self.dont_include_chunks_from;
        let past_seals = &mut self.past_seals;

        self.active_demurs.retain(|chunk_hash, seal| {
            let accepting_period_over = (now - seal.sent).num_milliseconds() > ACCEPTING_SEAL_PERIOD_MS;
            let parts_remain = seal.part_ords.len() > NUM_PARTS_LEFT_IN_SEAL;

            // note chunk producers that failed to make parts available
            if parts_remain && accepting_period_over {
                warn!(target: "client", "Couldn't reconstruct chunk {:?} from {:?}, I'm {:?}", chunk_hash, seal.chunk_producer, me);
                dont_include_chunks_from.cache_set(seal.chunk_producer.clone(), ());
                Self::insert_past_seal(past_seals, seal.height, chunk_hash.clone());

                // Do not retain this demur, it has expired
                false
            } else {
                true
            }
        });

        self.prune_past_seals();
    }*/

    fn should_trust_chunk_producer(&mut self, chunk_producer: &AccountId) -> bool {
        self.dont_include_chunks_from.get(chunk_producer).is_none()
    }
}

pub struct ShardsManager {
    me: Option<AccountId>,
    store: ReadOnlyChunksStore,

    runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter>,
    peer_manager_adapter: Arc<dyn PeerManagerAdapter>,
    client_adapter: Arc<dyn ClientAdapterForShardsManager>,
    rs: ReedSolomonWrapper,

    encoded_chunks: EncodedChunksCache,
    requested_partial_encoded_chunks: RequestPool,
    chunk_forwards_cache: lru::LruCache<ChunkHash, HashMap<u64, PartialEncodedChunkPart>>,

    // This is a best-effort cache of the chain's head, not the source of truth. The source
    // of truth is in the chain store and written to by the Client.
    chain_head: Tip,
    // Similarly, this is the best-effort cache of the chain's header_head. This is used to
    // determine how old a chunk is. We don't use the chain_head for that, because if we just
    // ran header sync and are downloading the chunks for older blocks, head is behind while
    // header_head is new, but we would only know that the older chunks are old because
    // header_head is much newer.
    chain_header_head: Tip,

    seals_mgr: SealsManager,
}

impl ShardsManager {
    pub fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter>,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        client_adapter: Arc<dyn ClientAdapterForShardsManager>,
        store: ReadOnlyChunksStore,
        initial_chain_head: Tip,
        initial_chain_header_head: Tip,
    ) -> Self {
        Self {
            me: me.clone(),
            store,
            runtime_adapter: runtime_adapter.clone(),
            peer_manager_adapter: network_adapter,
            client_adapter,
            rs: ReedSolomonWrapper::new(
                runtime_adapter.num_data_parts(),
                runtime_adapter.num_total_parts() - runtime_adapter.num_data_parts(),
            ),
            encoded_chunks: EncodedChunksCache::new(),
            requested_partial_encoded_chunks: RequestPool::new(
                Duration::from_millis(CHUNK_REQUEST_RETRY_MS),
                Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_OTHERS_MS),
                Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS),
                Duration::from_millis(CHUNK_REQUEST_RETRY_MAX_MS),
            ),
            chunk_forwards_cache: lru::LruCache::new(CHUNK_FORWARD_CACHE_SIZE),
            chain_head: initial_chain_head,
            chain_header_head: initial_chain_header_head,
            seals_mgr: SealsManager::new(me, runtime_adapter),
        }
    }

    pub fn update_chain_heads(&mut self, head: Tip, header_head: Tip) {
        self.encoded_chunks.update_largest_seen_height(
            head.height,
            &self.requested_partial_encoded_chunks.requests,
        );
        self.chain_head = head;
        self.chain_header_head = header_head;
    }

    fn request_partial_encoded_chunk(
        &mut self,
        height: BlockHeight,
        ancestor_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_hash: &ChunkHash,
        force_request_full: bool,
        request_own_parts_from_others: bool,
        request_from_archival: bool,
    ) -> Result<(), near_chain::Error> {
        let _span = tracing::debug_span!(
            target: "chunks",
            "request_partial_encoded_chunk",
            ?chunk_hash,
            ?height,
            ?shard_id,
            ?request_from_archival)
        .entered();
        let mut bp_to_parts = HashMap::<_, Vec<u64>>::new();

        let cache_entry = self.encoded_chunks.get(chunk_hash);

        let request_full = force_request_full
            || cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                ancestor_hash,
                shard_id,
                true,
                self.runtime_adapter.as_ref(),
            );

        let chunk_producer_account_id = &self.runtime_adapter.get_chunk_producer(
            &self.runtime_adapter.get_epoch_id_from_prev_block(ancestor_hash)?,
            height,
            shard_id,
        )?;

        // In the following we compute which target accounts we should request parts and receipts from
        // First we choose a shard representative target which is either the original chunk producer
        // or a random block producer tracks the shard.
        // If request_from_archival is true (indicating we are requesting a chunk not from the current
        // or the last epoch), request all parts and receipts from the shard representative target
        // For each part, if we are the part owner, we request the part from the shard representative
        // target, otherwise, the part owner
        // For receipts, request them from the shard representative target
        //
        // Also note that the target accounts decided is not necessarily the final destination
        // where requests are sent. We use them to construct AccountIdOrPeerTrackingShard struct,
        // which will be passed to PeerManagerActor. PeerManagerActor will try to request either
        // from the target account or any eligible peer of the node (See comments in
        // AccountIdOrPeerTrackingShard for when target account is used or peer is used)

        let me = self.me.as_ref();
        // A account that is either the original chunk producer or a random block producer tracking
        // the shard
        let shard_representative_target = if !request_own_parts_from_others
            && !request_from_archival
            && Some(chunk_producer_account_id) != me
        {
            Some(chunk_producer_account_id.clone())
        } else {
            self.get_random_target_tracking_shard(ancestor_hash, shard_id)?
        };

        let seal = self.seals_mgr.get_seal(chunk_hash, ancestor_hash, height, shard_id)?;
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(ancestor_hash)?;

        for part_ord in 0..self.rs.total_shard_count() {
            let part_ord = part_ord as u64;
            if cache_entry.map_or(false, |cache_entry| cache_entry.parts.contains_key(&part_ord)) {
                continue;
            }

            // Note: If request_from_archival is true, we potentially call
            // get_part_owner unnecessarily.  It’s probably not worth optimising
            // though unless you can think of a concise way to do it.
            let part_owner = self.runtime_adapter.get_part_owner(&epoch_id, part_ord)?;
            let we_own_part = Some(&part_owner) == me;
            if !request_full && !we_own_part && !seal.contains_part_ord(&part_ord) {
                continue;
            }

            // This is false positive, similar to what was reported here:
            // https://github.com/rust-lang/rust-clippy/issues/5940
            #[allow(clippy::if_same_then_else)]
            let fetch_from = if request_from_archival {
                shard_representative_target.clone()
            } else if we_own_part {
                // If missing own part, request it from the chunk producer / node tracking shard
                shard_representative_target.clone()
            } else {
                Some(part_owner)
            };

            bp_to_parts.entry(fetch_from).or_default().push(part_ord);
        }

        let shards_to_fetch_receipts =
        // TODO: only keep shards for which we don't have receipts yet
            if request_full { HashSet::new() } else { self.get_tracking_shards(ancestor_hash) };

        // The loop below will be sending PartialEncodedChunkRequestMsg to various block producers.
        // We need to send such a message to the original chunk producer if we do not have the receipts
        //     for some subset of shards, even if we don't need to request any parts from the original
        //     chunk producer.
        if !shards_to_fetch_receipts.is_empty() {
            bp_to_parts.entry(shard_representative_target.clone()).or_default();
        }

        let no_account_id = me.is_none();
        debug!(target: "chunks", "Will send {} requests to fetch chunk parts.", bp_to_parts.len());
        for (target_account, part_ords) in bp_to_parts {
            // extra check that we are not sending request to ourselves.
            if no_account_id || me != target_account.as_ref() {
                let prefer_peer = request_from_archival || rand::thread_rng().gen::<bool>();
                debug!(
                    target: "chunks",
                    ?part_ords,
                    shard_id,
                    ?target_account,
                    prefer_peer,
                    "Requesting parts",
                );

                let request = PartialEncodedChunkRequestMsg {
                    chunk_hash: chunk_hash.clone(),
                    part_ords,
                    tracking_shards: if target_account == shard_representative_target {
                        shards_to_fetch_receipts.clone()
                    } else {
                        HashSet::new()
                    },
                };
                let target = AccountIdOrPeerTrackingShard {
                    account_id: target_account,
                    prefer_peer,
                    shard_id,
                    only_archival: request_from_archival,
                    min_height: height.saturating_sub(CHUNK_REQUEST_PEER_HORIZON),
                };

                self.peer_manager_adapter.do_send(
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedChunkRequest {
                            target,
                            request,
                            create_time: Clock::instant().into(),
                        },
                    )
                    .with_span_context(),
                );
            } else {
                warn!(target: "client", "{:?} requests parts {:?} for chunk {:?} from self",
                    me, part_ords, chunk_hash
                );
            }
        }

        Ok(())
    }

    /// Get a random shard block producer that is not me.
    fn get_random_target_tracking_shard(
        &self,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<AccountId>, near_chain::Error> {
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash).unwrap();
        let block_producers = self
            .runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_id, parent_hash)?
            .into_iter()
            .filter_map(|(validator_stake, is_slashed)| {
                let account_id = validator_stake.take_account_id();
                if !is_slashed
                    && cares_about_shard_this_or_next_epoch(
                        Some(&account_id),
                        parent_hash,
                        shard_id,
                        false,
                        self.runtime_adapter.as_ref(),
                    )
                    && self.me.as_ref() != Some(&account_id)
                {
                    Some(account_id)
                } else {
                    None
                }
            });

        Ok(block_producers.choose(&mut rand::thread_rng()))
    }

    fn get_tracking_shards(&self, parent_hash: &CryptoHash) -> HashSet<ShardId> {
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash).unwrap();
        (0..self.runtime_adapter.num_shards(&epoch_id).unwrap())
            .filter(|chunk_shard_id| {
                cares_about_shard_this_or_next_epoch(
                    self.me.as_ref(),
                    parent_hash,
                    *chunk_shard_id,
                    true,
                    self.runtime_adapter.as_ref(),
                )
            })
            .collect::<HashSet<_>>()
    }

    /// Check whether the node should wait for chunk parts being forwarded to it
    /// The node will wait if it's a block producer or a chunk producer that is responsible
    /// for producing the next chunk in this shard.
    /// `prev_hash`: previous block hash of the chunk that we are requesting
    /// `next_chunk_height`: height of the next chunk of the chunk that we are requesting
    fn should_wait_for_chunk_forwarding(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
        next_chunk_height: BlockHeight,
    ) -> Result<bool, Error> {
        // chunks will not be forwarded to non-validators
        let me = match self.me.as_ref() {
            None => return Ok(false),
            Some(it) => it,
        };
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(prev_hash)?;
        let block_producers =
            self.runtime_adapter.get_epoch_block_producers_ordered(&epoch_id, prev_hash)?;
        for (bp, _) in block_producers {
            if bp.account_id() == me {
                return Ok(true);
            }
        }
        let chunk_producer =
            self.runtime_adapter.get_chunk_producer(&epoch_id, next_chunk_height, shard_id)?;
        if &chunk_producer == me {
            return Ok(true);
        }
        Ok(false)
    }

    /// Only marks this chunk as being requested
    /// Note no requests are actually sent at this point.
    fn request_chunk_single_mark_only(&mut self, chunk_header: &ShardChunkHeader) {
        self.request_chunk_single(chunk_header, chunk_header.prev_block_hash().clone(), true)
    }

    /// send partial chunk requests for one chunk
    /// `chunk_header`: the chunk being requested
    /// `ancestor_hash`: hash of an ancestor block of the requested chunk.
    ///                  It must satisfy
    ///                  1) it is from the same epoch than `epoch_id`
    ///                  2) it is processed
    ///                  If the above conditions are not met, the request will be dropped
    /// `mark_only`: if true, only add the request to the pool, but do not send it.
    fn request_chunk_single(
        &mut self,
        chunk_header: &ShardChunkHeader,
        ancestor_hash: CryptoHash,
        mark_only: bool,
    ) {
        let height = chunk_header.height_created();
        let shard_id = chunk_header.shard_id();
        let chunk_hash = chunk_header.chunk_hash();

        if self.requested_partial_encoded_chunks.contains_key(&chunk_hash) {
            return;
        }

        debug!(target: "chunks", height, shard_id, ?chunk_hash, "Requesting.");

        if let Some(entry) = self.encoded_chunks.get(&chunk_header.chunk_hash()) {
            if entry.complete {
                return;
            }
        } else {
            // In all code paths that lead to this function, we have already inserted the header.
            // However, if the chunk had just been processed and marked as complete, it might have
            // been removed from the cache if it is out of horizon. So in this case, the chunk is
            // already complete and we don't need to request anything.
            return;
        }

        let prev_block_hash = chunk_header.prev_block_hash().clone();
        self.requested_partial_encoded_chunks.insert(
            chunk_hash.clone(),
            ChunkRequestInfo {
                height,
                prev_block_hash,
                ancestor_hash,
                shard_id,
                last_requested: Clock::instant(),
                added: Clock::instant(),
            },
        );

        if !mark_only {
            let fetch_from_archival = self.runtime_adapter
                .chunk_needs_to_be_fetched_from_archival(&ancestor_hash, &self.chain_header_head.last_block_hash).unwrap_or_else(|err| {
                error!(target: "chunks", "Error during requesting partial encoded chunk. Cannot determine whether to request from an archival node, defaulting to not: {}", err);
                false
            });
            let old_block = self.chain_header_head.last_block_hash != prev_block_hash
                && self.chain_header_head.prev_block_hash != prev_block_hash;

            let should_wait_for_chunk_forwarding =
                self.should_wait_for_chunk_forwarding(&ancestor_hash, chunk_header.shard_id(), chunk_header.height_created()+1).unwrap_or_else(|_| {
                    // ancestor_hash must be accepted because we don't request missing chunks through this
                    // this function for orphans
                    debug_assert!(false, "{:?} must be accepted", ancestor_hash);
                    error!(target:"chunks", "requesting chunk whose ancestor_hash {:?} is not accepted", ancestor_hash);
                    false
                });

            // If chunks forwarding is enabled,
            // we purposely do not send chunk request messages right away for new blocks. Such requests
            // will eventually be sent because of the `resend_chunk_requests` loop. However,
            // we want to give some time for any `PartialEncodedChunkForward` messages to arrive
            // before we send requests.
            if !should_wait_for_chunk_forwarding || fetch_from_archival || old_block {
                let request_result = self.request_partial_encoded_chunk(
                    height,
                    &ancestor_hash,
                    shard_id,
                    &chunk_hash,
                    false,
                    old_block,
                    fetch_from_archival,
                );
                if let Err(err) = request_result {
                    error!(target: "chunks", "Error during requesting partial encoded chunk: {}", err);
                }
            } else {
                debug!(target: "chunks",should_wait_for_chunk_forwarding, fetch_from_archival, old_block,  "Delaying the chunk request.");
            }
        }
    }

    /// send chunk requests for some chunks in a block
    /// `chunks_to_request`: chunks to request
    /// `prev_hash`: hash of prev block of the block we are requesting missing chunks for
    ///              The function assumes the prev block is accepted
    pub fn request_chunks(
        &mut self,
        chunks_to_request: Vec<ShardChunkHeader>,
        prev_hash: CryptoHash,
    ) {
        for chunk_header in chunks_to_request {
            self.request_chunk_single(&chunk_header, prev_hash, false);
        }
    }

    /// Request chunks for an orphan block.
    /// `epoch_id`: epoch_id of the orphan block
    /// `ancestor_hash`: because BlockInfo for the immediate parent of an orphan block is not ready,
    ///                we use hash of an ancestor block of this orphan to request chunks. It must
    ///                satisfy
    ///                1) it is from the same epoch than `epoch_id`
    ///                2) it is processed
    ///                If the above conditions are not met, the request will be dropped
    pub fn request_chunks_for_orphan(
        &mut self,
        chunks_to_request: Vec<ShardChunkHeader>,
        epoch_id: &EpochId,
        ancestor_hash: CryptoHash,
    ) {
        let ancestor_epoch_id =
            unwrap_or_return!(self.runtime_adapter.get_epoch_id_from_prev_block(&ancestor_hash));
        if epoch_id != &ancestor_epoch_id {
            return;
        }

        for chunk_header in chunks_to_request {
            self.request_chunk_single(&chunk_header, ancestor_hash, false)
        }
    }

    /// Resends chunk requests if haven't received it within expected time.
    pub fn resend_chunk_requests(&mut self) {
        let _span = tracing::debug_span!(
            target: "client",
            "resend_chunk_requests",
            header_head_height = self.chain_header_head.height,
            pool_size = self.requested_partial_encoded_chunks.len())
        .entered();
        // Process chunk one part requests.
        let requests = self.requested_partial_encoded_chunks.fetch();
        for (chunk_hash, chunk_request) in requests {
            let fetch_from_archival = self.runtime_adapter
                .chunk_needs_to_be_fetched_from_archival(&chunk_request.ancestor_hash, &self.chain_header_head.last_block_hash).unwrap_or_else(|err| {
                debug_assert!(false);
                error!(target: "chunks", "Error during re-requesting partial encoded chunk. Cannot determine whether to request from an archival node, defaulting to not: {}", err);
                false
            });
            let old_block = self.chain_header_head.last_block_hash != chunk_request.prev_block_hash
                && self.chain_header_head.prev_block_hash != chunk_request.prev_block_hash;

            match self.request_partial_encoded_chunk(
                chunk_request.height,
                &chunk_request.ancestor_hash,
                chunk_request.shard_id,
                &chunk_hash,
                chunk_request.added.elapsed()
                    > self.requested_partial_encoded_chunks.switch_to_full_fetch_duration,
                old_block
                    || chunk_request.added.elapsed()
                        > self.requested_partial_encoded_chunks.switch_to_others_duration,
                fetch_from_archival,
            ) {
                Ok(()) => {}
                Err(err) => {
                    debug_assert!(false);
                    error!(target: "chunks", "Error during requesting partial encoded chunk: {}", err);
                }
            }
        }
    }

    pub fn receipts_recipient_filter<T>(
        from_shard_id: ShardId,
        tracking_shards: T,
        receipts_by_shard: &HashMap<ShardId, Vec<Receipt>>,
        proofs: &[MerklePath],
    ) -> Vec<ReceiptProof>
    where
        T: IntoIterator<Item = ShardId>,
    {
        tracking_shards
            .into_iter()
            .map(|to_shard_id| {
                let receipts =
                    receipts_by_shard.get(&to_shard_id).cloned().unwrap_or_else(Vec::new);
                let shard_proof = ShardProof {
                    from_shard_id,
                    to_shard_id,
                    proof: proofs[to_shard_id as usize].clone(),
                };
                ReceiptProof(receipts, shard_proof)
            })
            .collect()
    }

    pub fn process_partial_encoded_chunk_request(
        &mut self,
        request: PartialEncodedChunkRequestMsg,
        route_back: CryptoHash,
    ) {
        let _span = tracing::debug_span!(
            target: "chunks",
            "process_partial_encoded_chunk_request",
            chunk_hash = %request.chunk_hash.0)
        .entered();
        debug!(target: "chunks",
            chunk_hash = %request.chunk_hash.0,
            part_ords = ?request.part_ords,
            shards = ?request.tracking_shards,
            account = ?self.me.as_ref());

        let started = Instant::now();
        let (source, response) = self.prepare_partial_encoded_chunk_response(request);
        let elapsed = started.elapsed().as_secs_f64();
        let labels = [
            source.name_for_metrics(),
            if response.parts.is_empty() && response.receipts.is_empty() { "failed" } else { "ok" },
        ];
        metrics::PARTIAL_ENCODED_CHUNK_REQUEST_PROCESSING_TIME
            .with_label_values(&labels)
            .observe(elapsed);

        self.peer_manager_adapter.do_send(
            PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedChunkResponse { route_back, response },
            )
            .with_span_context(),
        );
    }

    /// Finds the parts and receipt proofs asked for in the request, and returns a response
    /// containing whatever was found. See comment for PartialEncodedChunkResponseSource for
    /// an explanation of that part of the return value.
    fn prepare_partial_encoded_chunk_response(
        &mut self,
        request: PartialEncodedChunkRequestMsg,
    ) -> (PartialEncodedChunkResponseSource, PartialEncodedChunkResponseMsg) {
        let PartialEncodedChunkRequestMsg { chunk_hash, part_ords, mut tracking_shards } = request;
        let mut response = PartialEncodedChunkResponseMsg {
            chunk_hash: chunk_hash.clone(),
            parts: vec![],
            receipts: vec![],
        };
        let mut part_ords = part_ords.into_iter().collect::<HashSet<_>>();
        if part_ords.is_empty() && tracking_shards.is_empty() {
            // Don't bother looking up anything if none was requested at all.
            return (PartialEncodedChunkResponseSource::None, response);
        }
        // Try getting data from in-memory cache.
        if let Some(entry) = self.encoded_chunks.get(&chunk_hash) {
            Self::lookup_partial_encoded_chunk_from_cache(
                &mut part_ords,
                &mut tracking_shards,
                &mut response,
                entry,
            );
        }
        if part_ords.is_empty() && tracking_shards.is_empty() {
            // If we found all parts and receipts, return now.
            return (PartialEncodedChunkResponseSource::InMemoryCache, response);
        }

        // Try fetching partial encoded chunk from storage.
        if let Ok(partial_chunk) = self.store.get_partial_chunk(&chunk_hash) {
            Self::lookup_partial_encoded_chunk_from_partial_chunk_storage(
                part_ords,
                tracking_shards,
                &mut response,
                &partial_chunk,
            );
            // If we have found the partial chunk, then the shard chunk would not have
            // anything extra (since we populate the shard chunk iff we track the shard
            // but if that's the case we would've populated all parts and receipts to
            // the partial chunk as well).
            return (PartialEncodedChunkResponseSource::PartialChunkOnDisk, response);
        }

        // If we are an archival node we might have garbage collected the
        // partial chunk while we still keep the full chunk. We can get the
        // chunk, recalculate the parts and respond to the request.
        if let Ok(chunk) = self.store.get_chunk(&chunk_hash) {
            self.lookup_partial_encoded_chunk_from_chunk_storage(
                part_ords,
                tracking_shards,
                &mut response,
                &chunk,
            );
        }

        // Note that we return this source even if we didn't find anything on disk,
        // because we still spent the time to look for it on disk.
        (PartialEncodedChunkResponseSource::ShardChunkOnDisk, response)
    }

    /// Looks up the given part_ords and tracking_shards from the cache, appending
    /// any we have found into the response, and deleting those we have found from
    /// part_ords and tracking_shards.
    fn lookup_partial_encoded_chunk_from_cache(
        part_ords: &mut HashSet<u64>,
        tracking_shards: &mut HashSet<ShardId>,
        response: &mut PartialEncodedChunkResponseMsg,
        entry: &EncodedChunksCacheEntry,
    ) {
        part_ords.retain(|part_ord| {
            if let Some(part) = entry.parts.get(part_ord) {
                response.parts.push(part.clone());
                false
            } else {
                true
            }
        });
        tracking_shards.retain(|shard_id| {
            if let Some(receipt_proof) = entry.receipts.get(shard_id) {
                response.receipts.push(receipt_proof.clone());
                false
            } else {
                true
            }
        });
    }

    /// Looks up the given part_ords and tracking_shards from the partial chunks
    /// storage, appending any we have found into the response, and deleting those we
    /// have found from part_ords and tracking_shards.
    pub fn lookup_partial_encoded_chunk_from_partial_chunk_storage(
        part_ords: HashSet<u64>,
        tracking_shards: HashSet<ShardId>,
        response: &mut PartialEncodedChunkResponseMsg,
        partial_chunk: &PartialEncodedChunk,
    ) {
        for part in partial_chunk.parts() {
            if part_ords.contains(&part.part_ord) {
                response.parts.push(part.clone());
            }
        }
        for receipt in partial_chunk.receipts() {
            if tracking_shards.contains(&receipt.1.to_shard_id) {
                response.receipts.push(receipt.clone());
            }
        }
    }

    /// Looks up the given part_ords and tracking_shards from the cache, appending
    /// any we have found into the response.
    ///
    /// This requires encoding the chunk and as such is computationally
    /// expensive operation.  If possible, the request should be served from
    /// EncodedChunksCacheEntry or PartialEncodedChunk instead.
    // pub for testing
    pub fn lookup_partial_encoded_chunk_from_chunk_storage(
        &mut self,
        part_ords: HashSet<u64>,
        tracking_shards: HashSet<ShardId>,
        response: &mut PartialEncodedChunkResponseMsg,
        chunk: &ShardChunk,
    ) {
        let header = chunk.cloned_header();

        // Get outgoing receipts for the chunk and construct vector of their
        // proofs.
        let outgoing_receipts = chunk.receipts();
        let present_receipts: HashMap<ShardId, _> = match make_outgoing_receipts_proofs(
            &header,
            &outgoing_receipts,
            self.runtime_adapter.as_ref(),
        ) {
            Ok(receipts) => receipts.map(|receipt| (receipt.1.to_shard_id, receipt)).collect(),
            Err(e) => {
                warn!(target: "chunks", "Not sending {:?}, failed to make outgoing receipts proofs: {}", chunk.chunk_hash(), e);
                return;
            }
        };

        // Construct EncodedShardChunk.  If we earlier determined that we will
        // need parity parts, instruct the constructor to calculate them as
        // well.  Otherwise we won’t bother.
        let (parts, encoded_length) = match EncodedShardChunk::encode_transaction_receipts(
            &mut self.rs,
            chunk.transactions().to_vec(),
            &outgoing_receipts,
        ) {
            Ok(result) => result,
            Err(err) => {
                warn!(target: "chunks",
                       "Not sending {:?}, failed to encode transactions and receipts: {}",
                       chunk.chunk_hash(), err);
                return;
            }
        };
        if header.encoded_length() != encoded_length {
            warn!(target: "chunks",
                   "Not sending {:?}, expected encoded length doesn’t match calculated: {} != {}",
                   chunk.chunk_hash(), header.encoded_length(), encoded_length);
            return;
        }

        let mut content = EncodedShardChunkBody { parts };
        if let Err(err) = content.reconstruct(&mut self.rs) {
            warn!(target: "chunks",
                   "Not sending {:?}, failed to reconstruct RS parity parts: {}",
                   chunk.chunk_hash(), err);
            return;
        }

        let (encoded_merkle_root, merkle_paths) = content.get_merkle_hash_and_paths();
        if header.encoded_merkle_root() != encoded_merkle_root {
            warn!(target: "chunks",
                   "Not sending {:?}, expected encoded Merkle root doesn’t match calculated: {} != {}",
                   chunk.chunk_hash(), header.encoded_merkle_root(), encoded_merkle_root);
            return;
        }
        if merkle_paths.len() != content.parts.len() {
            warn!(target: "chunks",
                   "Not sending {:?}, expected number of Merkle paths doesn’t match calculated: {} != {}",
                   chunk.chunk_hash(), merkle_paths.len(), content.parts.len());
            return;
        }
        for (part_ord, (part, merkle_proof)) in
            content.parts.into_iter().zip(merkle_paths.into_iter()).enumerate()
        {
            if let Some(part) = part {
                if part_ords.contains(&(part_ord as u64)) {
                    response.parts.push(PartialEncodedChunkPart {
                        part_ord: part_ord as u64,
                        part,
                        merkle_proof,
                    });
                }
            }
        }
        for (shard_id, receipt_proof) in present_receipts {
            if tracking_shards.contains(&shard_id) {
                response.receipts.push(receipt_proof);
            }
        }
    }

    // pub for testing
    pub fn check_chunk_complete(
        chunk: &mut EncodedShardChunk,
        rs: &mut ReedSolomonWrapper,
    ) -> ChunkStatus {
        let data_parts = rs.data_shard_count();
        if chunk.content().num_fetched_parts() >= data_parts {
            if let Ok(_) = chunk.content_mut().reconstruct(rs) {
                let (merkle_root, merkle_paths) = chunk.content().get_merkle_hash_and_paths();
                if merkle_root == chunk.encoded_merkle_root() {
                    ChunkStatus::Complete(merkle_paths)
                } else {
                    ChunkStatus::Invalid
                }
            } else {
                ChunkStatus::Invalid
            }
        } else {
            ChunkStatus::Incomplete
        }
    }

    /// Add a part to current encoded chunk stored in memory. It's present only if One Part was present and signed correctly.
    fn validate_part(
        &mut self,
        merkle_root: MerkleHash,
        part: &PartialEncodedChunkPart,
        num_total_parts: usize,
    ) -> Result<(), Error> {
        if (part.part_ord as usize) < num_total_parts {
            if !verify_path(merkle_root, &part.merkle_proof, &part.part) {
                return Err(Error::InvalidMerkleProof);
            }

            Ok(())
        } else {
            Err(Error::InvalidChunkPartId)
        }
    }

    fn decode_encoded_chunk_if_complete(
        &mut self,
        mut encoded_chunk: EncodedShardChunk,
    ) -> Result<Option<(ShardChunk, PartialEncodedChunk)>, Error> {
        match ShardsManager::check_chunk_complete(&mut encoded_chunk, &mut self.rs) {
            ChunkStatus::Complete(merkle_paths) => {
                self.requested_partial_encoded_chunks.remove(&encoded_chunk.chunk_hash());
                match decode_encoded_chunk(
                    &encoded_chunk,
                    merkle_paths,
                    self.me.as_ref(),
                    self.runtime_adapter.as_ref(),
                ) {
                    Ok(chunk) => Ok(Some(chunk)),
                    Err(err) => {
                        self.encoded_chunks.remove(&encoded_chunk.chunk_hash());
                        Err(err)
                    }
                }
            }
            ChunkStatus::Incomplete => Ok(None),
            ChunkStatus::Invalid => {
                let chunk_hash = encoded_chunk.chunk_hash();
                self.encoded_chunks.remove(&chunk_hash);
                Err(Error::InvalidChunk)
            }
        }
    }

    fn validate_partial_encoded_chunk_forward(
        &mut self,
        forward: &PartialEncodedChunkForwardMsg,
    ) -> Result<(), Error> {
        let valid_hash = forward.is_valid_hash(); // check hash

        if !valid_hash {
            return Err(Error::InvalidPartMessage);
        }

        // check part merkle proofs
        let num_total_parts = self.rs.total_shard_count();
        for part_info in forward.parts.iter() {
            self.validate_part(forward.merkle_root, part_info, num_total_parts)?;
        }

        // check signature
        let epoch_id =
            self.runtime_adapter.get_epoch_id_from_prev_block(&forward.prev_block_hash)?;
        let valid_signature = self.runtime_adapter.verify_chunk_signature_with_header_parts(
            &forward.chunk_hash,
            &forward.signature,
            &epoch_id,
            &forward.prev_block_hash,
            forward.height_created,
            forward.shard_id,
        )?;

        if !valid_signature {
            return Err(Error::InvalidChunkSignature);
        }

        Ok(())
    }

    /// Gets the header associated with the chunk hash from the `encoded_chunks` cache.
    /// An error is returned if the chunk is not present or the hash in the associated
    /// header does not match the given hash.
    fn get_partial_encoded_chunk_header(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<ShardChunkHeader, Error> {
        let header = self
            .encoded_chunks
            .get(chunk_hash)
            .map(|encoded_chunk| encoded_chunk.header.clone())
            .ok_or(Error::UnknownChunk)?;

        // Check the hashes match
        if header.chunk_hash() != *chunk_hash {
            byzantine_assert!(false);
            return Err(Error::InvalidChunkHeader);
        }

        Ok(header)
    }

    fn insert_forwarded_chunk(&mut self, forward: PartialEncodedChunkForwardMsg) {
        let chunk_hash = forward.chunk_hash.clone();
        let num_total_parts = self.rs.total_shard_count() as u64;
        match self.chunk_forwards_cache.get_mut(&chunk_hash) {
            None => {
                // Never seen this chunk hash before, collect the parts and cache them
                let parts = forward.parts.into_iter().filter_map(|part| {
                    let part_ord = part.part_ord;
                    if part_ord > num_total_parts {
                        warn!(target: "chunks", "Received chunk part with part_ord greater than the the total number of chunks");
                        None
                    } else {
                        Some((part_ord, part))
                    }
                }).collect();
                self.chunk_forwards_cache.put(chunk_hash, parts);
            }

            Some(existing_parts) => {
                for part in forward.parts {
                    let part_ord = part.part_ord;
                    if part_ord > num_total_parts {
                        warn!(target: "chunks", "Received chunk part with part_ord greater than the the total number of chunks");
                        continue;
                    }
                    existing_parts.insert(part_ord, part);
                }
            }
        }
    }

    pub fn process_partial_encoded_chunk_forward(
        &mut self,
        forward: PartialEncodedChunkForwardMsg,
    ) -> Result<(), Error> {
        let maybe_header = self
            .validate_partial_encoded_chunk_forward(&forward)
            .and_then(|_| self.get_partial_encoded_chunk_header(&forward.chunk_hash));

        let header = match maybe_header {
            Ok(header) => Ok(header),
            Err(Error::UnknownChunk) => {
                // We don't know this chunk yet; cache the forwarded part
                // to be used after we get the header.
                self.insert_forwarded_chunk(forward);
                PARTIAL_ENCODED_CHUNK_FORWARD_CACHED_WITHOUT_HEADER.inc();
                return Err(Error::UnknownChunk);
            }
            Err(Error::ChainError(chain_error)) => {
                match chain_error {
                    near_chain::Error::DBNotFoundErr(_) => {
                        // We can't check if this chunk came from a valid chunk producer because
                        // we don't know `prev_block`, however the signature is checked when
                        // forwarded parts are later processed as partial encoded chunks, so we
                        // can mark it as unknown for now.
                        self.insert_forwarded_chunk(forward);
                        PARTIAL_ENCODED_CHUNK_FORWARD_CACHED_WITHOUT_HEADER.inc();
                        return Err(Error::UnknownChunk);
                    }
                    // Some other error occurred, we don't know how to handle it
                    _ => Err(Error::ChainError(chain_error)),
                }
            }
            Err(err) => Err(err),
        }?;
        let partial_chunk = PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header,
            parts: forward.parts,
            receipts: Vec::new(),
        });
        self.process_partial_encoded_chunk(MaybeValidated::from_validated(partial_chunk))?;
        Ok(())
    }

    /// Validate a chunk header
    /// 1) check that the chunk header is signed by the correct chunk producer for the chunk at
    ///    the height for the shard
    /// 2) check that the chunk header is compatible with the current protocol version
    ///
    // Note that this function only does partial validation. Full validation is only possible
    // after the previous block of the chunk is processed. To be able to process partial encoded
    // chunk messages in advance, this function tries to verify with other accepted block hash in
    // the chain if the previous block hash is not accepted. Validation is only partially done
    // in those cases. Full validation can be achieved by calling this function after
    // the previous block hash is accepted.
    //
    // To achieve full validation, this function is called twice for each chunk entry
    // first when the chunk entry is inserted in `encoded_chunks`
    // then in `process_partial_encoded_chunk` after checking the previous block is ready
    fn validate_chunk_header(&self, header: &ShardChunkHeader) -> Result<(), Error> {
        let chunk_hash = header.chunk_hash();
        // 1.  check signature
        // Ideally, validating the chunk header needs the previous block to be accepted already.
        // However, we want to be able to validate chunk header in advance so we can save
        // the corresponding parts and receipts before the previous block is processed
        // We do this three layered check
        // 1) if prev_block_hash is processed, we use that
        // 2) if we have sent request for the chunk, we know the `ancestor_hash` from the original
        //    request and we know that get_epoch_id_from_prev_block(ancestor_hash) =
        //    get_epoch_id_from_prev_block(prev_block_hash). Thus, we can calculate epoch_id
        //    from ancestor_hash
        // 3) otherwise, we use the current chain_head to calculate epoch id. In this case,
        //    we are not sure if we are using the correct epoch id, thus `epoch_id_confirmed` is false.
        //    And if the validation fails in this case, we actually can't say if the chunk is actually
        //    invalid. So we must return chain_error instead of return error
        let (ancestor_hash, epoch_id, epoch_id_confirmed) = {
            let prev_block_hash = header.prev_block_hash().clone();
            let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash);
            if let Ok(epoch_id) = epoch_id {
                (prev_block_hash, epoch_id, true)
            } else if let Some(request_info) =
                self.requested_partial_encoded_chunks.get_request_info(&chunk_hash)
            {
                let ancestor_hash = request_info.ancestor_hash;
                let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&ancestor_hash)?;
                (ancestor_hash, epoch_id, true)
            } else {
                // we can safely unwrap here because chain head must already be accepted
                let epoch_id = self
                    .runtime_adapter
                    .get_epoch_id_from_prev_block(&self.chain_head.last_block_hash)
                    .unwrap();
                (self.chain_head.last_block_hash, epoch_id, false)
            }
        };

        match self.runtime_adapter.verify_chunk_header_signature(header, &epoch_id, &ancestor_hash)
        {
            Ok(false) => {
                return if epoch_id_confirmed {
                    byzantine_assert!(false);
                    Err(Error::InvalidChunkSignature)
                } else {
                    // we are not sure if we are using the correct epoch id for validation, so
                    // we can't be sure if the chunk header is actually invalid. Let's return
                    // DbNotFoundError for now, which means we don't have all needed information yet
                    Err(DBNotFoundErr(format!("block {:?}", header.prev_block_hash())).into())
                };
            }
            Ok(true) => (),
            Err(chain_error) => {
                return Err(chain_error.into());
            }
        }

        // 2. check protocol version
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        if header.valid_for(protocol_version) {
            Ok(())
        } else if epoch_id_confirmed {
            Err(Error::InvalidChunkHeader)
        } else {
            Err(DBNotFoundErr(format!("block {:?}", header.prev_block_hash())).into())
        }
    }

    /// Inserts the header if it is not already known, and process the forwarded chunk parts cached
    /// for this chunk, if any. Returns true if the header was newly inserted or forwarded parts
    /// were newly processed.
    fn insert_header_if_not_exists_and_process_cached_chunk_forwards(
        &mut self,
        header: &ShardChunkHeader,
    ) -> bool {
        let header_known_before = self.encoded_chunks.get(&header.chunk_hash()).is_some();
        if self.encoded_chunks.get_or_insert_from_header(header).complete {
            return false;
        }
        if let Some(parts) = self.chunk_forwards_cache.pop(&header.chunk_hash()) {
            // Note that we don't need any further validation for the forwarded part.
            // The forwarded part was earlier validated via validate_partial_encoded_chunk_forward,
            // which checks the part against the merkle root in the forward message, and the merkle
            // root is checked against the chunk hash in the forward message, and that chunk hash
            // is used to identify the chunk. Furthermore, it's OK to directly use the header if
            // it is the first time we learn of the header here, because later when we call
            // try_process_chunk_parts_and_receipts, we will perform a header validation if we
            // didn't already.
            self.encoded_chunks.merge_in_partial_encoded_chunk(&PartialEncodedChunkV2 {
                header: header.clone(),
                parts: parts.into_values().collect(),
                receipts: vec![],
            });
            return true;
        }
        !header_known_before
    }

    /// Processes a partial encoded chunk message, which means
    /// 1) Checks that the partial encoded chunk message is valid, including checking
    ///    header, parts and receipts
    /// 2) If the chunk message is valid, save the parts and receipts in cache
    /// 3) Forwards newly received owned parts to other validators, if any
    /// 4) Processes forwarded chunk parts that haven't been processed yet
    /// 5) Checks if the chunk has all parts and receipts, if so and if the node cares about the shard,
    ///    decodes and persists the full chunk
    ///
    /// Params
    /// `partial_encoded_chunk`: the partial encoded chunk needs to be processed. `MaybeValidated`
    ///                          denotes whether the chunk header has been validated or not
    ///
    /// Returns
    ///  ProcessPartialEncodedChunkResult::Known: if all information in
    ///    the `partial_encoded_chunk` is already known and no further processing is needed
    ///  ProcessPartialEncodedChunkResult::NeedBlock: if the previous block is needed
    ///    to finish processing
    ///  ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts: if more parts and receipts
    ///    are needed for processing the full chunk
    ///  ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts: if all parts and
    ///    receipts in the chunk are received and the chunk has been processed.
    pub fn process_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: MaybeValidated<PartialEncodedChunk>,
    ) -> Result<ProcessPartialEncodedChunkResult, Error> {
        let partial_encoded_chunk =
            partial_encoded_chunk.map(|chunk| PartialEncodedChunkV2::from(chunk));
        let header = &partial_encoded_chunk.header;
        let chunk_hash = header.chunk_hash();
        debug!(
            target: "chunks",
            ?chunk_hash,
            height = header.height_created(),
            shard_id = header.shard_id(),
            parts = ?partial_encoded_chunk.get_inner().parts.iter().map(|p| p.part_ord).collect::<Vec<_>>(),
            "Process partial encoded chunk",
        );
        // Verify the partial encoded chunk is valid and worth processing
        // 1.a Leave if we received known chunk
        if let Some(entry) = self.encoded_chunks.get(&chunk_hash) {
            if entry.complete {
                return Ok(ProcessPartialEncodedChunkResult::Known);
            }
            debug!(target: "chunks", "{} parts in cache, total needed: {}", entry.parts.len(), self.rs.data_shard_count());
        } else {
            debug!(target: "chunks", "0 parts in cache, total needed: {}", self.rs.data_shard_count());
        }

        // 1.b Checking chunk height
        let chunk_requested = self.requested_partial_encoded_chunks.contains_key(&chunk_hash);
        if !chunk_requested {
            if !self.encoded_chunks.height_within_horizon(header.height_created()) {
                return Err(Error::ChainError(near_chain::Error::InvalidChunkHeight));
            }
            // We shouldn't process unrequested chunk if we have seen one with same (height_created + shard_id) but different chunk_hash
            if let Some(hash) = self
                .encoded_chunks
                .get_chunk_hash_by_height_and_shard(header.height_created(), header.shard_id())
            {
                if hash != &chunk_hash {
                    warn!(target: "client", "Rejecting unrequested chunk {:?}, height {}, shard_id {}, because of having {:?}", chunk_hash, header.height_created(), header.shard_id(), hash);
                    return Err(Error::DuplicateChunkHeight);
                }
            }
        }

        // 1.c checking header validity
        match partial_encoded_chunk
            .validate_with(|pec| self.validate_chunk_header(&pec.header).map(|()| true))
        {
            Err(Error::ChainError(chain_error)) => match chain_error {
                // validate_chunk_header returns DBNotFoundError if the previous block is not ready
                // in this case, we return NeedBlock instead of error
                near_chain::Error::DBNotFoundErr(_) => {
                    debug!(target:"client", "Dropping partial encoded chunk {:?} height {}, shard_id {} because we don't have enough information to validate it",
                           header.chunk_hash(), header.height_created(), header.shard_id());
                    return Ok(ProcessPartialEncodedChunkResult::NeedBlock);
                }
                _ => return Err(chain_error.into()),
            },
            Err(err) => return Err(err),
            Ok(_) => (),
        }
        let partial_encoded_chunk = partial_encoded_chunk.as_ref().into_inner();

        // 1.d Checking part_ords' validity
        let num_total_parts = self.rs.total_shard_count();
        for part_info in partial_encoded_chunk.parts.iter() {
            // TODO: only validate parts we care about
            // https://github.com/near/nearcore/issues/5885
            self.validate_part(header.encoded_merkle_root(), part_info, num_total_parts)?;
        }

        // 1.e Checking receipts validity
        for proof in partial_encoded_chunk.receipts.iter() {
            // TODO: only validate receipts we care about
            // https://github.com/near/nearcore/issues/5885
            // we can't simply use prev_block_hash to check if the node tracks this shard or not
            // because prev_block_hash may not be ready
            let shard_id = proof.1.to_shard_id;
            let ReceiptProof(shard_receipts, receipt_proof) = proof;
            let receipt_hash = CryptoHash::hash_borsh(ReceiptList(shard_id, shard_receipts));
            if !verify_path(header.outgoing_receipts_root(), &receipt_proof.proof, &receipt_hash) {
                byzantine_assert!(false);
                return Err(Error::ChainError(near_chain::Error::InvalidReceiptsProof));
            }
        }

        // 2. Consider it valid; mergeparts and receipts included in the partial encoded chunk
        // into chunk cache
        let new_part_ords =
            self.encoded_chunks.merge_in_partial_encoded_chunk(partial_encoded_chunk);

        // 3. Forward my parts to others tracking this chunk's shard
        // It's possible that the previous block has not been processed yet. We will want to
        // forward the chunk parts in this case, so we try our best to estimate current epoch id
        // using the chain head. At epoch boundary, it could happen that this epoch id is not the
        // actual epoch of the block, which is ok. In the worst case, chunk parts are not forwarded to the
        // the right block producers, which may make validators wait for chunks for a little longer,
        // but it doesn't affect the correctness of the protocol.
        if let Ok(epoch_id) = self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&partial_encoded_chunk.header.prev_block_hash())
        {
            self.send_partial_encoded_chunk_to_chunk_trackers(
                partial_encoded_chunk,
                new_part_ords,
                &epoch_id,
                &partial_encoded_chunk.header.prev_block_hash(),
            )?;
        } else {
            let epoch_id = self
                .runtime_adapter
                .get_epoch_id_from_prev_block(&self.chain_head.last_block_hash)?;
            self.send_partial_encoded_chunk_to_chunk_trackers(
                partial_encoded_chunk,
                new_part_ords,
                &epoch_id,
                &self.chain_head.last_block_hash.clone(),
            )?;
        };

        // 4. Process the forwarded parts in chunk_forwards_cache.
        self.insert_header_if_not_exists_and_process_cached_chunk_forwards(header);

        // 5. Check if the chunk is complete; requesting more if not.
        let result = self.try_process_chunk_parts_and_receipts(header)?;
        match result {
            ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts => {
                // This may be the first time we see this chunk, so mark it in the request pool.
                // If it's not already requested for, next time we resend requests we would
                // request the chunk.
                self.request_chunk_single_mark_only(header);
            }
            _ => {}
        }
        Ok(result)
    }

    pub fn process_partial_encoded_chunk_response(
        &mut self,
        response: PartialEncodedChunkResponseMsg,
    ) -> Result<(), Error> {
        let header = self.get_partial_encoded_chunk_header(&response.chunk_hash)?;
        let partial_chunk = PartialEncodedChunk::new(header, response.parts, response.receipts);
        // We already know the header signature is valid because we read it from the
        // shard manager.
        self.process_partial_encoded_chunk(MaybeValidated::from_validated(partial_chunk))?;
        Ok(())
    }

    /// Let the ShardsManager know about the chunk header, when encountering that chunk header
    /// from the block and the chunk is possibly not yet known to the ShardsManager.
    pub fn process_chunk_header_from_block(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<(), Error> {
        if self.insert_header_if_not_exists_and_process_cached_chunk_forwards(header) {
            self.try_process_chunk_parts_and_receipts(header)?;
        }
        Ok(())
    }

    /// Checks if the chunk has all parts and receipts, if so and if the node cares about the shard,
    /// decodes and persists the full chunk
    /// `header`: header of the chunk. It must be known by `ShardsManager`, either
    ///           by previous call to `process_partial_encoded_chunk` or `request_partial_encoded_chunk`
    fn try_process_chunk_parts_and_receipts(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<ProcessPartialEncodedChunkResult, Error> {
        // The logic from now on requires previous block is processed because
        // calculating owner parts requires that, so we first check
        // whether prev_block_hash is in the chain, if not, returns NeedBlock
        let prev_block_hash = header.prev_block_hash();
        let epoch_id = match self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash) {
            Ok(epoch_id) => epoch_id,
            Err(_) => {
                return Ok(ProcessPartialEncodedChunkResult::NeedBlock);
            }
        };
        let chunk_hash = header.chunk_hash();
        // check the header exists in encoded_chunks and validate it again (full validation)
        // now that prev_block is processed
        if let Some(chunk_entry) = self.encoded_chunks.get(&chunk_hash) {
            if !chunk_entry.header_fully_validated {
                let res = self.validate_chunk_header(header);
                match res {
                    Ok(()) => {
                        self.encoded_chunks.mark_entry_validated(&chunk_hash);
                    }
                    Err(err) => {
                        return match err {
                            Error::ChainError(chain_error) => Err(chain_error.into()),
                            _ => {
                                // the chunk header is invalid
                                // remove this entry from the cache and remove the request from the request pool
                                self.encoded_chunks.remove(&chunk_hash);
                                self.requested_partial_encoded_chunks.remove(&chunk_hash);
                                Err(err)
                            }
                        };
                    }
                }
            }
        } else {
            return Err(Error::UnknownChunk);
        }

        // Now check whether we have all parts and receipts needed for the given chunk
        // Note that have_all_parts and have_all_receipts don't mean that we have every part and receipt
        // in this chunk, it simply means that we have the parts and receipts that we need for this
        // chunk. See comments in has_all_parts and has_all_receipts to see the conditions.
        // we can safely unwrap here because we already checked that chunk_hash exist in encoded_chunks
        let entry = self.encoded_chunks.get(&chunk_hash).unwrap();
        let have_all_parts = self.has_all_parts(&prev_block_hash, entry)?;
        let have_all_receipts = self.has_all_receipts(&prev_block_hash, entry)?;

        let can_reconstruct = entry.parts.len() >= self.runtime_adapter.num_data_parts();
        let chunk_producer = self.runtime_adapter.get_chunk_producer(
            &epoch_id,
            header.height_created(),
            header.shard_id(),
        )?;

        // TODO(#3180): seals are disabled in single shard setting
        // self.seals_mgr.track_seals();

        if have_all_parts && self.seals_mgr.should_trust_chunk_producer(&chunk_producer) {
            if self.encoded_chunks.mark_chunk_for_inclusion(&chunk_hash) {
                self.client_adapter
                    .chunk_header_ready_for_inclusion(header.clone(), chunk_producer);
            }
        }
        // we can safely unwrap here because we already checked that chunk_hash exist in encoded_chunks
        let entry = self.encoded_chunks.get(&chunk_hash).unwrap();

        // TODO(#3180): seals are disabled in single shard setting
        /*let seal = self.seals_mgr.get_seal(
            &chunk_hash,
            &prev_block_hash,
            header.inner.height_created,
            header.inner.shard_id,
        )?;
        let have_all_seal = seal.process(entry);*/

        let cares_about_shard = cares_about_shard_this_or_next_epoch(
            self.me.as_ref(),
            &prev_block_hash,
            header.shard_id(),
            true,
            self.runtime_adapter.as_ref(),
        );

        if !cares_about_shard && have_all_parts && have_all_receipts {
            // If we don't care about the shard, we only need the parts and the receipts that we
            // own, before marking the chunk as completed.
            let partial_chunk = make_partial_encoded_chunk_from_owned_parts_and_needed_receipts(
                header,
                entry.parts.values(),
                entry.receipts.values(),
                self.me.as_ref(),
                self.runtime_adapter.as_ref(),
            );

            self.complete_chunk(partial_chunk, None);
            return Ok(ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts);
        }

        // If we can reconstruct the chunk, then all parts and receipts are available so we can
        // always complete the chunk.
        if can_reconstruct {
            let height = header.height_created();
            let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
            let mut encoded_chunk = EncodedShardChunk::from_header(
                header.clone(),
                self.rs.total_shard_count(),
                protocol_version,
            );

            for (part_ord, part_entry) in entry.parts.iter() {
                encoded_chunk.content_mut().parts[*part_ord as usize] =
                    Some(part_entry.part.clone());
            }

            let (shard_chunk, partial_chunk) = self
                .decode_encoded_chunk_if_complete(encoded_chunk)?
                .expect("decoding shouldn't fail");

            self.seals_mgr.approve_chunk(height, &chunk_hash);

            // For consistency, only persist shard_chunk if we actually care about the shard.
            // Don't persist if we don't care about the shard, even if we accidentally got enough
            // parts to reconstruct the full shard.
            if cares_about_shard {
                self.complete_chunk(partial_chunk, Some(shard_chunk));
            } else {
                self.complete_chunk(partial_chunk, None);
            }
            return Ok(ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts);
        }
        Ok(ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts)
    }

    /// A helper function to be called after a chunk is considered complete
    fn complete_chunk(
        &mut self,
        partial_chunk: PartialEncodedChunk,
        shard_chunk: Option<ShardChunk>,
    ) {
        let chunk_hash = partial_chunk.chunk_hash();
        self.encoded_chunks.mark_entry_complete(&chunk_hash);
        self.encoded_chunks.remove_from_cache_if_outside_horizon(&chunk_hash);
        self.requested_partial_encoded_chunks.remove(&chunk_hash);
        debug!(target: "chunks", "Completed chunk {:?}", chunk_hash);
        self.client_adapter.did_complete_chunk(partial_chunk, shard_chunk);
    }

    /// Try to process chunks in the chunk cache whose previous block hash is `prev_block_hash` and
    /// who are not marked as complete yet
    /// This function is needed because chunks in chunk cache will only be marked as complete after
    /// the previous block is accepted. So we need to check if there are any chunks can be marked as
    /// complete when a new block is accepted.
    pub fn check_incomplete_chunks(&mut self, prev_block_hash: &CryptoHash) {
        let mut chunks_to_process = vec![];
        if let Some(chunk_hashes) = self.encoded_chunks.get_incomplete_chunks(prev_block_hash) {
            for chunk_hash in chunk_hashes {
                if let Some(entry) = self.encoded_chunks.get(chunk_hash) {
                    chunks_to_process.push(entry.header.clone());
                }
            }
        }
        for header in chunks_to_process {
            debug!(target:"chunks", "try to process incomplete chunk {:?}, prev_block: {:?}", header.chunk_hash(), prev_block_hash);
            if let Err(err) = self.try_process_chunk_parts_and_receipts(&header) {
                error!(target:"chunks", "unexpected error processing orphan chunk {:?}", err)
            }
        }
    }

    /// Send the parts of the partial_encoded_chunk that are owned by `self.me` to the
    /// other validators that are tracking the shard.
    fn send_partial_encoded_chunk_to_chunk_trackers(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
        part_ords: HashSet<u64>,
        epoch_id: &EpochId,
        lastest_block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let me = match self.me.as_ref() {
            Some(me) => me,
            None => return Ok(()),
        };
        let owned_parts: Vec<_> = partial_encoded_chunk
            .parts
            .iter()
            .filter(|part| {
                part_ords.contains(&part.part_ord)
                    && self
                        .runtime_adapter
                        .get_part_owner(epoch_id, part.part_ord)
                        .map_or(false, |owner| &owner == me)
            })
            .cloned()
            .collect();

        if owned_parts.is_empty() {
            return Ok(());
        }

        let forward = PartialEncodedChunkForwardMsg::from_header_and_parts(
            &partial_encoded_chunk.header,
            owned_parts,
        );

        let block_producers = self
            .runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_id, lastest_block_hash)?;
        let current_chunk_height = partial_encoded_chunk.header.height_created();
        let num_shards = self.runtime_adapter.num_shards(&epoch_id)?;
        let mut next_chunk_producers = (0..num_shards)
            .map(|shard_id| {
                self.runtime_adapter.get_chunk_producer(
                    &epoch_id,
                    current_chunk_height + 1,
                    shard_id,
                )
            })
            .collect::<Result<HashSet<_>, _>>()?;
        next_chunk_producers.remove(me);
        for (bp, _) in block_producers {
            let bp_account_id = bp.take_account_id();
            // no need to send anything to myself
            if me == &bp_account_id {
                continue;
            }
            next_chunk_producers.remove(&bp_account_id);

            // Technically, here we should check if the block producer actually cares about the shard.
            // We don't because with the current implementation, we force all validators to track all
            // shards by making their config tracking all shards.
            // See https://github.com/near/nearcore/issues/7388
            self.peer_manager_adapter.do_send(
                PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkForward {
                        account_id: bp_account_id,
                        forward: forward.clone(),
                    },
                )
                .with_span_context(),
            );
        }

        // We also forward chunk parts to incoming chunk producers because we want them to be able
        // to produce the next chunk without delays. For the same reason as above, we don't check if they
        // actually track this shard.
        for next_chunk_producer in next_chunk_producers {
            self.peer_manager_adapter.do_send(
                PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkForward {
                        account_id: next_chunk_producer,
                        forward: forward.clone(),
                    },
                )
                .with_span_context(),
            );
        }

        Ok(())
    }

    /// Returns true if we have all the necessary receipts for this chunk entry to process it.
    /// NOTE: this doesn't mean that we got *all* the receipts.
    /// It means that we have all receipts included in this chunk sending to the shards we track.
    fn has_all_receipts(
        &self,
        prev_block_hash: &CryptoHash,
        chunk_entry: &EncodedChunksCacheEntry,
    ) -> Result<bool, Error> {
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(prev_block_hash)?;
        for shard_id in 0..self.runtime_adapter.num_shards(&epoch_id)? {
            let shard_id = shard_id as ShardId;
            if !chunk_entry.receipts.contains_key(&shard_id) {
                if need_receipt(
                    prev_block_hash,
                    shard_id,
                    self.me.as_ref(),
                    self.runtime_adapter.as_ref(),
                ) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Returns true if we have all the parts that are needed to validate the block.
    /// NOTE: this doesn't mean that we got *all* the parts (as given verifier only needs the ones
    /// for which it is the 'owner').
    fn has_all_parts(
        &self,
        prev_block_hash: &CryptoHash,
        chunk_entry: &EncodedChunksCacheEntry,
    ) -> Result<bool, Error> {
        for part_ord in 0..self.rs.total_shard_count() {
            let part_ord = part_ord as u64;
            if !chunk_entry.parts.contains_key(&part_ord) {
                if need_part(
                    prev_block_hash,
                    part_ord,
                    self.me.as_ref(),
                    self.runtime_adapter.as_ref(),
                )? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn create_encoded_shard_chunk(
        prev_block_hash: CryptoHash,
        prev_state_root: StateRoot,
        outcome_root: CryptoHash,
        height: u64,
        shard_id: ShardId,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
        validator_proposals: Vec<ValidatorStake>,
        transactions: Vec<SignedTransaction>,
        outgoing_receipts: &[Receipt],
        outgoing_receipts_root: CryptoHash,
        tx_root: CryptoHash,
        signer: &dyn ValidatorSigner,
        rs: &mut ReedSolomonWrapper,
        protocol_version: ProtocolVersion,
    ) -> Result<(EncodedShardChunk, Vec<MerklePath>), Error> {
        EncodedShardChunk::new(
            prev_block_hash,
            prev_state_root,
            outcome_root,
            height,
            shard_id,
            rs,
            gas_used,
            gas_limit,
            balance_burnt,
            tx_root,
            validator_proposals,
            transactions,
            outgoing_receipts,
            outgoing_receipts_root,
            signer,
            protocol_version,
        )
        .map_err(|err| err.into())
    }

    pub fn distribute_encoded_chunk(
        &mut self,
        partial_chunk: PartialEncodedChunk,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: &Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
    ) -> Result<(), Error> {
        let shard_id = encoded_chunk.shard_id();
        let _timer = metrics::DISTRIBUTE_ENCODED_CHUNK_TIME
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let chunk_header = encoded_chunk.cloned_header();
        let prev_block_hash = chunk_header.prev_block_hash();
        let _span = tracing::debug_span!(
            target: "client",
            "distribute_encoded_chunk",
            ?prev_block_hash,
            ?shard_id)
        .entered();

        let mut block_producer_mapping = HashMap::new();
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash)?;
        for part_ord in 0..self.rs.total_shard_count() {
            let part_ord = part_ord as u64;
            let to_whom = self.runtime_adapter.get_part_owner(&epoch_id, part_ord).unwrap();

            let entry = block_producer_mapping.entry(to_whom).or_insert_with(Vec::new);
            entry.push(part_ord);
        }

        let receipt_proofs = make_outgoing_receipts_proofs(
            &chunk_header,
            &outgoing_receipts,
            self.runtime_adapter.as_ref(),
        )?
        .map(Arc::new)
        .collect::<Vec<_>>();
        for (to_whom, part_ords) in block_producer_mapping {
            let part_receipt_proofs = receipt_proofs
                .iter()
                .filter(|proof| {
                    let proof_shard_id = proof.1.to_shard_id;
                    cares_about_shard_this_or_next_epoch(
                        Some(&to_whom),
                        &prev_block_hash,
                        proof_shard_id,
                        false,
                        self.runtime_adapter.as_ref(),
                    )
                })
                .cloned()
                .collect();

            let partial_encoded_chunk = encoded_chunk
                .create_partial_encoded_chunk_with_arc_receipts(
                    part_ords,
                    part_receipt_proofs,
                    &merkle_paths,
                );

            if Some(&to_whom) != self.me.as_ref() {
                self.peer_manager_adapter.do_send(
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id: to_whom.clone(),
                            partial_encoded_chunk,
                        },
                    )
                    .with_span_context(),
                );
            }
        }

        // Add it to the set of chunks to be included in the next block
        self.encoded_chunks.merge_in_partial_encoded_chunk(&partial_chunk.clone().into());
        self.encoded_chunks.mark_chunk_for_inclusion(&chunk_header.chunk_hash());

        Ok(())
    }

    pub fn handle_client_request(&mut self, request: ShardsManagerRequestFromClient) {
        match request {
            ShardsManagerRequestFromClient::ProcessChunkHeaderFromBlock(chunk_header) => {
                if let Err(e) = self.process_chunk_header_from_block(&chunk_header) {
                    warn!(target: "chunks", "Error processing chunk header from block: {:?}", e);
                }
            }
            ShardsManagerRequestFromClient::UpdateChainHeads { head, header_head } => {
                self.update_chain_heads(head, header_head)
            }
            ShardsManagerRequestFromClient::DistributeEncodedChunk {
                partial_chunk,
                encoded_chunk,
                merkle_paths,
                outgoing_receipts,
            } => {
                if let Err(e) = self.distribute_encoded_chunk(
                    partial_chunk,
                    encoded_chunk,
                    &merkle_paths,
                    outgoing_receipts,
                ) {
                    warn!(target: "chunks", "Error distributing encoded chunk: {:?}", e);
                }
            }
            ShardsManagerRequestFromClient::RequestChunks { chunks_to_request, prev_hash } => {
                self.request_chunks(chunks_to_request, prev_hash)
            }
            ShardsManagerRequestFromClient::RequestChunksForOrphan {
                chunks_to_request,
                epoch_id,
                ancestor_hash,
            } => self.request_chunks_for_orphan(chunks_to_request, &epoch_id, ancestor_hash),
            ShardsManagerRequestFromClient::CheckIncompleteChunks(prev_block_hash) => {
                self.check_incomplete_chunks(&prev_block_hash)
            }
        }
    }

    pub fn handle_network_request(&mut self, request: ShardsManagerRequestFromNetwork) {
        match request {
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(partial_encoded_chunk) => {
                if let Err(e) = self.process_partial_encoded_chunk(partial_encoded_chunk.into()) {
                    warn!(target: "chunks", "Error processing partial encoded chunk: {:?}", e);
                }
            }
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                partial_encoded_chunk_forward,
            ) => {
                if let Err(e) =
                    self.process_partial_encoded_chunk_forward(partial_encoded_chunk_forward)
                {
                    warn!(target: "chunks", "Error processing partial encoded chunk forward: {:?}", e);
                }
            }
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                partial_encoded_chunk_response,
                received_time,
            } => {
                PARTIAL_ENCODED_CHUNK_RESPONSE_DELAY.observe(received_time.elapsed().as_secs_f64());
                if let Err(e) =
                    self.process_partial_encoded_chunk_response(partial_encoded_chunk_response)
                {
                    warn!(target: "chunks", "Error processing partial encoded chunk response: {:?}", e);
                }
            }
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request,
                route_back,
            } => {
                self.process_partial_encoded_chunk_request(
                    partial_encoded_chunk_request,
                    route_back,
                );
            }
        }
    }
}

/// Indicates where we fetched the response to a PartialEncodedChunkRequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartialEncodedChunkResponseSource {
    /// No lookup was performed.
    None,
    /// We only had to look into the in-memory partial chunk cache.
    InMemoryCache,
    /// We had to look at the PartialChunk column on disk.
    PartialChunkOnDisk,
    /// We had to look at the ShardChunk column on disk, and if we found it,
    /// reconstructed the partial chunk from it.
    ShardChunkOnDisk,
}

impl PartialEncodedChunkResponseSource {
    /// Returns the name to be used for exporting prometheus metrics.
    pub fn name_for_metrics(&self) -> &'static str {
        match self {
            PartialEncodedChunkResponseSource::None => "none",
            PartialEncodedChunkResponseSource::InMemoryCache => "cache",
            PartialEncodedChunkResponseSource::PartialChunkOnDisk => "partial",
            PartialEncodedChunkResponseSource::ShardChunkOnDisk => "chunk",
        }
    }
}

#[cfg(test)]
mod test {

    use assert_matches::assert_matches;
    use near_chain::types::EpochManagerAdapter;
    use near_primitives::test_utils::create_test_signer;
    use std::sync::Arc;
    use std::time::Duration;

    use near_chain::test_utils::{KeyValueRuntime, ValidatorSchedule};
    use near_chain::{Chain, ChainStore};
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_network::types::NetworkRequests;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::block::Tip;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::merkle::merklize;
    use near_primitives::sharding::ReedSolomonWrapper;
    use near_primitives::types::EpochId;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;

    use super::*;
    use crate::logic::persist_chunk;
    use crate::test_utils::*;

    /// should not request partial encoded chunk from self
    #[test]
    fn test_request_partial_encoded_chunk_from_self() {
        let store = create_test_store();
        let mock_tip = Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        };
        let runtime_adapter = Arc::new(KeyValueRuntime::new(store.clone(), 5));
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let client_adapter = Arc::new(MockClientAdapterForShardsManager::default());
        let mut shards_manager = ShardsManager::new(
            Some("test".parse().unwrap()),
            runtime_adapter,
            network_adapter.clone(),
            client_adapter,
            ReadOnlyChunksStore::new(store),
            mock_tip.clone(),
            mock_tip,
        );
        let added = Clock::instant();
        shards_manager.requested_partial_encoded_chunks.insert(
            ChunkHash(hash(&[1])),
            ChunkRequestInfo {
                height: 0,
                ancestor_hash: Default::default(),
                prev_block_hash: Default::default(),
                shard_id: 0,
                added: added,
                last_requested: added,
            },
        );
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();

        // For the chunks that would otherwise be requested from self we expect a request to be
        // sent to any peer tracking shard

        let msg = network_adapter.requests.read().unwrap()[0].as_network_requests_ref().clone();
        if let NetworkRequests::PartialEncodedChunkRequest { target, .. } = msg {
            assert!(target.account_id == None);
        } else {
            println!("{:?}", network_adapter.requests.read().unwrap());
            assert!(false);
        };
    }

    #[test]
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    fn test_seal_removal() {
        init_test_logger();
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![vec![
            "test".parse().unwrap(),
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            "test3".parse().unwrap(),
        ]]);
        let runtime_adapter =
            Arc::new(KeyValueRuntime::new_with_validators(create_test_store(), vs, 5));
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let client_adapter = Arc::new(MockClientAdapterForShardsManager::default());
        let chain_store = ChainStore::new(create_test_store(), 0, true);
        let mock_tip = Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        };
        let mut shards_manager = ShardsManager::new(
            Some("test".parse().unwrap()),
            runtime_adapter.clone(),
            network_adapter,
            client_adapter,
            chain_store.new_read_only_chunks_store(),
            mock_tip.clone(),
            mock_tip,
        );
        let signer = create_test_signer("test");
        let mut rs = ReedSolomonWrapper::new(4, 10);
        let shard_layout = runtime_adapter.get_shard_layout(&EpochId::default()).unwrap();
        let (encoded_chunk, proof) = ShardsManager::create_encoded_shard_chunk(
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            1,
            0,
            0,
            0,
            0,
            vec![],
            vec![],
            &[],
            merklize(&Chain::build_receipts_hashes(&[], &shard_layout)).0,
            CryptoHash::default(),
            &signer,
            &mut rs,
            PROTOCOL_VERSION,
        )
        .unwrap();
        let header = encoded_chunk.cloned_header();
        let prev_block_hash = header.prev_block_hash();
        shards_manager.requested_partial_encoded_chunks.insert(
            header.chunk_hash(),
            ChunkRequestInfo {
                height: header.height_created(),
                ancestor_hash: prev_block_hash.clone(),
                prev_block_hash: prev_block_hash.clone(),
                shard_id: header.shard_id(),
                last_requested: Clock::instant(),
                added: Clock::instant(),
            },
        );
        shards_manager
            .request_partial_encoded_chunk(
                header.height_created(),
                &header.prev_block_hash(),
                header.shard_id(),
                &header.chunk_hash(),
                false,
                false,
                false,
            )
            .unwrap();
        let partial_encoded_chunk1 =
            encoded_chunk.create_partial_encoded_chunk(vec![0, 1], vec![], &proof);
        let partial_encoded_chunk2 =
            encoded_chunk.create_partial_encoded_chunk(vec![2, 3, 4], vec![], &proof);
        std::thread::sleep(Duration::from_millis(crate::ACCEPTING_SEAL_PERIOD_MS as u64 + 100));
        for partial_encoded_chunk in vec![partial_encoded_chunk1, partial_encoded_chunk2] {
            shards_manager
                .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
                .unwrap();
        }
    }

    #[test]
    fn test_get_seal() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        let seal_assert = |seals_manager: &mut SealsManager| {
            let seal = seals_manager
                .get_seal(
                    &fixture.mock_chunk_hash,
                    &fixture.mock_parent_hash,
                    fixture.mock_height,
                    fixture.mock_shard_id,
                )
                .unwrap();
            let demur = match seal {
                Seal::Active(demur) => demur,
                Seal::Past => panic!("Expected ActiveSealDemur"),
            };
            assert_eq!(demur.part_ords.len(), NUM_PARTS_REQUESTED_IN_SEAL);
            assert_eq!(demur.height, fixture.mock_height);
            assert_eq!(demur.chunk_producer, fixture.mock_chunk_producer);
        };

        // SealsManger::get_seal should:

        // 1. return a new seal when one does not exist
        assert!(seals_manager.active_demurs.is_empty());
        seal_assert(&mut seals_manager);
        assert_eq!(seals_manager.active_demurs.len(), 1);

        // 2. return the same seal when it is already created
        seal_assert(&mut seals_manager);
        assert_eq!(seals_manager.active_demurs.len(), 1);
    }

    #[test]
    fn test_approve_chunk() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        // SealsManager::approve_chunk should indicate all parts were retrieved and
        // move the seal into the past seals map.
        fixture.create_seal(&mut seals_manager);
        seals_manager.approve_chunk(fixture.mock_height, &fixture.mock_chunk_hash);
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager.should_trust_chunk_producer(&fixture.mock_chunk_producer));
        assert!(seals_manager
            .past_seals
            .get(&fixture.mock_height)
            .unwrap()
            .contains(&fixture.mock_chunk_hash));
    }

    // TODO(#3180): seals are disabled in single shard setting
    /*#[test]
    fn test_track_seals() {
        let fixture = SealsManagerTestFixture::default();
        let mut seals_manager = fixture.create_seals_manager();

        // create a seal with old timestamp
        fixture.create_expired_seal(
            &mut seals_manager,
            &fixture.mock_chunk_hash,
            &fixture.mock_parent_hash,
            fixture.mock_height,
        );

        // SealsManager::track_seals should:

        // 1. mark the chunk producer as faulty if the parts were not retrieved and
        //    move the seal into the past seals map
        seals_manager.track_seals();
        assert!(!seals_manager.should_trust_chunk_producer(&fixture.mock_chunk_producer));
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager
            .past_seals
            .get(&fixture.mock_height)
            .unwrap()
            .contains(&fixture.mock_chunk_hash));

        // 2. remove seals older than the USED_SEAL_HEIGHT_HORIZON
        fixture.create_expired_seal(
            &mut seals_manager,
            &fixture.mock_distant_chunk_hash,
            &fixture.mock_distant_block_hash,
            fixture.mock_height + PAST_SEAL_HEIGHT_HORIZON + 1,
        );
        seals_manager.track_seals();
        assert!(seals_manager.active_demurs.is_empty());
        assert!(seals_manager.past_seals.get(&fixture.mock_height).is_none());
    }*/

    #[test]
    fn test_resend_chunk_requests() {
        // Test that resending chunk requests won't request for parts the node already received
        let mut fixture = ChunkTestFixture::new(true);
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        // process chunk part 0
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&[0]);
        let result = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
            .unwrap();
        assert_matches!(result, ProcessPartialEncodedChunkResult::NeedBlock);

        // should not request part 0
        shards_manager.request_chunk_single(
            &fixture.mock_chunk_header,
            CryptoHash::default(),
            false,
        );
        let collect_request_parts = |fixture: &mut ChunkTestFixture| -> HashSet<u64> {
            let mut parts = HashSet::new();
            while let Some(r) = fixture.mock_network.pop() {
                match r.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkRequest { request, .. } => {
                        for part_ord in &request.part_ords {
                            parts.insert(*part_ord);
                        }
                    }
                    _ => {}
                }
            }
            parts
        };
        let requested_parts = collect_request_parts(&mut fixture);
        assert_eq!(requested_parts, (1..fixture.mock_chunk_parts.len() as u64).collect());

        // process chunk part 1
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&[1]);
        let result = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
            .unwrap();
        assert_matches!(result, ProcessPartialEncodedChunkResult::NeedBlock);

        // resend request and check chunk part 0 and 1 are not requested again
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();

        let requested_parts = collect_request_parts(&mut fixture);
        assert_eq!(requested_parts, (2..fixture.mock_chunk_parts.len() as u64).collect());

        // immediately resend chunk requests
        // this should not send any new requests because it doesn't pass the time check
        shards_manager.resend_chunk_requests();
        let requested_parts = collect_request_parts(&mut fixture);
        assert_eq!(requested_parts, HashSet::new());
    }

    #[test]
    fn test_invalid_chunk() {
        // Test that process_partial_encoded_chunk will reject invalid chunk
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );

        // part id > num parts
        let mut partial_encoded_chunk = fixture.make_partial_encoded_chunk(&[0]);
        if let PartialEncodedChunk::V2(ref mut chunk) = partial_encoded_chunk {
            chunk.parts[0].part_ord = fixture.mock_chunk_parts.len() as u64;
        }
        let result = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk));
        assert_matches!(result, Err(Error::InvalidChunkPartId));

        // TODO: add more test cases
    }

    #[test]
    fn test_chunk_forwarding() {
        // When ShardsManager receives parts it owns, it should forward them to the shard trackers
        // and not request any parts (yet).
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&fixture.mock_part_ords);
        let result = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
            .unwrap();
        match result {
            ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts => {}
            _ => panic!("Expected to need more parts!"),
        }
        shards_manager.request_chunk_single(
            &fixture.mock_chunk_header,
            *fixture.mock_chunk_header.prev_block_hash(),
            false,
        );
        let count_forwards_and_requests = |fixture: &ChunkTestFixture| -> (usize, usize) {
            let mut forwards_count = 0;
            let mut requests_count = 0;
            for request in fixture.mock_network.requests.read().unwrap().iter() {
                match request.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkForward { .. } => forwards_count += 1,
                    NetworkRequests::PartialEncodedChunkRequest { .. } => requests_count += 1,
                    _ => (),
                }
            }
            (forwards_count, requests_count)
        };

        let (forwards_count, requests_count) = count_forwards_and_requests(&fixture);
        assert!(forwards_count > 0);
        assert_eq!(requests_count, 0);

        // After some time, we should send requests if we have not been forwarded the parts
        // we need.
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();
        let (_, requests_count) = count_forwards_and_requests(&fixture);
        assert!(requests_count > 0);
    }

    #[test]
    fn test_chunk_forwarding_dedup() {
        // Tests that we only forward a chunk if it's the first time we receive it.
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let count_num_forward_msgs = |fixture: &ChunkTestFixture| {
            fixture
                .mock_network
                .requests
                .read()
                .unwrap()
                .iter()
                .filter(|request| match request.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkForward { .. } => true,
                    _ => false,
                })
                .count()
        };
        let non_owned_part_ords: Vec<u64> = (0..(fixture.mock_runtime.num_total_parts() as u64))
            .filter(|ord| !fixture.mock_part_ords.contains(ord))
            .collect();
        // Received 3 partial encoded chunks; the owned part is received 3 times, but should
        // only forward the part on receiving it the first time.
        let mut partial_encoded_chunks = vec![
            fixture
                .make_partial_encoded_chunk(&[non_owned_part_ords[0], fixture.mock_part_ords[0]]),
            fixture
                .make_partial_encoded_chunk(&[non_owned_part_ords[1], fixture.mock_part_ords[0]]),
            fixture
                .make_partial_encoded_chunk(&[non_owned_part_ords[2], fixture.mock_part_ords[0]]),
        ];
        shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunks.remove(0)))
            .unwrap();
        let num_forward_msgs_after_first_receiving = count_num_forward_msgs(&fixture);
        assert!(num_forward_msgs_after_first_receiving > 0);
        shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunks.remove(0)))
            .unwrap();
        shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunks.remove(0)))
            .unwrap();
        let num_forward_msgs_after_receiving_duplicates = count_num_forward_msgs(&fixture);
        assert_eq!(
            num_forward_msgs_after_receiving_duplicates,
            num_forward_msgs_after_first_receiving
        );
    }

    #[derive(PartialEq, Eq, Debug)]
    struct RequestChunksResult {
        marked_as_requested: bool,
        sent_request_message_immediately: bool,
        sent_request_message_after_timeout: bool,
    }

    /// Make ShardsManager request a chunk when run as the given account ID.
    fn run_request_chunks_with_account(
        fixture: &mut ChunkTestFixture,
        account_id: Option<AccountId>,
    ) -> RequestChunksResult {
        let mut shards_manager = ShardsManager::new(
            account_id,
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        shards_manager.insert_header_if_not_exists_and_process_cached_chunk_forwards(
            &fixture.mock_chunk_header,
        );
        shards_manager.request_chunks(
            vec![fixture.mock_chunk_header.clone()],
            fixture.mock_chunk_header.prev_block_hash().clone(),
        );
        let marked_as_requested = shards_manager
            .requested_partial_encoded_chunks
            .contains_key(&fixture.mock_chunk_header.chunk_hash());

        let mut sent_request_message_immediately = false;
        while let Some(_) = fixture.mock_network.pop() {
            sent_request_message_immediately = true;
        }
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();
        let mut sent_request_message_after_timeout = false;
        while let Some(_) = fixture.mock_network.pop() {
            sent_request_message_after_timeout = true;
        }
        RequestChunksResult {
            marked_as_requested,
            sent_request_message_immediately,
            sent_request_message_after_timeout,
        }
    }

    #[test]
    // test that
    // when a non validator requests chunks, the request is sent immediately
    // when a validator requests chunks, the request is recorded but not sent, because it
    // will wait for chunks being forwarded
    fn test_chunk_forward_non_validator() {
        // A non-validator that tracks all shards should request immediately.
        let mut fixture = ChunkTestFixture::new_with_all_shards_tracking();
        assert_eq!(
            run_request_chunks_with_account(&mut fixture, None),
            RequestChunksResult {
                marked_as_requested: true,
                sent_request_message_immediately: true,
                sent_request_message_after_timeout: true,
            }
        );

        // still a non-validator because the account id is not a validator account id
        assert_eq!(
            run_request_chunks_with_account(&mut fixture, None),
            RequestChunksResult {
                marked_as_requested: true,
                sent_request_message_immediately: true,
                sent_request_message_after_timeout: true,
            }
        );

        // when a tracking chunk producer request chunks, the request should not be send
        // immediately.
        let account_id = Some(fixture.mock_shard_tracker.clone());
        assert_eq!(
            run_request_chunks_with_account(&mut fixture, account_id),
            RequestChunksResult {
                marked_as_requested: true,
                sent_request_message_immediately: false,
                sent_request_message_after_timeout: true,
            }
        );
    }

    // Test that chunk parts are forwarded to chunk only producers iff they are the next chunk producer
    // Also test that chunk only producers wait for forwarded chunks iff they are the next chunk producer
    #[test]
    fn test_chunk_forward_chunk_only_producers() {
        init_test_logger();

        let mut fixture = ChunkTestFixture::new_with_chunk_only_producers();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&fixture.mock_part_ords);
        let _ = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
            .unwrap();
        let chunk_only_producers: Vec<_> = fixture
            .mock_runtime
            .get_chunk_only_producers_for_shard(
                &fixture.mock_chain_head.epoch_id,
                fixture.mock_chunk_header.shard_id(),
            )
            .unwrap()
            .into_iter()
            .map(|v| v.account_id().clone())
            .collect();
        let next_chunk_producer = fixture
            .mock_runtime
            .get_chunk_producer(
                &fixture.mock_chain_head.epoch_id,
                fixture.mock_chunk_header.height_created() + 1,
                fixture.mock_chunk_header.shard_id(),
            )
            .unwrap();
        // Check here that next chunk producer is a chunk only producer. We need that
        // to ensure that the test is set up right to test chunk only producers.
        assert!(
            chunk_only_producers.contains(&next_chunk_producer),
            "chunk only producers: {:?}, next_chunk_producer: {:?}",
            chunk_only_producers,
            next_chunk_producer
        );

        // Check that the part owner has forwarded the chunk part to the next chunk producer,
        // but not the rest of chunk only producers
        let mut next_chunk_producer_forwarded = false;
        while let Some(r) = fixture.mock_network.pop() {
            match r.as_network_requests_ref() {
                NetworkRequests::PartialEncodedChunkForward { account_id, .. } => {
                    if account_id == &next_chunk_producer {
                        next_chunk_producer_forwarded = true;
                    } else {
                        assert!(
                            !chunk_only_producers.contains(&account_id),
                            "shouldn't forward to {:?}",
                            account_id
                        );
                    }
                }
                _ => {}
            }
        }
        assert!(next_chunk_producer_forwarded);

        // when a validator request chunks, the request should be send immediately
        for account_id in chunk_only_producers {
            println!("account {:?}, {:?}", account_id, next_chunk_producer);
            assert_eq!(
                run_request_chunks_with_account(&mut fixture, Some(account_id.clone())),
                RequestChunksResult {
                    marked_as_requested: true,
                    sent_request_message_immediately: account_id != next_chunk_producer,
                    sent_request_message_after_timeout: true,
                }
            )
        }
    }

    #[test]
    // Test that when a validator receives a chunk before the chunk header, it should store
    // the forward and use it when it receives the header
    fn test_receive_forward_before_header() {
        // Here we test the case when the chunk is received, its previous block is not processed yet
        // We want to verify that the chunk forward can be stored and wait to be processed in this
        // case too
        let fixture = ChunkTestFixture::new(true);
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let (most_parts, other_parts) = {
            let mut most_parts = fixture.mock_chunk_parts.clone();
            let n = most_parts.len();
            let other_parts = most_parts.split_off(n - (n / 4));
            (most_parts, other_parts)
        };
        let forward = PartialEncodedChunkForwardMsg::from_header_and_parts(
            &fixture.mock_chunk_header,
            most_parts,
        );
        // The validator receives the chunk forward
        assert_matches!(
            shards_manager.process_partial_encoded_chunk_forward(forward),
            Err(Error::UnknownChunk)
        );
        let partial_encoded_chunk = PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header: fixture.mock_chunk_header.clone(),
            parts: other_parts,
            receipts: Vec::new(),
        });
        // The validator receives a chunk header with the rest of the parts it needed
        let result = shards_manager
            .process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk))
            .unwrap();

        match result {
            ProcessPartialEncodedChunkResult::NeedBlock => (),
            other_result => panic!("Expected NeedBlock, but got {:?}", other_result),
        }
        // Now try to request for this chunk, first explicitly, and then through resend_chunk_requests.
        // No requests should have been sent since all the required parts were contained in the
        // forwarded parts.
        shards_manager.request_chunks_for_orphan(
            vec![fixture.mock_chunk_header.clone()],
            &EpochId::default(),
            CryptoHash::default(),
        );
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();
        assert!(fixture
            .mock_network
            .requests
            .read()
            .unwrap()
            .iter()
            .find(|r| {
                match r.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkRequest { .. } => true,
                    _ => false,
                }
            })
            .is_none());
    }

    #[test]
    // Test that when a validator receives a chunk forward before the chunk header, and that the
    // chunk header first arrives as part of a block, it should store the the forward and use it
    // when it receives the header.
    fn test_receive_forward_before_chunk_header_from_block() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let forward = PartialEncodedChunkForwardMsg::from_header_and_parts(
            &fixture.mock_chunk_header,
            fixture.mock_chunk_parts.clone(),
        );
        // The validator receives the chunk forward
        assert_matches!(
            shards_manager.process_partial_encoded_chunk_forward(forward),
            Err(Error::UnknownChunk)
        );
        // The validator then receives the block, which is missing chunks; it notifies the
        // ShardsManager of the chunk header, and ShardsManager is able to complete the chunk
        // because of the forwarded parts.shards_manager
        shards_manager.insert_header_if_not_exists_and_process_cached_chunk_forwards(
            &fixture.mock_chunk_header,
        );
        let process_result = shards_manager
            .try_process_chunk_parts_and_receipts(&fixture.mock_chunk_header)
            .unwrap();
        match process_result {
            ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts => {}
            _ => {
                panic!("Unexpected process_result: {:?}", process_result);
            }
        }
        assert_eq!(fixture.count_chunk_completion_messages(), 1);
        // Requesting it again should not send any actual requests as the chunk is already
        // complete. Sleeping and resending later should also not send any requests.
        shards_manager.request_chunk_single(
            &fixture.mock_chunk_header,
            *fixture.mock_chunk_header.prev_block_hash(),
            false,
        );

        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        shards_manager.resend_chunk_requests();
        assert!(fixture
            .mock_network
            .requests
            .read()
            .unwrap()
            .iter()
            .find(|r| {
                match r.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkRequest { .. } => true,
                    _ => false,
                }
            })
            .is_none());
    }

    #[test]
    fn test_chunk_cache_hit_for_produced_chunk() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );

        shards_manager
            .distribute_encoded_chunk(
                fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
                fixture.mock_encoded_chunk.clone(),
                &fixture.mock_merkle_paths,
                fixture.mock_outgoing_receipts.clone(),
            )
            .unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::InMemoryCache);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_cache_hit_for_received_chunk() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );

        shards_manager
            .process_partial_encoded_chunk(
                fixture.make_partial_encoded_chunk(&fixture.all_part_ords).into(),
            )
            .unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::InMemoryCache);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_response_for_uncached_partial_chunk() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );

        persist_chunk(
            fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
            None,
            &mut fixture.chain_store,
        )
        .unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::PartialChunkOnDisk);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_response_for_uncached_shard_chunk() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );

        let mut update = fixture.chain_store.store_update();
        let shard_chunk =
            fixture.mock_encoded_chunk.decode_chunk(fixture.mock_runtime.num_data_parts()).unwrap();
        update.save_chunk(shard_chunk);
        update.commit().unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::ShardChunkOnDisk);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_response_combining_cache_and_partial_chunks() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        // Split the part ords into two groups.
        assert!(fixture.all_part_ords.len() >= 2);
        let (cache_ords, partial_ords) =
            fixture.all_part_ords.split_at(fixture.all_part_ords.len() / 2);

        shards_manager
            .process_partial_encoded_chunk(fixture.make_partial_encoded_chunk(cache_ords).into())
            .unwrap();

        persist_chunk(
            fixture.make_partial_encoded_chunk(partial_ords),
            None,
            &mut fixture.chain_store,
        )
        .unwrap();
        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::PartialChunkOnDisk);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_response_combining_cache_and_shard_chunks() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        // Only add half of the parts to the cache.
        assert!(fixture.all_part_ords.len() >= 2);

        shards_manager
            .process_partial_encoded_chunk(
                fixture
                    .make_partial_encoded_chunk(
                        &fixture.all_part_ords[0..fixture.all_part_ords.len() / 2],
                    )
                    .into(),
            )
            .unwrap();

        let mut update = fixture.chain_store.store_update();
        let shard_chunk =
            fixture.mock_encoded_chunk.decode_chunk(fixture.mock_runtime.num_data_parts()).unwrap();
        update.save_chunk(shard_chunk);
        update.commit().unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::ShardChunkOnDisk);
        assert_eq!(response.parts.len(), fixture.all_part_ords.len());
    }

    #[test]
    fn test_chunk_response_combining_cache_and_partial_chunks_with_some_missing() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        // Split the part ords into three groups; put one in cache, the second in partial
        // and the third is missing. We should return the first two groups.
        assert!(fixture.all_part_ords.len() >= 3);
        let n = fixture.all_part_ords.len();
        let cache_ords = &fixture.all_part_ords.as_slice()[0..n / 3];
        let partial_ords = &fixture.all_part_ords.as_slice()[n / 3..(n * 2 / 3)];

        shards_manager
            .process_partial_encoded_chunk(fixture.make_partial_encoded_chunk(cache_ords).into())
            .unwrap();

        persist_chunk(
            fixture.make_partial_encoded_chunk(partial_ords),
            None,
            &mut fixture.chain_store,
        )
        .unwrap();
        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: fixture.all_part_ords.clone(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::PartialChunkOnDisk);
        assert_eq!(response.parts.len(), n * 2 / 3);
    }

    #[test]
    fn test_chunk_response_empty_request() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: Vec::new(),
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::None);
        assert!(response.parts.is_empty());
    }

    #[test]
    fn test_chunk_response_for_nonexistent_chunk() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: vec![0],
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::ShardChunkOnDisk);
        assert!(response.parts.is_empty());
    }

    #[test]
    fn test_chunk_response_for_request_including_invalid_part_ord() {
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let mut update = fixture.chain_store.store_update();
        let shard_chunk =
            fixture.mock_encoded_chunk.decode_chunk(fixture.mock_runtime.num_data_parts()).unwrap();
        update.save_chunk(shard_chunk);
        update.commit().unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: vec![0, fixture.mock_runtime.num_total_parts() as u64],
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::ShardChunkOnDisk);
        // part 0 should be returned; part N should be ignored.
        assert_eq!(response.parts.len(), 1);
    }

    #[test]
    fn test_chunk_response_for_request_with_duplicate_part_ords() {
        // We should not return any duplicates.
        let mut fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let mut update = fixture.chain_store.store_update();
        let shard_chunk =
            fixture.mock_encoded_chunk.decode_chunk(fixture.mock_runtime.num_data_parts()).unwrap();
        update.save_chunk(shard_chunk);
        update.commit().unwrap();

        let (source, response) =
            shards_manager.prepare_partial_encoded_chunk_response(PartialEncodedChunkRequestMsg {
                chunk_hash: fixture.mock_chunk_header.chunk_hash(),
                part_ords: vec![0, 1, 0, 1, 0, 1, 0, 1],
                tracking_shards: HashSet::new(),
            });
        assert_eq!(source, PartialEncodedChunkResponseSource::ShardChunkOnDisk);
        assert_eq!(response.parts.len(), 2);
    }

    #[test]
    fn test_report_chunk_for_inclusion_to_client() {
        let fixture = ChunkTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            fixture.mock_client_adapter.clone(),
            fixture.chain_store.new_read_only_chunks_store(),
            fixture.mock_chain_head.clone(),
            fixture.mock_chain_head.clone(),
        );
        let part = fixture.make_partial_encoded_chunk(&fixture.mock_part_ords);
        shards_manager.process_partial_encoded_chunk(part.clone().into()).unwrap();
        assert_eq!(fixture.count_chunk_ready_for_inclusion_messages(), 1);

        // test that chunk inclusion message is only sent once.
        shards_manager.process_partial_encoded_chunk(part.into()).unwrap();
        assert_eq!(fixture.count_chunk_ready_for_inclusion_messages(), 0);
    }
}
