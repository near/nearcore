//! This module implements ShardManager, which handles chunks requesting and processing.
//! Since blocks only contain chunk headers, full chunks must be communicated separately.
//! For data availability, information in a chunk is divided into parts by Reed Solomon encoding,
//! and each validator holds a subset of parts of each chunk (a validator is called the owner
//! of the parts that they hold). This way, a chunk can be retrieved from any X validators
//! where X is the threshold for the Reed Solomon encoding for retrieving the full information.
//! Currently, X is set to be 1/3 of total validator seats (num_data_parts).
//!
//! **How chunks are propagated in the network
//! Instead sending the full chunk, chunk content is communicated between nodes through
//! PartialEncodedChunk, which includes the chunk header, some parts and receipts of the chunk.
//! Full chunk can be reconstructed if a node receives enough chunk parts.
//! A node receives partial encoded chunks in three ways,
//! - by requesting it and receiving a PartialEncodedChunkResponse,
//! - by receiving a PartialEncodedChunk, which is sent from the original chunk producer to the part owners
//!   after the chunk is produced
//! - by receiving a PartialEncodedChunkForward, which is sent from part owners to validators who
//!   track the shard, when a validator first receives a part it owns.
//!   TODO: this is actually not the current behavior. https://github.com/near/nearcore/issues/5886
//! Note that last two messages can only be sent from validators to validators, so the only way a
//! non-validator receives a partial encoded chunk is by requesting it.
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

use borsh::BorshSerialize;
use cached::{Cached, SizedCache};
use chrono::DateTime;
use log::{debug, error, warn};
use near_primitives::time::Utc;
use rand::seq::IteratorRandom;
use rand::seq::SliceRandom;

use near_chain::validate::validate_chunk_proofs;
use near_chain::{
    byzantine_assert, ChainStore, ChainStoreAccess, ChainStoreUpdate, ErrorKind, RuntimeAdapter,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_pool::{PoolIteratorWrapper, TransactionPool};
use near_primitives::block::Tip;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, verify_path, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart,
    PartialEncodedChunkV1, PartialEncodedChunkV2, ReceiptList, ReceiptProof, ReedSolomonWrapper,
    ShardChunkHeader, ShardProof,
};
use near_primitives::time::Clock;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, Gas, MerkleHash, ShardId, StateRoot,
};
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolVersion;
use near_primitives::{checked_feature, unwrap_or_return};

use crate::chunk_cache::{EncodedChunksCache, EncodedChunksCacheEntry};
use near_chain::near_chain_primitives::error::ErrorKind::DBNotFoundErr;
pub use near_chunks_primitives::Error;
use near_network_primitives::types::{
    AccountIdOrPeerTrackingShard, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg,
};
use near_primitives::epoch_manager::RngSeed;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use rand::Rng;

mod chunk_cache;
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
    runtime_adapter: Arc<dyn RuntimeAdapter>,

    active_demurs: HashMap<ChunkHash, ActiveSealDemur>,
    past_seals: BTreeMap<BlockHeight, HashSet<ChunkHash>>,
    dont_include_chunks_from: SizedCache<AccountId, ()>,
}

impl SealsManager {
    fn new(me: Option<AccountId>, runtime_adapter: Arc<dyn RuntimeAdapter>) -> Self {
        Self {
            me,
            runtime_adapter,
            active_demurs: HashMap::new(),
            past_seals: BTreeMap::new(),
            dont_include_chunks_from: SizedCache::with_size(CHUNK_PRODUCER_BLACKLIST_SIZE),
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
                let chunk_producer = self.runtime_adapter.get_chunk_producer(
                    &self.runtime_adapter.get_epoch_id_from_prev_block(parent_hash)?,
                    height,
                    shard_id,
                )?;
                let candidates = {
                    let n = self.runtime_adapter.num_total_parts();
                    // `n` is an upper bound for elements in the accumulator; declaring with
                    // this capacity up front will mean no further allocations will occur
                    // from `push` calls in the loop.
                    let mut accumulator = Vec::with_capacity(n);

                    for part_ord in 0..n {
                        let part_ord = part_ord as u64;
                        let part_owner =
                            self.runtime_adapter.get_part_owner(parent_hash, part_ord)?;
                        if part_owner == chunk_producer || Some(part_owner) == self.me {
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
        self.dont_include_chunks_from.cache_get(chunk_producer).is_none()
    }
}

pub struct ShardsManager {
    me: Option<AccountId>,

    tx_pools: HashMap<ShardId, TransactionPool>,

    runtime_adapter: Arc<dyn RuntimeAdapter>,
    peer_manager_adapter: Arc<dyn PeerManagerAdapter>,

    encoded_chunks: EncodedChunksCache,
    requested_partial_encoded_chunks: RequestPool,
    chunk_forwards_cache: SizedCache<ChunkHash, HashMap<u64, PartialEncodedChunkPart>>,

    seals_mgr: SealsManager,
    /// Useful to make tests deterministic and reproducible,
    /// while keeping the security of randomization of transactions in pool
    rng_seed: RngSeed,
}

impl ShardsManager {
    pub fn new(
        me: Option<AccountId>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        network_adapter: Arc<dyn PeerManagerAdapter>,
        rng_seed: RngSeed,
    ) -> Self {
        Self {
            me: me.clone(),
            tx_pools: HashMap::new(),
            runtime_adapter: runtime_adapter.clone(),
            peer_manager_adapter: network_adapter,
            encoded_chunks: EncodedChunksCache::new(),
            requested_partial_encoded_chunks: RequestPool::new(
                Duration::from_millis(CHUNK_REQUEST_RETRY_MS),
                Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_OTHERS_MS),
                Duration::from_millis(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS),
                Duration::from_millis(CHUNK_REQUEST_RETRY_MAX_MS),
            ),
            chunk_forwards_cache: SizedCache::with_size(CHUNK_FORWARD_CACHE_SIZE),
            seals_mgr: SealsManager::new(me, runtime_adapter),
            rng_seed,
        }
    }

    pub fn update_largest_seen_height(&mut self, new_height: BlockHeight) {
        self.encoded_chunks.update_largest_seen_height(
            new_height,
            &self.requested_partial_encoded_chunks.requests,
        );
    }

    pub fn get_pool_iterator(&mut self, shard_id: ShardId) -> Option<PoolIteratorWrapper<'_>> {
        self.tx_pools.get_mut(&shard_id).map(|pool| pool.pool_iterator())
    }

    pub fn cares_about_shard_this_or_next_epoch(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.runtime_adapter.cares_about_shard(account_id, parent_hash, shard_id, is_me)
            || self.runtime_adapter.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
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
        let mut bp_to_parts = HashMap::<_, Vec<u64>>::new();

        let cache_entry = self.encoded_chunks.get(chunk_hash);

        let request_full = force_request_full
            || self.cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                ancestor_hash,
                shard_id,
                true,
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

        for part_ord in 0..self.runtime_adapter.num_total_parts() {
            let part_ord = part_ord as u64;
            if cache_entry.map_or(false, |cache_entry| cache_entry.parts.contains_key(&part_ord)) {
                continue;
            }

            let need_to_fetch_part = if request_full || seal.contains_part_ord(&part_ord) {
                true
            } else {
                if let Some(me) = me {
                    &self.runtime_adapter.get_part_owner(ancestor_hash, part_ord)? == me
                } else {
                    false
                }
            };

            if need_to_fetch_part {
                let fetch_from = if request_from_archival {
                    shard_representative_target.clone()
                } else {
                    let part_owner =
                        self.runtime_adapter.get_part_owner(ancestor_hash, part_ord)?;

                    if Some(&part_owner) == me {
                        // If missing own part, request it from the chunk producer / node tracking shard
                        shard_representative_target.clone()
                    } else {
                        Some(part_owner)
                    }
                };

                bp_to_parts.entry(fetch_from).or_default().push(part_ord);
            }
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
        for (target_account, part_ords) in bp_to_parts {
            // extra check that we are not sending request to ourselves.
            if no_account_id || me != target_account.as_ref() {
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
                    prefer_peer: request_from_archival || rand::thread_rng().gen::<bool>(),
                    shard_id,
                    only_archival: request_from_archival,
                    min_height: height.saturating_sub(CHUNK_REQUEST_PEER_HORIZON),
                };

                self.peer_manager_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkRequest { target, request },
                ));
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
                    && self.cares_about_shard_this_or_next_epoch(
                        Some(&account_id),
                        parent_hash,
                        shard_id,
                        false,
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
                self.cares_about_shard_this_or_next_epoch(
                    self.me.as_ref(),
                    parent_hash,
                    *chunk_shard_id,
                    true,
                )
            })
            .collect::<HashSet<_>>()
    }

    /// Check whether the node should wait for chunk parts being forwarded to it
    /// Chunks will be forwarded to validators if feature ForwardChunkParts is enabled
    fn should_wait_for_chunk_forwarding(&self, prev_hash: &CryptoHash) -> Result<bool, Error> {
        // chunks will not be forwarded to non-validators
        if self.me.is_none() {
            return Ok(false);
        }
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(prev_hash)?;
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        if !checked_feature!("stable", ForwardChunkParts, protocol_version) {
            return Ok(false);
        }
        Ok(self
            .runtime_adapter
            .get_validator_by_account_id(&epoch_id, prev_hash, self.me.as_ref().unwrap())
            .is_ok())
    }

    /// Only marks this chunk as being requested
    /// Note no requests are actually sent at this point.
    fn request_chunk_single_mark_only(&mut self, chunk_header: &ShardChunkHeader) {
        self.request_chunk_single(chunk_header, chunk_header.prev_block_hash(), None, false)
    }

    /// send partial chunk requests for one chunk
    /// `chunk_header`: the chunk being requested
    /// `header_head`: header head of the current chain. If it is None, the request will be only
    ///                added to he request pool, but not sent.
    /// `should_wait_for_chunk_forwarding`: whether chunks will be forwarded to this node
    fn request_chunk_single(
        &mut self,
        chunk_header: &ShardChunkHeader,
        ancestor_hash: CryptoHash,
        header_head: Option<&Tip>,
        should_wait_for_chunk_forwarding: bool,
    ) {
        let height = chunk_header.height_created();
        let shard_id = chunk_header.shard_id();
        let chunk_hash = chunk_header.chunk_hash();

        if self.requested_partial_encoded_chunks.contains_key(&chunk_hash) {
            return;
        }

        self.encoded_chunks.try_insert(&chunk_header);

        let prev_block_hash = chunk_header.prev_block_hash();
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

        if let Some(header_head) = header_head {
            let fetch_from_archival = self.runtime_adapter
                .chunk_needs_to_be_fetched_from_archival(&ancestor_hash, &header_head.last_block_hash).unwrap_or_else(|err| {
                error!(target: "chunks", "Error during requesting partial encoded chunk. Cannot determine whether to request from an archival node, defaulting to not: {}", err);
                false
            });
            let old_block = header_head.last_block_hash != prev_block_hash
                && header_head.prev_block_hash != prev_block_hash;

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
            }
        }
    }

    /// send chunk requests for some chunks in a block
    /// `chunks_to_request`: chunks to request
    /// `prev_hash`: hash of prev block of the block we are requesting missing chunks for
    ///              The function assumes the prev block is accepted
    /// `header_head`: current head of the header chain
    pub fn request_chunks<T>(
        &mut self,
        chunks_to_request: T,
        prev_hash: CryptoHash,
        header_head: &Tip,
    ) where
        T: IntoIterator<Item = ShardChunkHeader>,
    {
        let is_chunk_forwarding_enabled =
            self.should_wait_for_chunk_forwarding(&prev_hash).unwrap_or_else(|_| {
                // prev_hash must be accepted because we don't request missing chunks through this
                // this function for orphans
                debug_assert!(false, "{:?} must be accepted", prev_hash);
                error!(target:"chunks", "requesting chunks for orphan {:?}", prev_hash);
                false
            });
        for chunk_header in chunks_to_request {
            self.request_chunk_single(
                &chunk_header,
                prev_hash,
                Some(header_head),
                is_chunk_forwarding_enabled,
            );
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
    pub fn request_chunks_for_orphan<T>(
        &mut self,
        chunks_to_request: T,
        epoch_id: &EpochId,
        ancestor_hash: CryptoHash,
        header_head: &Tip,
    ) where
        T: IntoIterator<Item = ShardChunkHeader>,
    {
        let ancestor_epoch_id =
            unwrap_or_return!(self.runtime_adapter.get_epoch_id_from_prev_block(&ancestor_hash));
        if epoch_id != &ancestor_epoch_id {
            return;
        }

        let should_wait_for_chunk_forwarding =
            self.should_wait_for_chunk_forwarding(&ancestor_hash).unwrap_or_else(|_| {
                // ancestor_hash must be accepted because we don't request missing chunks through this
                // this function for orphans
                debug_assert!(false, "{:?} must be accepted", ancestor_hash);
                error!(target:"chunks", "requesting chunks for orphan whose ancestor_hash {:?} is not accepted", ancestor_hash);
                false
            });
        for chunk_header in chunks_to_request {
            self.request_chunk_single(
                &chunk_header,
                ancestor_hash,
                Some(header_head),
                should_wait_for_chunk_forwarding,
            )
        }
    }

    /// Resends chunk requests if haven't received it within expected time.
    pub fn resend_chunk_requests(&mut self, header_head: &Tip) {
        // Process chunk one part requests.
        let requests = self.requested_partial_encoded_chunks.fetch();
        for (chunk_hash, chunk_request) in requests {
            let fetch_from_archival = self.runtime_adapter
                .chunk_needs_to_be_fetched_from_archival(&chunk_request.ancestor_hash, &header_head.last_block_hash).unwrap_or_else(|err| {
                error!(target: "chunks", "Error during re-requesting partial encoded chunk. Cannot determine whether to request from an archival node, defaulting to not: {}", err);
                false
            });
            let old_block = header_head.last_block_hash != chunk_request.prev_block_hash
                && header_head.prev_block_hash != chunk_request.prev_block_hash;

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
                    error!(target: "chunks", "Error during requesting partial encoded chunk: {}", err);
                }
            }
        }
    }

    pub fn num_chunks_for_block(&mut self, prev_block_hash: &CryptoHash) -> ShardId {
        self.encoded_chunks.num_chunks_for_block(prev_block_hash)
    }

    pub fn prepare_chunks(
        &mut self,
        prev_block_hash: &CryptoHash,
    ) -> HashMap<ShardId, ShardChunkHeader> {
        self.encoded_chunks.get_chunk_headers_for_block(prev_block_hash)
    }

    /// Returns true if transaction is not in the pool before call
    pub fn insert_transaction(&mut self, shard_id: ShardId, tx: SignedTransaction) -> bool {
        self.pool_for_shard(shard_id).insert_transaction(tx)
    }

    pub fn remove_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        if let Some(pool) = self.tx_pools.get_mut(&shard_id) {
            pool.remove_transactions(transactions)
        }
    }

    /// Computes a deterministic random seed for given `shard_id`.
    /// This seed is used to randomize the transaction pool.
    /// For better security we want the seed to different in each shard.
    /// For testing purposes we want it to be the reproducible and derived from the `self.rng_seed` and `shard_id`
    fn random_seed(base_seed: &RngSeed, shard_id: ShardId) -> RngSeed {
        let mut res = *base_seed;
        res[0] = shard_id as u8;
        res[1] = (shard_id / 256) as u8;
        res
    }

    fn pool_for_shard(&mut self, shard_id: ShardId) -> &mut TransactionPool {
        self.tx_pools.entry(shard_id).or_insert_with(|| {
            TransactionPool::new(ShardsManager::random_seed(&self.rng_seed, shard_id))
        })
    }

    pub fn reintroduce_transactions(
        &mut self,
        shard_id: ShardId,
        transactions: &Vec<SignedTransaction>,
    ) {
        self.pool_for_shard(shard_id).reintroduce_transactions(transactions.clone());
    }

    pub fn group_receipts_by_shard(
        &self,
        receipts: Vec<Receipt>,
        shard_layout: &ShardLayout,
    ) -> HashMap<ShardId, Vec<Receipt>> {
        let mut result = HashMap::with_capacity(shard_layout.num_shards() as usize);
        for receipt in receipts {
            let shard_id = account_id_to_shard_id(&receipt.receiver_id, shard_layout);
            let entry = result.entry(shard_id).or_insert_with(Vec::new);
            entry.push(receipt)
        }
        result
    }

    pub fn receipts_recipient_filter<T>(
        &self,
        from_shard_id: ShardId,
        tracking_shards: T,
        receipts_by_shard: &HashMap<ShardId, Vec<Receipt>>,
        proofs: &Vec<MerklePath>,
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
        chain_store: &mut ChainStore,
    ) {
        debug!(target: "chunks", "Received partial encoded chunk request for {:?}, part_ordinals: {:?}, receipts: {:?}, I'm {:?}", request.chunk_hash.0, request.part_ords, request.tracking_shards, self.me);

        // Check if we have the chunk in our cache
        if let Some(entry) = self.encoded_chunks.get(&request.chunk_hash) {
            // Create iterators which _might_ contain the requested parts.
            let parts_iter = request.part_ords.iter().map(|ord| entry.parts.get(ord).cloned());
            let receipts_iter = request
                .tracking_shards
                .iter()
                .map(|shard_id| entry.receipts.get(shard_id).cloned());

            // Pass iterators to function which will evaluate them. Since iterators are lazy
            // we will clone as few elements as possible before realizing not all are present.
            // In the case all are present, the response is sent.
            return self.maybe_send_partial_encoded_chunk_response(
                request.chunk_hash,
                route_back,
                parts_iter,
                receipts_iter,
            );
        // If not in the cache then check the storage
        } else if let Ok(partial_chunk) = chain_store.get_partial_chunk(&request.chunk_hash) {
            // Index _references_ to the parts we know about by their `part_ord`. Since only
            // references are used in this index, we will only clone the requested parts, not
            // all of them.
            let present_parts: HashMap<u64, _> =
                partial_chunk.parts().iter().map(|part| (part.part_ord, part)).collect();
            // Create an iterator which _might_ contain the request parts. Again, we are
            // using the laziness of iterators for efficiency.
            let parts_iter =
                request.part_ords.iter().map(|ord| present_parts.get(ord).map(|x| *x).cloned());

            // Same process for receipts as above for parts.
            let present_receipts: HashMap<ShardId, _> = partial_chunk
                .receipts()
                .iter()
                .map(|receipt| (receipt.1.to_shard_id, receipt))
                .collect();
            let receipts_iter = request
                .tracking_shards
                .iter()
                .map(|shard_id| present_receipts.get(shard_id).map(|x| *x).cloned());

            // Pass iterators to function, same as cache case.
            return self.maybe_send_partial_encoded_chunk_response(
                request.chunk_hash,
                route_back,
                parts_iter,
                receipts_iter,
            );
        };
    }

    /// Checks `parts_iter` and `receipts_iter`, if all elements are `Some` then sends
    /// a `PartialEncodedChunkResponse`. `parts_iter` is only evaluated up to the first `None`
    /// (if any); since iterators are lazy this could save some work if there were any `Some`
    /// elements later in the iterator. `receipts_iter` is only evaluated if `part_iter` was
    /// completely present. Similarly, `receipts_iter` is only evaluated up to the first `None`
    /// if it is evaluated at all.
    fn maybe_send_partial_encoded_chunk_response<A, B>(
        &self,
        chunk_hash: ChunkHash,
        route_back: CryptoHash,
        parts_iter: A,
        receipts_iter: B,
    ) where
        A: Iterator<Item = Option<PartialEncodedChunkPart>>,
        B: Iterator<Item = Option<ReceiptProof>>,
    {
        let maybe_known_parts: Option<Vec<_>> = parts_iter.collect();
        let parts = match maybe_known_parts {
            None => {
                debug!(target:"chunks", "Not responding, some parts are missing");
                return;
            }
            Some(known_parts) => known_parts,
        };

        let maybe_known_receipts: Option<Vec<_>> = receipts_iter.collect();
        let receipts = match maybe_known_receipts {
            None => {
                debug!(target:"chunks", "Not responding, some receipts are missing");
                return;
            }
            Some(known_receipts) => known_receipts,
        };

        let response = PartialEncodedChunkResponseMsg { chunk_hash, parts, receipts };

        self.peer_manager_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedChunkResponse { route_back, response },
        ));
    }

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

    pub fn decode_and_persist_encoded_chunk_if_complete(
        &mut self,
        mut encoded_chunk: EncodedShardChunk,
        chain_store: &mut ChainStore,
        rs: &mut ReedSolomonWrapper,
    ) -> Result<bool, Error> {
        match ShardsManager::check_chunk_complete(&mut encoded_chunk, rs) {
            ChunkStatus::Complete(merkle_paths) => {
                self.decode_and_persist_encoded_chunk(encoded_chunk, chain_store, merkle_paths)?;
                Ok(true)
            }
            ChunkStatus::Incomplete => Ok(false),
            ChunkStatus::Invalid => {
                let chunk_hash = encoded_chunk.chunk_hash();
                self.encoded_chunks.remove(&chunk_hash);
                Err(Error::InvalidChunk)
            }
        }
    }

    pub fn validate_partial_encoded_chunk_forward(
        &mut self,
        forward: &PartialEncodedChunkForwardMsg,
    ) -> Result<(), Error> {
        let valid_hash = forward.is_valid_hash(); // check hash

        if !valid_hash {
            return Err(Error::InvalidPartMessage);
        }

        // check part merkle proofs
        let num_total_parts = self.runtime_adapter.num_total_parts();
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
    pub fn get_partial_encoded_chunk_header(
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

    pub fn insert_forwarded_chunk(&mut self, forward: PartialEncodedChunkForwardMsg) {
        let chunk_hash = forward.chunk_hash.clone();
        let num_total_parts = self.runtime_adapter.num_total_parts() as u64;
        match self.chunk_forwards_cache.cache_get_mut(&chunk_hash) {
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
                self.chunk_forwards_cache.cache_set(chunk_hash, parts);
            }

            Some(existing_parts) => {
                for part in forward.parts {
                    let part_ord = part.part_ord;
                    if part_ord > num_total_parts {
                        warn!(target: "chunks", "Received chunk part with part_ord greater than the the total number of chunks");
                        continue;
                    }
                    if existing_parts.insert(part_ord, part).is_some() {
                        warn!(target: "chunks", "Part ord {} for chunk {:?} was forwarded multiple times", part_ord, chunk_hash.0);
                    }
                }
            }
        }
    }

    /// Get a list of incomplete chunks whose previous block hash is `prev_block_hash`
    pub fn get_incomplete_chunks(&self, prev_block_hash: &CryptoHash) -> Vec<ShardChunkHeader> {
        if let Some(chunk_hashes) = self.encoded_chunks.get_incomplete_chunks(prev_block_hash) {
            chunk_hashes
                .iter()
                .flat_map(|chunk_hash| {
                    self.encoded_chunks.get(chunk_hash).map(|e| e.header.clone())
                })
                .collect()
        } else {
            vec![]
        }
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
    pub fn validate_chunk_header(
        &self,
        chain_head: Option<&Tip>,
        header: &ShardChunkHeader,
    ) -> Result<(), Error> {
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
            let prev_block_hash = header.prev_block_hash();
            let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash);
            if let Ok(epoch_id) = epoch_id {
                (prev_block_hash, epoch_id, true)
            } else if let Some(request_info) =
                self.requested_partial_encoded_chunks.get_request_info(&chunk_hash)
            {
                let ancestor_hash = request_info.ancestor_hash;
                let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&ancestor_hash)?;
                (ancestor_hash, epoch_id, true)
            } else if let Some(chain_head) = chain_head {
                // we can safely unwrap here because chain head must already be accepted
                let epoch_id = self
                    .runtime_adapter
                    .get_epoch_id_from_prev_block(&chain_head.last_block_hash)
                    .unwrap();
                (chain_head.last_block_hash, epoch_id, false)
            } else {
                return Err(epoch_id.err().unwrap().into());
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
                    Err(near_chain::Error::from(DBNotFoundErr(
                        format!("block {:?}", header.prev_block_hash()).to_string(),
                    ))
                    .into())
                };
            }
            Ok(true) => (),
            Err(chain_error) => {
                return Err(chain_error.into());
            }
        }

        // 2. check protocol version
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        if !header.version_range().contains(protocol_version) {
            return if epoch_id_confirmed {
                Err(Error::InvalidChunkHeader)
            } else {
                Err(near_chain::Error::from(DBNotFoundErr(
                    format!("block {:?}", header.prev_block_hash()).to_string(),
                ))
                .into())
            };
        }
        Ok(())
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
    /// `chain_head`: current chain head, used for validating chunk header
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
        partial_encoded_chunk: MaybeValidated<&PartialEncodedChunkV2>,
        chain_head: Option<&Tip>,
        chain_store: &mut ChainStore,
        rs: &mut ReedSolomonWrapper,
    ) -> Result<ProcessPartialEncodedChunkResult, Error> {
        debug!(target: "chunks", "process partial encoded chunk {:?}, me: {:?}", partial_encoded_chunk.header.chunk_hash(), self.me);
        let header = &partial_encoded_chunk.header;
        let chunk_hash = header.chunk_hash();
        // Verify the partial encoded chunk is valid and worth processing
        // 1.a Leave if we received known chunk
        if let Some(entry) = self.encoded_chunks.get(&chunk_hash) {
            if entry.complete {
                return Ok(ProcessPartialEncodedChunkResult::Known);
            }
        };

        // 1.b Checking chunk height
        let chunk_requested = self.requested_partial_encoded_chunks.contains_key(&chunk_hash);
        if !chunk_requested {
            if !self.encoded_chunks.height_within_horizon(header.height_created()) {
                return Err(Error::ChainError(ErrorKind::InvalidChunkHeight.into()));
            }
            // We shouldn't process unrequested chunk if we have seen one with same (height_created + shard_id)
            if let Ok(hash) = chain_store
                .get_any_chunk_hash_by_height_shard(header.height_created(), header.shard_id())
            {
                if *hash != chunk_hash {
                    warn!(target: "client", "Rejecting unrequested chunk {:?}, height {}, shard_id {}, because of having {:?}", chunk_hash, header.height_created(), header.shard_id(), hash);
                    return Err(Error::DuplicateChunkHeight.into());
                }
                return Ok(ProcessPartialEncodedChunkResult::Known);
            }
        }

        // 1.c checking header validity
        match partial_encoded_chunk
            .validate_with(|pec| self.validate_chunk_header(chain_head, &pec.header).map(|()| true))
        {
            Err(Error::ChainError(chain_error)) => match chain_error.kind() {
                // validate_chunk_header returns DBNotFoundError if the previous block is not ready
                // in this case, we return NeedBlock instead of error
                ErrorKind::DBNotFoundErr(_) => {
                    return Ok(ProcessPartialEncodedChunkResult::NeedBlock)
                }
                _ => return Err(chain_error.into()),
            },
            Err(err) => return Err(err),
            Ok(_) => (),
        }
        let partial_encoded_chunk = partial_encoded_chunk.into_inner();

        // 1.d Checking part_ords' validity
        let num_total_parts = self.runtime_adapter.num_total_parts();
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
            let receipt_hash = hash(&ReceiptList(shard_id, shard_receipts).try_to_vec().unwrap());
            if !verify_path(header.outgoing_receipts_root(), &receipt_proof.proof, &receipt_hash) {
                byzantine_assert!(false);
                return Err(Error::ChainError(ErrorKind::InvalidReceiptsProof.into()));
            }
        }

        // 2. Consider it valid and stores it
        // Store chunk hash into chunk_hash_per_height_shard collection
        let mut store_update = chain_store.store_update();
        store_update.save_chunk_hash(
            header.height_created(),
            header.shard_id(),
            chunk_hash.clone(),
        );
        store_update.commit()?;

        // Merge parts and receipts included in the partial encoded chunk into chunk cache
        self.encoded_chunks.merge_in_partial_encoded_chunk(partial_encoded_chunk);

        // 3. Process the forwarded parts in chunk_forwards_cache
        if let Some(forwarded_parts) = self.chunk_forwards_cache.cache_remove(&chunk_hash) {
            // We have the header now, and there were some parts we were forwarded earlier.
            // Let's process those parts now.
            let forwarded_chunk = PartialEncodedChunkV2 {
                header: header.clone(),
                parts: forwarded_parts.into_iter().map(|(_, part)| part).collect(),
                receipts: Vec::new(),
            };
            // Make sure we mark this chunk as being requested so that it is handled
            // properly in the next call. Note no requests are actually sent at this point.
            self.request_chunk_single_mark_only(header);
            // Call process_partial_encoded_chunk recursively, "simulating" as that forwarded
            // part is just received from the network
            return self.process_partial_encoded_chunk(
                // We can assert the signature on the header is valid because
                // it would have been checked in an earlier call to this function.
                MaybeValidated::from_validated(&forwarded_chunk),
                None,
                chain_store,
                rs,
            );
        }

        // 4. Forward my parts to others tracking this chunk's shard
        let epoch_id = match self
            .runtime_adapter
            .get_epoch_id_from_prev_block(&partial_encoded_chunk.header.prev_block_hash())
        {
            Ok(epoch_id) => epoch_id,
            Err(_) => {
                return Ok(ProcessPartialEncodedChunkResult::NeedBlock);
            }
        };
        let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
        if checked_feature!("stable", ForwardChunkParts, protocol_version) {
            self.send_partial_encoded_chunk_to_chunk_trackers(partial_encoded_chunk)?;
        };

        self.try_process_chunk_parts_and_receipts(&partial_encoded_chunk.header, chain_store, rs)
    }

    /// Checks if the chunk has all parts and receipts, if so and if the node cares about the shard,
    /// decodes and persists the full chunk
    /// `header`: header of the chunk. It must be known by `ShardsManager`, either
    ///           by previous call to `process_partial_encoded_chunk` or `request_partial_encoded_chunk`
    pub fn try_process_chunk_parts_and_receipts(
        &mut self,
        header: &ShardChunkHeader,
        chain_store: &mut ChainStore,
        rs: &mut ReedSolomonWrapper,
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
                let res = self.validate_chunk_header(None, header);
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

        // Now check whether we have all parts and receipts for the given chunk
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
            self.encoded_chunks.insert_chunk_header(header.shard_id(), header.clone());
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

        if have_all_parts && have_all_receipts {
            let cares_about_shard = self.cares_about_shard_this_or_next_epoch(
                self.me.as_ref(),
                &prev_block_hash,
                header.shard_id(),
                true,
            );

            if let Err(_) = chain_store.get_partial_chunk(&chunk_hash) {
                let mut store_update = chain_store.store_update();
                self.persist_partial_chunk_for_data_availability(entry, &mut store_update);
                store_update.commit()?;
            }

            // If all the parts and receipts are received, and we don't care about the shard,
            //    no need to request anything else.
            // If we do care about the shard, we will remove the request once the full chunk is
            //    assembled.
            if !cares_about_shard {
                self.complete_chunk(&chunk_hash);
                return Ok(ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts);
            }
        }

        if can_reconstruct {
            let height = header.height_created();
            let protocol_version = self.runtime_adapter.get_epoch_protocol_version(&epoch_id)?;
            let mut encoded_chunk = EncodedShardChunk::from_header(
                header.clone(),
                self.runtime_adapter.num_total_parts(),
                protocol_version,
            );

            for (part_ord, part_entry) in entry.parts.iter() {
                encoded_chunk.content_mut().parts[*part_ord as usize] =
                    Some(part_entry.part.clone());
            }

            let successfully_decoded =
                self.decode_and_persist_encoded_chunk_if_complete(encoded_chunk, chain_store, rs)?;

            assert!(successfully_decoded);

            self.seals_mgr.approve_chunk(height, &chunk_hash);

            self.complete_chunk(&chunk_hash);
            return Ok(ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts);
        }

        // add the chunk to the request pool in case it is not there already
        self.request_chunk_single_mark_only(header);
        Ok(ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts)
    }

    /// A helper function to be called after a chunk is considered complete
    fn complete_chunk(&mut self, chunk_hash: &ChunkHash) {
        self.encoded_chunks.mark_entry_complete(chunk_hash);
        self.encoded_chunks.remove_from_cache_if_outside_horizon(chunk_hash);
        self.requested_partial_encoded_chunks.remove(chunk_hash);
    }

    /// Send the parts of the partial_encoded_chunk that are owned by `self.me` to the
    /// other validators that are tracking the shard.
    pub fn send_partial_encoded_chunk_to_chunk_trackers(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
    ) -> Result<(), Error> {
        let me = match &self.me {
            Some(me) => me,
            None => return Ok(()),
        };
        let parent_hash = partial_encoded_chunk.header.prev_block_hash();
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(&parent_hash)?;
        let shard_id = partial_encoded_chunk.header.shard_id();
        let owned_parts: Vec<_> = partial_encoded_chunk
            .parts
            .iter()
            .filter(|part| {
                self.runtime_adapter
                    .get_part_owner(&parent_hash, part.part_ord)
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

        let block_producers =
            self.runtime_adapter.get_epoch_block_producers_ordered(&epoch_id, &parent_hash)?;
        for (bp, _) in block_producers {
            let bp_account_id = bp.take_account_id();
            // no need to send anything to myself
            if me == &bp_account_id {
                continue;
            }

            let cares_about_shard = self.cares_about_shard_this_or_next_epoch(
                Some(&bp_account_id),
                &parent_hash,
                shard_id,
                false,
            );
            if cares_about_shard {
                self.peer_manager_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkForward {
                        account_id: bp_account_id,
                        forward: forward.clone(),
                    },
                ));
            }
        }

        Ok(())
    }

    fn need_receipt(&self, prev_block_hash: &CryptoHash, shard_id: ShardId) -> bool {
        self.cares_about_shard_this_or_next_epoch(self.me.as_ref(), prev_block_hash, shard_id, true)
    }

    /// Returns true if we need this part to sign the block.
    fn need_part(&self, prev_block_hash: &CryptoHash, part_ord: u64) -> Result<bool, Error> {
        Ok(Some(self.runtime_adapter.get_part_owner(prev_block_hash, part_ord)?) == self.me)
    }

    fn has_all_receipts(
        &self,
        prev_block_hash: &CryptoHash,
        chunk_entry: &EncodedChunksCacheEntry,
    ) -> Result<bool, Error> {
        let epoch_id = self.runtime_adapter.get_epoch_id_from_prev_block(prev_block_hash)?;
        for shard_id in 0..self.runtime_adapter.num_shards(&epoch_id)? {
            let shard_id = shard_id as ShardId;
            if !chunk_entry.receipts.contains_key(&shard_id) {
                if self.need_receipt(prev_block_hash, shard_id) {
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
        for part_ord in 0..self.runtime_adapter.num_total_parts() {
            let part_ord = part_ord as u64;
            if !chunk_entry.parts.contains_key(&part_ord) {
                if self.need_part(prev_block_hash, part_ord)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn create_encoded_shard_chunk(
        &mut self,
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
        outgoing_receipts: &Vec<Receipt>,
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

    pub fn persist_partial_chunk_for_data_availability(
        &self,
        chunk_entry: &EncodedChunksCacheEntry,
        store_update: &mut ChainStoreUpdate<'_>,
    ) {
        let cares_about_shard = self.cares_about_shard_this_or_next_epoch(
            self.me.as_ref(),
            &chunk_entry.header.prev_block_hash(),
            chunk_entry.header.shard_id(),
            true,
        );
        let prev_block_hash = chunk_entry.header.prev_block_hash();
        let parts = chunk_entry
            .parts
            .iter()
            .filter_map(|(part_ord, part_entry)| {
                if cares_about_shard || self.need_part(&prev_block_hash, *part_ord).unwrap_or(false)
                {
                    Some(part_entry.clone())
                } else if let Ok(need_part) = self.need_part(&prev_block_hash, *part_ord) {
                    if need_part {
                        Some(part_entry.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        let receipts = chunk_entry
            .receipts
            .iter()
            .filter_map(|(shard_id, receipt)| {
                if cares_about_shard || self.need_receipt(&prev_block_hash, *shard_id) {
                    Some(receipt.clone())
                } else if self.need_receipt(&prev_block_hash, *shard_id) {
                    Some(receipt.clone())
                } else {
                    None
                }
            })
            .collect();
        let partial_chunk = match chunk_entry.header.clone() {
            ShardChunkHeader::V1(header) => {
                PartialEncodedChunk::V1(PartialEncodedChunkV1 { header, parts, receipts })
            }
            header => PartialEncodedChunk::V2(PartialEncodedChunkV2 { header, parts, receipts }),
        };

        store_update.save_partial_chunk(partial_chunk);
    }

    pub fn decode_and_persist_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        chain_store: &mut ChainStore,
        merkle_paths: Vec<MerklePath>,
    ) -> Result<(), Error> {
        let chunk_hash = encoded_chunk.chunk_hash();

        let mut store_update = chain_store.store_update();
        if let Ok(shard_chunk) = encoded_chunk
            .decode_chunk(self.runtime_adapter.num_data_parts())
            .map_err(|err| Error::from(err))
            .and_then(|shard_chunk| {
                if !validate_chunk_proofs(&shard_chunk, &*self.runtime_adapter)? {
                    return Err(Error::InvalidChunk);
                }
                Ok(shard_chunk)
            })
        {
            debug!(target: "chunks", "Reconstructed and decoded chunk {}, encoded length was {}, num txs: {}, I'm {:?}", chunk_hash.0, encoded_chunk.encoded_length(), shard_chunk.transactions().len(), self.me);

            self.create_and_persist_partial_chunk(
                &encoded_chunk,
                merkle_paths,
                shard_chunk.receipts().clone(),
                &mut store_update,
            )?;

            // Decoded a valid chunk, store it in the permanent store
            store_update.save_chunk(shard_chunk);
            store_update.commit()?;

            self.requested_partial_encoded_chunks.remove(&chunk_hash);

            return Ok(());
        } else {
            // Can't decode chunk or has invalid proofs, ignore it
            error!(target: "chunks", "Reconstructed, but failed to decoded chunk {}, I'm {:?}", chunk_hash.0, self.me);
            store_update.save_invalid_chunk(encoded_chunk);
            store_update.commit()?;
            self.encoded_chunks.remove(&chunk_hash);
            self.requested_partial_encoded_chunks.remove(&chunk_hash);
            return Err(Error::InvalidChunk);
        }
    }

    pub fn create_and_persist_partial_chunk(
        &mut self,
        encoded_chunk: &EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
        store_update: &mut ChainStoreUpdate<'_>,
    ) -> Result<(), Error> {
        let header = encoded_chunk.cloned_header();
        let shard_id = header.shard_id();
        let shard_layout =
            self.runtime_adapter.get_shard_layout_from_prev_block(&header.prev_block_hash())?;
        let outgoing_receipts_hashes =
            self.runtime_adapter.build_receipts_hashes(&outgoing_receipts, &shard_layout);
        let (outgoing_receipts_root, outgoing_receipts_proofs) =
            merklize(&outgoing_receipts_hashes);
        assert_eq!(header.outgoing_receipts_root(), outgoing_receipts_root);

        // Save this chunk into encoded_chunks & process encoded chunk to add to the store.
        let mut receipts_by_shard = self.group_receipts_by_shard(outgoing_receipts, &shard_layout);
        let receipts = outgoing_receipts_proofs
            .into_iter()
            .enumerate()
            .map(|(to_shard_id, proof)| {
                let to_shard_id = to_shard_id as u64;
                let receipts = receipts_by_shard.remove(&to_shard_id).unwrap_or_else(Vec::new);
                let shard_proof = ShardProof { from_shard_id: shard_id, to_shard_id, proof };
                ReceiptProof(receipts, shard_proof)
            })
            .collect();
        let partial_chunk = PartialEncodedChunkV2 {
            header,
            parts: encoded_chunk
                .content()
                .parts
                .clone()
                .into_iter()
                .zip(merkle_paths)
                .enumerate()
                .map(|(part_ord, (part, merkle_proof))| {
                    let part_ord = part_ord as u64;
                    let part = part.unwrap();
                    PartialEncodedChunkPart { part_ord, part, merkle_proof }
                })
                .collect(),
            receipts,
        };

        // Save this chunk into encoded_chunks.
        self.encoded_chunks.merge_in_partial_encoded_chunk(&partial_chunk);

        // Save the partial chunk for data availability
        // the unwrap is save because `merge_in_partial_encoded_chunk` just added the chunk
        let cache_entry = self.encoded_chunks.get(&partial_chunk.header.chunk_hash()).unwrap();
        self.persist_partial_chunk_for_data_availability(cache_entry, store_update);

        Ok(())
    }

    pub fn distribute_encoded_chunk(
        &mut self,
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        outgoing_receipts: Vec<Receipt>,
        chain_store: &mut ChainStore,
    ) -> Result<(), Error> {
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let chunk_header = encoded_chunk.cloned_header();
        let prev_block_hash = chunk_header.prev_block_hash();
        let shard_id = chunk_header.shard_id();
        let shard_layout =
            self.runtime_adapter.get_shard_layout_from_prev_block(&prev_block_hash)?;
        let outgoing_receipts_hashes =
            self.runtime_adapter.build_receipts_hashes(&outgoing_receipts, &shard_layout);
        let (outgoing_receipts_root, outgoing_receipts_proofs) =
            merklize(&outgoing_receipts_hashes);
        assert_eq!(chunk_header.outgoing_receipts_root(), outgoing_receipts_root);

        let mut block_producer_mapping = HashMap::new();

        for part_ord in 0..self.runtime_adapter.num_total_parts() {
            let part_ord = part_ord as u64;
            let to_whom = self.runtime_adapter.get_part_owner(&prev_block_hash, part_ord).unwrap();

            let entry = block_producer_mapping.entry(to_whom).or_insert_with(Vec::new);
            entry.push(part_ord);
        }

        let mut receipts_by_shard = self.group_receipts_by_shard(outgoing_receipts, &shard_layout);
        let receipt_proofs: Vec<_> = outgoing_receipts_proofs
            .into_iter()
            .enumerate()
            .map(|(proof_shard_id, proof)| {
                let proof_shard_id = proof_shard_id as u64;
                let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
                let shard_proof =
                    ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
                Arc::new(ReceiptProof(receipts, shard_proof))
            })
            .collect();

        for (to_whom, part_ords) in block_producer_mapping {
            let part_receipt_proofs = receipt_proofs
                .iter()
                .filter(|proof| {
                    let proof_shard_id = proof.1.to_shard_id;
                    self.cares_about_shard_this_or_next_epoch(
                        Some(&to_whom),
                        &prev_block_hash,
                        proof_shard_id,
                        false,
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
                self.peer_manager_adapter.do_send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id: to_whom.clone(),
                        partial_encoded_chunk,
                    },
                ));
            }
        }

        // Add it to the set of chunks to be included in the next block
        self.encoded_chunks.insert_chunk_header(shard_id, chunk_header);

        // Store the chunk in the permanent storage
        self.decode_and_persist_encoded_chunk(encoded_chunk, chain_store, merkle_paths)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;
    use std::time::Duration;

    use near_chain::test_utils::KeyValueRuntime;
    use near_chain::{ChainStore, RuntimeAdapter};
    use near_crypto::KeyType;
    use near_logger_utils::init_test_logger;
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_network::types::NetworkRequests;
    use near_primitives::block::Tip;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::merkle::merklize;
    use near_primitives::sharding::ReedSolomonWrapper;
    use near_primitives::types::EpochId;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;

    use super::*;
    use crate::test_utils::*;

    const TEST_SEED: RngSeed = [3; 32];

    /// should not request partial encoded chunk from self
    #[test]
    fn test_request_partial_encoded_chunk_from_self() {
        let runtime_adapter = Arc::new(KeyValueRuntime::new(create_test_store(), 5));
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut shards_manager = ShardsManager::new(
            Some("test".parse().unwrap()),
            runtime_adapter,
            network_adapter.clone(),
            TEST_SEED,
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
        shards_manager.resend_chunk_requests(&Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        });

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
        let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
            create_test_store(),
            vec![vec![
                "test".parse().unwrap(),
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                "test3".parse().unwrap(),
            ]],
            1,
            1,
            5,
        ));
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut chain_store = ChainStore::new(create_test_store(), 0);
        let mut shards_manager = ShardsManager::new(
            Some("test".parse().unwrap()),
            runtime_adapter.clone(),
            network_adapter.clone(),
            TEST_SEED,
        );
        let signer =
            InMemoryValidatorSigner::from_seed("test".parse().unwrap(), KeyType::ED25519, "test");
        let mut rs = ReedSolomonWrapper::new(4, 10);
        let shard_layout = runtime_adapter.get_shard_layout(&EpochId::default()).unwrap();
        let (encoded_chunk, proof) = shards_manager
            .create_encoded_shard_chunk(
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
                &vec![],
                merklize(&runtime_adapter.build_receipts_hashes(&vec![], &shard_layout)).0,
                CryptoHash::default(),
                &signer,
                &mut rs,
                PROTOCOL_VERSION,
            )
            .unwrap();
        let header = encoded_chunk.cloned_header();
        shards_manager.requested_partial_encoded_chunks.insert(
            header.chunk_hash(),
            ChunkRequestInfo {
                height: header.height_created(),
                ancestor_hash: header.prev_block_hash(),
                prev_block_hash: header.prev_block_hash(),
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
            let pec_v2 = partial_encoded_chunk.into();
            shards_manager
                .process_partial_encoded_chunk(
                    MaybeValidated::from(&pec_v2),
                    None,
                    &mut chain_store,
                    &mut rs,
                )
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
    fn test_chunk_forwarding() {
        // When ShardsManager receives parts it owns, it should forward them to the shard trackers
        // and not request any parts (yet).
        let mut fixture = ChunkForwardingTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_chunk_part_owner.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            TEST_SEED,
        );
        let partial_encoded_chunk = fixture.make_partial_encoded_chunk(&fixture.mock_part_ords);
        let result = shards_manager
            .process_partial_encoded_chunk(
                MaybeValidated::from(&partial_encoded_chunk),
                None,
                &mut fixture.chain_store,
                &mut fixture.rs,
            )
            .unwrap();
        match result {
            ProcessPartialEncodedChunkResult::NeedMorePartsOrReceipts => {
                shards_manager.request_chunk_single_mark_only(&fixture.mock_chunk_header)
            }

            _ => panic!("Expected to need more parts!"),
        }
        let count_forwards_and_requests = |fixture: &ChunkForwardingTestFixture| -> (usize, usize) {
            let mut forwards_count = 0;
            let mut requests_count = 0;
            fixture.mock_network.requests.read().unwrap().iter().for_each(|r| {
                match r.as_network_requests_ref() {
                    NetworkRequests::PartialEncodedChunkForward { .. } => forwards_count += 1,
                    NetworkRequests::PartialEncodedChunkRequest { .. } => requests_count += 1,
                    _ => (),
                }
            });
            (forwards_count, requests_count)
        };

        let (forwards_count, requests_count) = count_forwards_and_requests(&fixture);
        assert!(forwards_count > 0);
        assert_eq!(requests_count, 0);

        // After some time, we should send requests if we have not been forwarded the parts
        // we need.
        std::thread::sleep(Duration::from_millis(2 * CHUNK_REQUEST_RETRY_MS));
        let head = Tip {
            height: 0,
            last_block_hash: Default::default(),
            prev_block_hash: Default::default(),
            epoch_id: Default::default(),
            next_epoch_id: Default::default(),
        };
        shards_manager.resend_chunk_requests(&head);
        let (_, requests_count) = count_forwards_and_requests(&fixture);
        assert!(requests_count > 0);
    }

    #[test]
    // test that
    // when a non valdiator requests chunks, the request is sent immediately
    // when a validator requests chunks, the request is recorded but not sent, because it
    // will wait for chunks being forwarded
    fn test_chunk_forward_non_validator() {
        // When a non validator node requests chunks, the request should be send immediately
        let fixture = ChunkForwardingTestFixture::default();
        let test_seed = [4; 32];
        let mut shards_manager = ShardsManager::new(
            None,
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            test_seed,
        );
        let header_head = Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        };
        shards_manager.request_chunks(
            vec![fixture.mock_chunk_header.clone()],
            fixture.mock_chunk_header.prev_block_hash(),
            &header_head,
        );
        assert!(shards_manager
            .requested_partial_encoded_chunks
            .contains_key(&fixture.mock_chunk_header.chunk_hash()));
        let mut requested = false;
        while let Some(_) = fixture.mock_network.pop() {
            requested = true;
        }
        assert!(requested);

        // still a non-validator because the account id is not a validator account id
        let mut shards_manager = ShardsManager::new(
            Some("none".parse().unwrap()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            test_seed,
        );
        shards_manager.request_chunks(
            vec![fixture.mock_chunk_header.clone()],
            fixture.mock_chunk_header.prev_block_hash(),
            &header_head,
        );
        assert!(shards_manager
            .requested_partial_encoded_chunks
            .contains_key(&fixture.mock_chunk_header.chunk_hash()));
        let mut requested = false;
        while let Some(_) = fixture.mock_network.pop() {
            requested = true;
        }
        assert!(requested);

        // when a validator request chunks, the request should be send immediately
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            test_seed,
        );
        shards_manager.request_chunks(
            vec![fixture.mock_chunk_header.clone()],
            fixture.mock_chunk_header.prev_block_hash(),
            &header_head,
        );
        assert!(shards_manager
            .requested_partial_encoded_chunks
            .contains_key(&fixture.mock_chunk_header.chunk_hash()));
        assert!(fixture.mock_network.pop().is_none());
    }

    #[test]
    fn test_receive_forward_before_header() {
        // When a node receives a chunk forward before the chunk header, it should store
        // the forward and use it when it receives the header
        let mut fixture = ChunkForwardingTestFixture::default();
        let mut shards_manager = ShardsManager::new(
            Some(fixture.mock_shard_tracker.clone()),
            fixture.mock_runtime.clone(),
            fixture.mock_network.clone(),
            TEST_SEED,
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
        shards_manager.insert_forwarded_chunk(forward);
        let partial_encoded_chunk = PartialEncodedChunkV2 {
            header: fixture.mock_chunk_header.clone(),
            parts: other_parts,
            receipts: Vec::new(),
        };
        let result = shards_manager
            .process_partial_encoded_chunk(
                MaybeValidated::from(&partial_encoded_chunk),
                None,
                &mut fixture.chain_store,
                &mut fixture.rs,
            )
            .unwrap();

        match result {
            ProcessPartialEncodedChunkResult::HaveAllPartsAndReceipts => (),
            other_result => panic!("Expected HaveAllPartsAndReceipts, but got {:?}", other_result),
        }
        // No requests should have been sent since all the required parts were contained in the
        // forwarded parts.
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
    fn test_random_seed_with_shard_id() {
        let seed0 = ShardsManager::random_seed(&TEST_SEED, 0);
        let seed10 = ShardsManager::random_seed(&TEST_SEED, 10);
        let seed256 = ShardsManager::random_seed(&TEST_SEED, 256);
        let seed1000 = ShardsManager::random_seed(&TEST_SEED, 1000);
        let seed1000000 = ShardsManager::random_seed(&TEST_SEED, 1_000_000);
        assert_ne!(seed0, seed10);
        assert_ne!(seed0, seed256);
        assert_ne!(seed0, seed1000);
        assert_ne!(seed0, seed1000000);
        assert_ne!(seed10, seed256);
        assert_ne!(seed10, seed1000);
        assert_ne!(seed10, seed1000000);
        assert_ne!(seed256, seed1000);
        assert_ne!(seed256, seed1000000);
        assert_ne!(seed1000, seed1000000);
    }
}
