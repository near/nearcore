use super::*;
use crate::spice::tests::save_and_record_block;
use itertools::Itertools;
use near_async::time::{Clock, Duration, FakeClock};
use near_chain::{Block, Chain, ChainStoreAccess};
use near_chain_configs::MutableConfigValue;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::spice::partial_data::SpiceDataPart;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::types::{ChunkExecutionResult, SpiceChunkId, SpiceUncertifiedChunkInfo};
use near_primitives::utils::get_execution_results_key;
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

mod batching;
mod delivery;
mod encoded_length;
mod lifecycle;
mod orphans;
mod recovery;
mod retries;
mod witnesses;

/// A block on `prev` whose core statements certify `chunk_ids`. Not validated or processed.
fn certifying_block(prev: &Block, chunk_ids: &[SpiceChunkId]) -> Arc<Block> {
    let statements = chunk_ids
        .iter()
        .map(|chunk_id| SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: chunk_id.clone(),
            execution_result: ChunkExecutionResult {
                chunk_extra: ChunkExtra::new_with_only_state_root(&chunk_id.block_hash),
                outgoing_receipts_root: CryptoHash::default(),
            },
        })
        .collect();
    let signer = Arc::new(create_test_signer("test1"));
    TestBlockBuilder::from_prev_block(Clock::real(), prev, signer)
        .spice_core_statements(statements)
        .build()
}

impl SpiceDataManager {
    /// Receives `parts` for `id` in a message `sender`'s test signer signed.
    fn receive_from(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) -> Result<PartsOutcome, SenderFault> {
        let signer = create_test_signer(sender.as_str());
        self.on_parts_received(signed_by(&signer, id, commitment, parts))
    }
}

/// Each producer pulled from for its own ordinal.
fn own_ordinal_pulls() -> BTreeMap<AccountId, BTreeSet<u64>> {
    producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| (producer, ordinals(&[ordinal as u64])))
        .collect()
}

type WantsByProducer = BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>>;

fn by_producer(requests: Vec<PullRequest>) -> WantsByProducer {
    let mut by_producer = WantsByProducer::new();
    for PullRequest { producer, wants } in requests {
        assert!(by_producer.insert(producer, wants).is_none(), "two requests to one producer");
    }
    by_producer
}

/// The requests for `id`, as `producer -> ordinals`.
fn wants_for(requests: Vec<PullRequest>, id: &DataId) -> BTreeMap<AccountId, BTreeSet<u64>> {
    by_producer(requests)
        .into_iter()
        .filter_map(|(producer, mut wants)| Some((producer, wants.remove(id)?)))
        .collect()
}

/// A manager whose node applies one shard, shard 1 unless built with `applying`, as a chunk producer
/// of it: the proof from every other shard into that one is pushed to it, and it needs it unless
/// that proof is on disk. On the two-shard chain that is `(0 -> 1)` only. Nothing is certified until
/// `certify_up_to` says so, and nothing is finally executed until
/// `set_final_execution_head` says so. Time stands still until `clock` is advanced.
struct TestManager {
    manager: SpiceDataManager,
    clock: FakeClock,
    store: Store,
    /// The highest height `certify_up_to` certified.
    certified_height: BlockHeight,
    shard_ids: Vec<ShardId>,
}

impl TestManager {
    fn new(chain: &Chain) -> Self {
        Self::applying(chain, 1)
    }

    fn applying(chain: &Chain, shard: u64) -> Self {
        Self::with_pull_config(chain, shard, PullConfig::default())
    }

    /// A manager whose node produces no chunk and applies `shard` without a push reaching it.
    fn pull_only(chain: &Chain, shard: u64) -> Self {
        Self::with_shard_tracker(
            chain,
            applying_shard_tracker(chain, shard),
            Some(account("observer.near")),
            PullConfig::default(),
        )
    }

    fn with_pull_config(chain: &Chain, shard: u64, pull_config: PullConfig) -> Self {
        Self::with_shard_tracker(
            chain,
            applying_shard_tracker(chain, shard),
            Some(shard_producer(shard, 0)),
            pull_config,
        )
    }

    fn with_shard_tracker(
        chain: &Chain,
        shard_tracker: ShardTracker,
        me: Option<AccountId>,
        pull_config: PullConfig,
    ) -> Self {
        let store = chain.chain_store.store();
        let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        Self {
            manager: SpiceDataManager::new(
                pull_config,
                store.chain_store(),
                chain.epoch_manager.clone(),
                shard_tracker,
                MutableConfigValue::new(
                    me.map(|me| Arc::new(create_test_signer(me.as_str()))),
                    "validator_signer",
                ),
            ),
            clock: FakeClock::default(),
            store,
            certified_height: 0,
            shard_ids: shard_layout.shard_ids().collect(),
        }
    }

    /// Certifies every shard's chunks of every canonical block up to `height`, as the block after
    /// it would: the stored uncertified chunks of the canonical blocks above it drop them, and the
    /// manager applies the core statements of a block certifying them.
    fn certify_up_to(&mut self, height: BlockHeight) {
        let chain_store = self.store.chain_store();
        let chunk_ids: Vec<SpiceChunkId> = (1..=height)
            .filter_map(|height| chain_store.get_block_hash_by_height(height).ok())
            .flat_map(|block_hash| {
                self.shard_ids.iter().map(move |&shard_id| SpiceChunkId { block_hash, shard_id })
            })
            .collect();
        let head = chain_store.get_block(&chain_store.head().unwrap().last_block_hash).unwrap();
        self.drop_uncertified_chunks(height + 1..=head.header().height(), &chunk_ids);
        self.manager.apply_core_statements(&certifying_block(&head, &chunk_ids)).unwrap();
        self.certified_height = self.certified_height.max(height);
    }

    /// Removes `chunk_ids` from the stored uncertified chunks of the canonical blocks at `heights`.
    fn drop_uncertified_chunks(
        &self,
        heights: RangeInclusive<BlockHeight>,
        chunk_ids: &[SpiceChunkId],
    ) {
        self.edit_uncertified_chunks(heights, |_, row| {
            row.retain(|info| !chunk_ids.contains(&info.chunk_id))
        });
    }

    /// Rewrites the stored uncertified chunks of the canonical blocks at `heights` with `edit`,
    /// given each block's height. The column is written once, so each row is deleted before it is
    /// rewritten.
    fn edit_uncertified_chunks(
        &self,
        heights: RangeInclusive<BlockHeight>,
        edit: impl Fn(BlockHeight, &mut Vec<SpiceUncertifiedChunkInfo>),
    ) {
        let chain_store = self.store.chain_store();
        let col = DBCol::uncertified_chunks();
        let mut rows = Vec::new();
        for height in heights {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else {
                continue;
            };
            let Some(mut row) =
                self.store.get_ser::<Vec<SpiceUncertifiedChunkInfo>>(col, block_hash.as_ref())
            else {
                continue;
            };
            edit(height, &mut row);
            rows.push((block_hash, row));
        }
        let mut deletes = self.store.store_update();
        for (block_hash, _) in &rows {
            deletes.delete(col, block_hash.as_ref());
        }
        deletes.commit();
        let mut inserts = self.store.store_update();
        for (block_hash, row) in &rows {
            inserts.insert_ser(col, block_hash.as_ref(), row);
        }
        inserts.commit();
    }

    /// A node with `me`'s account that applies `shard`.
    fn as_validator(chain: &Chain, shard: u64, me: AccountId) -> Self {
        Self::with_shard_tracker(
            chain,
            applying_shard_tracker(chain, shard),
            Some(me),
            PullConfig::default(),
        )
    }

    fn set_final_execution_head(&self, block: &Block) {
        set_final_execution_head(&self.store, block);
    }

    /// Applies `change` to the proofs into `shard`, as the chunk executor sends it, and returns the
    /// pull requests.
    fn change_needs(&mut self, shard: u64, change: NeedChange) -> Vec<PullRequest> {
        self.manager.on_receipt_proof_needs(ShardId::new(shard), change, self.clock.now())
    }

    /// The chunk executor misses the proofs from `sources` into `shard`.
    fn need(&mut self, shard: u64, sources: &[SpiceChunkId]) -> Vec<PullRequest> {
        self.change_needs(shard, NeedChange::Needed(sources.to_vec()))
    }

    /// Writes `block` to the store as the consensus final head.
    fn set_final_head(&self, block: &Block) {
        let mut store_update = self.store.store_update();
        store_update.chain_store_update().set_final_head(&Tip::from_header(block.header()));
        store_update.commit();
    }

    fn track_block(&mut self, block: &Block) {
        self.manager.track_block(block).unwrap();
    }

    /// The chunk of `shard` in `block`, with its execution result stored as its certification does.
    fn certified_chunk(&self, block: &Block, shard: u64) -> SpiceChunkId {
        let chunk_id = SpiceChunkId { block_hash: *block.hash(), shard_id: ShardId::new(shard) };
        let execution_result = ChunkExecutionResult {
            chunk_extra: ChunkExtra::new_with_only_state_root(block.hash()),
            outgoing_receipts_root: CryptoHash::default(),
        };
        let mut store_update = self.store.store_update();
        store_update.insert_ser(
            DBCol::execution_results(),
            &get_execution_results_key(block.hash(), chunk_id.shard_id),
            &execution_result,
        );
        store_update.commit();
        chunk_id
    }

    /// The heights holding tracked receipt proofs, lowest first.
    fn tracked_proof_heights(&self) -> Vec<BlockHeight> {
        self.manager
            .items
            .ids_in(..)
            .filter(|(_, id)| matches!(id, DataId::ReceiptProof(_)))
            .map(|(height, _)| height)
            .sorted()
            .dedup()
            .collect()
    }

    fn is_tracking(&self, id: &DataId) -> bool {
        self.manager.is_tracking(id)
    }

    fn pulling_from(&self, id: &DataId) -> HashSet<AccountId> {
        self.manager.pulls.pulling_from(id, self.clock.now())
    }

    fn is_pullable(&self, id: &DataId) -> bool {
        self.manager.items.is_pullable(id)
    }

    /// Asserts no item and no index entry is left for `id`.
    fn assert_forgotten(&self, id: &DataId) {
        assert!(!self.is_tracking(id));
        assert!(!self.manager.items.ids_in(..).any(|(_, indexed)| indexed == id));
        assert!(!self.is_pullable(id));
        assert!(!self.manager.pulls.holds_state_for(id));
        assert!(!self.manager.delivered_items.contains(id));
        assert!(!self.manager.items.done_ids_in(..).any(|indexed| indexed == id));
    }

    /// Tracks `block` and returns the id of the one proof the policy needs from it.
    fn track_needed_proof(&mut self, block: &Block) -> DataId {
        self.track_block(block);
        receipt_id(block, 0, 1)
    }

    /// `block` was processed at the clock's current time.
    fn on_block_processed(&mut self, block: &Block) -> Vec<PullRequest> {
        // a block's chunks are certified only by a later block
        assert!(
            block.header().height() > self.certified_height,
            "processed block at {} is not above the certified height {}",
            block.header().height(),
            self.certified_height
        );
        self.manager.on_block_processed(block.hash(), self.clock.now()).pull_requests
    }

    /// Delivers `parts` and asserts the item keeps collecting.
    fn push_and_assert_collecting(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) {
        let result = self.manager.receive_from(sender, id, commitment, parts).unwrap();
        assert_matches!(result, PartsOutcome::Collecting);
    }

    /// Delivers enough parts of `data` from `sender` to decode `id`.
    fn deliver(&mut self, sender: &AccountId, id: &DataId, data: &SpiceData) {
        let (commitment, mut parts) = encode_to_wire(&encoder(), data);
        parts.truncate(DATA_PARTS);
        let result = self.manager.receive_from(sender, id, &commitment, parts).unwrap();
        assert_matches!(result, PartsOutcome::Decoded(decoded) if decoded == decoded_data(id, data.clone()));
    }

    fn item(&self, id: &DataId) -> &FetchItem {
        self.manager.items.get(id).unwrap_or_else(|| panic!("no item for {id:?}"))
    }

    fn bound_commitment(&self, id: &DataId, producer: &AccountId) -> Option<&SpiceDataCommitment> {
        let item = self.item(id);
        let index = item
            .producer_index(producer)
            .unwrap_or_else(|| panic!("{producer} is not a producer of {id:?}"));
        item.producers[index].commitment.as_ref()
    }

    fn tracker(&self, id: &DataId, commitment: &SpiceDataCommitment) -> &CodedTracker {
        match self.item(id).commitments.get(commitment) {
            Some(CommitmentState::Tracking(tracker)) => tracker,
            other => panic!("commitment is not tracked: {other:?}"),
        }
    }
}

/// One item from each of `blocks`, tracked and made pullable.
fn pullable_items(manager: &mut TestManager, blocks: &[Arc<Block>]) -> Vec<DataId> {
    let ids = blocks.iter().map(|block| manager.track_needed_proof(block)).collect();
    manager.certify_up_to(blocks.last().unwrap().header().height());
    ids
}

/// Well-formed wire parts under a commitment whose bytes decode to nothing.
fn encode_garbage_to_wire(encoded_length: usize) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
    let parts = (0..TOTAL_PARTS).map(|_| vec![0xff; part_length].into_boxed_slice()).collect();
    wire_parts(parts, encoded_length as u64, CryptoHash::default())
}

/// The ids of the receipt proofs pushed to `me` that a node with `shard_tracker` needs from `block`.
fn needed_receipt_ids(
    chain: &Chain,
    shard_tracker: &ShardTracker,
    me: &AccountId,
    block: &Block,
) -> HashSet<DataId> {
    receipt_proof::needed_items(
        chain.epoch_manager.as_ref(),
        shard_tracker,
        Some(me),
        block.header(),
    )
    .unwrap()
    .into_iter()
    .collect()
}
