use super::*;
use crate::spice::chunk_executor_actor::save_receipt_proof;
use itertools::Itertools;
use near_async::time::{Clock, Duration, FakeClock};
use near_chain::test_utils::{get_chain_with_num_shards, process_block_sync};
use near_chain::{Block, BlockProcessingArtifact, Chain, ChainStoreAccess, Provenance};
use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Tip;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::block_header::BlockHeader;
use near_primitives::spice::partial_data::SpiceDataPart;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::types::{ChunkExecutionResult, SpiceChunkId};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{ShardUId, Store};
use std::collections::{BTreeMap, BTreeSet};

mod batching;
mod delivery;
mod lifecycle;
mod recovery;
mod retries;

/// A two-shard chain with `num_blocks` processed empty blocks; `blocks[i]` is at
/// height `i + 1`.
fn chain_with_blocks(num_blocks: usize) -> (Chain, Vec<Arc<Block>>) {
    let mut chain = get_chain_with_num_shards(Clock::real(), 2);
    let genesis_hash = chain.chain_store.head().unwrap().last_block_hash;
    let mut prev = chain.chain_store.get_block(&genesis_hash).unwrap();
    let mut blocks = Vec::new();
    for _ in 0..num_blocks {
        let block = process_block_at(&mut chain, &prev, prev.header().height() + 1);
        blocks.push(block.clone());
        prev = block;
    }
    (chain, blocks)
}

/// Builds and processes a block on top of `prev` at `height`; the chain keeps it
/// whether or not it becomes the head.
fn process_block_at(chain: &mut Chain, prev: &Block, height: BlockHeight) -> Arc<Block> {
    let signer = Arc::new(create_test_signer("test1"));
    let block =
        TestBlockBuilder::from_prev_block(Clock::real(), prev, signer).height(height).build();
    process_block_sync(
        chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    block
}

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

/// Writes `block` and its header to the chain's store, as block processing would.
fn save_block(chain: &mut Chain, block: &Arc<Block>) {
    let mut store_update = chain.mut_chain_store().store_update();
    store_update.save_block_header(block.header().clone()).unwrap();
    store_update.save_block(block.clone());
    store_update.commit().unwrap();
}

/// The chain's policies with fixed producer lists in place of the chain's single
/// chunk producer and extra receipt-proof (from, to) pairs.
struct TestPolicy {
    chain_policies: Policies,
    producers: Vec<AccountId>,
    /// Producers for items from these shards, in place of `producers`.
    producers_by_from_shard: HashMap<u64, Vec<AccountId>>,
    /// `(from_shard, to_shard)` pairs needed from every block on top of the chain's.
    extra_pairs: Vec<(u64, u64)>,
    /// Blocks whose `needed_items` fails.
    failing_blocks: HashSet<CryptoHash>,
}

impl DataPolicy for TestPolicy {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        if self.failing_blocks.contains(block.hash()) {
            return Err(Error::Other("no items".to_string()));
        }
        let chain_ids = self.chain_policies.needed_items(block)?.into_iter().map(|(id, _)| id);
        let extra_ids = self.extra_pairs.iter().map(|(from_shard, to_shard)| {
            DataId::receipt_proof(*block.hash(), ShardId::new(*from_shard), ShardId::new(*to_shard))
        });
        Ok(chain_ids
            .chain(extra_ids)
            .map(|id| {
                let DataId::ReceiptProof { source, .. } = &id;
                let from_shard: u64 = source.shard_id.into();
                let producers =
                    self.producers_by_from_shard.get(&from_shard).unwrap_or(&self.producers);
                (id, producers.clone())
            })
            .collect())
    }

    fn is_done(&self, id: &DataId) -> bool {
        self.chain_policies.is_done(id)
    }

    fn chunks_to_certify_before_pull(&self, id: &DataId) -> Vec<SpiceChunkId> {
        self.chain_policies.chunks_to_certify_before_pull(id)
    }
}

/// The producers of every item here; one per part.
fn producers() -> Vec<AccountId> {
    (0..TOTAL_PARTS).map(|i| account(&format!("producer{i}.near"))).collect()
}

fn ordinals(ordinals: &[u64]) -> BTreeSet<u64> {
    ordinals.iter().copied().collect()
}

type WantsByProducer = BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>>;

fn by_producer(requests: Vec<PullRequest>) -> WantsByProducer {
    let mut by_producer = WantsByProducer::new();
    for PullRequest { producer, wants } in requests {
        assert!(by_producer.insert(producer, wants).is_none(), "two requests to one producer");
    }
    by_producer
}

/// The requests as `producer -> ordinals` for the one item `id`.
fn wants_for(requests: Vec<PullRequest>, id: &DataId) -> BTreeMap<AccountId, BTreeSet<u64>> {
    by_producer(requests)
        .into_iter()
        .map(|(producer, mut wants)| {
            let ordinals = wants.remove(id).unwrap_or_else(|| panic!("no wants for {id:?}"));
            assert!(wants.is_empty(), "wants for other items: {wants:?}");
            (producer, ordinals)
        })
        .collect()
}

/// A manager whose policy applies shard 1 only: of a block's four proofs it needs
/// `(0 -> 1)`, unless that proof is on disk. Nothing is certified until
/// `certify_up_to` says so, and nothing is finally executed until
/// `set_final_execution_head` says so. Time stands still until `clock` is advanced.
struct TestManager {
    manager: SpiceDataManager<TestPolicy>,
    clock: FakeClock,
    store: Store,
    /// The highest height `certify_up_to` certified.
    certified_height: BlockHeight,
}

impl TestManager {
    fn new(chain: &Chain) -> Self {
        let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        let tracked = ShardUId::from_shard_id_and_layout(ShardId::new(1), &shard_layout);
        let shard_tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(vec![tracked]),
            chain.epoch_manager.clone(),
            MutableConfigValue::new(None, "validator_signer"),
        );
        let store = chain.chain_store.store();
        let policies =
            Policies::new(store.chain_store(), chain.epoch_manager.clone(), shard_tracker);
        let policy = TestPolicy {
            chain_policies: policies,
            producers: producers(),
            producers_by_from_shard: HashMap::new(),
            extra_pairs: Vec::new(),
            failing_blocks: HashSet::new(),
        };
        Self {
            manager: SpiceDataManager::new(
                PullConfig::default(),
                DATA_PARTS_RATIO,
                store.chain_store(),
                policy,
            ),
            clock: FakeClock::default(),
            store,
            certified_height: 0,
        }
    }

    /// Records a block certifying both shards' chunks of every canonical block up to
    /// `height`, as tracking it would.
    fn certify_up_to(&mut self, height: BlockHeight) {
        let chain_store = self.store.chain_store();
        let chunk_ids: Vec<SpiceChunkId> = (1..=height)
            .filter_map(|height| chain_store.get_block_hash_by_height(height).ok())
            .flat_map(|block_hash| {
                [0, 1].map(|shard| SpiceChunkId { block_hash, shard_id: ShardId::new(shard) })
            })
            .collect();
        let head = chain_store.get_block(&chain_store.head().unwrap().last_block_hash).unwrap();
        self.manager.record_chunks_certified_by(&certifying_block(&head, &chunk_ids)).unwrap();
        self.certified_height = self.certified_height.max(height);
    }

    /// Writes `block` to the store as the final execution head.
    fn set_final_execution_head(&self, block: &Block) {
        let mut store_update = self.store.store_update();
        store_update
            .chain_store_update()
            .set_spice_final_execution_head(&Tip::from_header(block.header()));
        store_update.commit();
    }

    /// The policy also needs the proofs `(from_shard -> to_shard)` in `pairs`.
    fn set_extra_pairs(&mut self, pairs: Vec<(u64, u64)>) {
        self.manager.policies.extra_pairs = pairs;
    }

    fn set_pull_config(&mut self, config: PullConfig) {
        self.manager.pull_config = config;
    }

    fn track_block(&mut self, block: &Block) {
        self.manager.track_block(block).unwrap();
    }

    fn is_tracking(&self, id: &DataId) -> bool {
        self.manager.is_tracking(id)
    }

    /// Tracks `block` and returns the id of the one proof the policy needs from it.
    fn track_needed_proof(&mut self, block: &Block) -> DataId {
        self.track_block(block);
        receipt_id(block, 0, 1)
    }

    /// Items from `from_shard` are served by `producers` instead of the default list.
    fn set_producers_for(&mut self, from_shard: u64, producers: Vec<AccountId>) {
        self.manager.policies.producers_by_from_shard.insert(from_shard, producers);
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
        self.manager.on_block_processed(block.hash(), self.clock.now())
    }

    /// Delivers `parts` and asserts the item keeps collecting.
    fn push_and_assert_collecting(
        &mut self,
        sender: &AccountId,
        id: &DataId,
        commitment: &SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
    ) {
        let result =
            self.manager.on_parts_received(sender, id, commitment, parts, TOTAL_PARTS).unwrap();
        assert_matches!(result, PartsOutcome::Collecting);
    }

    /// Delivers enough parts of `data` from `sender` to decode `id`.
    fn deliver(&mut self, sender: &AccountId, id: &DataId, data: &SpiceData) {
        let (commitment, mut parts) = encode_to_wire(&encoder(), data);
        parts.truncate(DATA_PARTS);
        let result =
            self.manager.on_parts_received(sender, id, &commitment, parts, TOTAL_PARTS).unwrap();
        assert_matches!(result, PartsOutcome::Decoded(decoded) if &decoded == data);
    }

    fn item(&self, id: &DataId) -> &FetchItem {
        self.manager.items.get(id).unwrap_or_else(|| panic!("no item for {id:?}"))
    }

    fn state(&self, id: &DataId, producer: &AccountId) -> &ProducerState {
        let (_, state) = self
            .item(id)
            .producers
            .iter()
            .find(|(account, _)| account == producer)
            .unwrap_or_else(|| panic!("{producer} is not a producer of {id:?}"));
        state
    }

    fn tracker(&self, id: &DataId, commitment: &SpiceDataCommitment) -> &CodedTracker {
        match self.item(id).commitments.get(commitment) {
            Some(CommitmentState::Tracking(tracker)) => tracker,
            other => panic!("commitment is not tracked: {other:?}"),
        }
    }
}

fn receipt_id(block: &Block, from_shard: u64, to_shard: u64) -> DataId {
    DataId::receipt_proof(*block.hash(), ShardId::new(from_shard), ShardId::new(to_shard))
}

/// Encodes `data` into wire parts under its commitment.
fn encode_to_wire(
    encoder: &Arc<ReedSolomonEncoder>,
    data: &SpiceData,
) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let (parts, encoded_length) = encoder.encode(data);
    let parts: Vec<Box<[u8]>> = parts.into_iter().map(Option::unwrap).collect();
    wire_parts(parts, encoded_length as u64, hash(&borsh::to_vec(data).unwrap()))
}

/// Well-formed wire parts under a commitment whose bytes decode to nothing.
fn encode_garbage_to_wire(encoded_length: usize) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
    let parts = (0..TOTAL_PARTS).map(|_| vec![0xff; part_length].into_boxed_slice()).collect();
    wire_parts(parts, encoded_length as u64, CryptoHash::default())
}

fn wire_parts(
    parts: Vec<Box<[u8]>>,
    encoded_length: u64,
    data_hash: CryptoHash,
) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let (root, proofs) = merklize(&parts);
    let commitment = SpiceDataCommitment { hash: data_hash, root, encoded_length };
    let parts = parts
        .into_iter()
        .zip(proofs)
        .enumerate()
        .map(|(ordinal, (part, merkle_proof))| SpiceDataPart {
            part_ord: ordinal as u64,
            part,
            merkle_proof,
        })
        .collect();
    (commitment, parts)
}

/// The wire parts with the given ordinals.
fn parts_with_ordinals(parts: &[SpiceDataPart], ordinals: &[u64]) -> Vec<SpiceDataPart> {
    parts.iter().filter(|part| ordinals.contains(&part.part_ord)).cloned().collect()
}

fn save_proof(chain: &Chain, block: &Block, data: &SpiceData) {
    let SpiceData::ReceiptProof(proof) = data else { panic!("not a receipt proof") };
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), proof);
    store_update.commit();
}
