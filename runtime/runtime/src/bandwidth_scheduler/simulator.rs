//! This is a blockchain simulation used for testing the bandwidth scheduler. There are blocks and
//! chunks (both can be missing). Chunks create receipts and send them to each other. Bandwidth
//! scheduler is run during chunk application to determine how many outgoing receipts a chunk is
//! able to produce. It doesn't take into account congestion control, gas or witness limits, it's a
//! lightweight simulation used to directly test the scheduling algorithm. Having a dedicated
//! simulation makes it much easier to test the algorithm itself. There is no interference from
//! various limits and quirks of the load shedding algorithms that are present in the real
//! blockchain. It's also much faster to run a lightweight simulation than a whole chain, allowing
//! us to test more scenarios without being constrained by the available resources.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::rc::Rc;

use borsh::BorshSerialize;
use bytesize::ByteSize;
use near_primitives::bandwidth_scheduler::{
    BandwidthRequest, BandwidthRequests, BandwidthRequestsV1, BandwidthSchedulerParams,
    BandwidthSchedulerState, BandwidthSchedulerStateV1, BlockBandwidthRequests,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{BlockHeight, ShardId, ShardIndex};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

use super::scheduler::{BandwidthScheduler, ShardStatus};
use testlib::bandwidth_scheduler::{
    ChunkBandwidthStats, LargeReceiptSizeGenerator, MaxReceiptSizeGenerator,
    MediumReceiptSizeGenerator, RandomReceiptSizeGenerator, SmallReceiptSizeGenerator,
    TestBandwidthStats, TestScenario, TestScenarioBuilder, TestSummary,
};

/// Main struct which keeps all of the simulation data
struct ChainSimulator {
    shard_layout: ShardLayout,
    blocks: BTreeMap<BlockHeight, Option<Rc<Block>>>,
    // State of the shard after applying a chunk at some height
    shard_states: BTreeMap<(BlockHeight, ShardId), ShardState>,
    // Result of applying a chunk at some height
    application_results: BTreeMap<(BlockHeight, ShardId), ChunkApplicationResult>,
    rng: ChaCha20Rng,
    next_block_height: BlockHeight,
    scenario: TestScenario,
    chunk_stats: BTreeMap<(BlockHeight, ShardIndex), ChunkBandwidthStats>,
}

/// Simulated block
#[derive(Debug, Clone, BorshSerialize)]
struct Block {
    chunks: BTreeMap<ShardId, NewOrOld<Rc<Chunk>>>,
}

/// New or old (missing) chunk
/// Not using MaybeNew because it needs to own the value
#[derive(Debug, Clone, BorshSerialize)]
enum NewOrOld<T> {
    New(T),
    Old(T),
}

impl<T> NewOrOld<T> {
    fn get(&self) -> &T {
        match self {
            NewOrOld::New(t) => t,
            NewOrOld::Old(t) => t,
        }
    }
}

/// Simulated chunk
#[derive(Debug, Clone, BorshSerialize)]
struct Chunk {
    /// Outgoing receipts to other shards generated during the previous chunk application on this shard.
    prev_outgoing_receipts: Rc<BTreeMap<ShardId, Vec<SimReceipt>>>,
    bandwidth_requests: BandwidthRequests,
}

/// Current state of some shard in the simulation.
#[derive(Debug, Clone, BorshSerialize)]
struct ShardState {
    buffered_outgoing_receipts: BTreeMap<ShardId, VecDeque<SimReceipt>>,
    scheduler_state: BandwidthSchedulerState,
}

/// Simulated receipt, represents a receipt with some size.
#[derive(Debug, Clone, BorshSerialize)]
struct SimReceipt {
    size: u64,
}

#[derive(Debug)]
struct ChunkApplicationResult {
    outgoing_receipts: Rc<BTreeMap<ShardId, Vec<SimReceipt>>>,
    bandwidth_requests: BandwidthRequests,
    pre_state_hash: CryptoHash,
    post_state_hash: CryptoHash,
}

impl ChainSimulator {
    fn new(scenario: TestScenario) -> Self {
        let shard_layout = ShardLayout::multi_shard(scenario.num_shards, 0);
        let mut genesis_block = Block { chunks: BTreeMap::new() };
        for shard_id in shard_layout.shard_ids() {
            genesis_block.chunks.insert(
                shard_id,
                NewOrOld::New(Rc::new(Chunk {
                    prev_outgoing_receipts: Rc::new(BTreeMap::new()),
                    bandwidth_requests: BandwidthRequests::empty(),
                })),
            );
        }
        let mut blocks = BTreeMap::new();
        blocks.insert(0, Some(Rc::new(genesis_block)));

        let rng = ChaCha20Rng::seed_from_u64(0);

        Self {
            blocks,
            shard_states: BTreeMap::new(),
            application_results: BTreeMap::new(),
            shard_layout,
            rng,
            next_block_height: 1,
            scenario,
            chunk_stats: BTreeMap::new(),
        }
    }

    fn run_for(&mut self, num_blocks: usize) {
        for _ in 0..num_blocks {
            self.produce_next_block();
        }
        self.assert_pre_post_state();
    }

    fn produce_next_block(&mut self) {
        let new_block_height = self.next_block_height;
        self.next_block_height += 1;

        let is_block_missing = self.rng.gen_bool(self.scenario.missing_block_probability);
        if is_block_missing {
            self.blocks.insert(new_block_height, None);
            return;
        }

        let mut previous_block_height = new_block_height - 1;
        while self.blocks.get(&previous_block_height).unwrap().is_none() {
            previous_block_height -= 1;
        }

        let previous_block = self.blocks.get(&previous_block_height).unwrap().clone().unwrap();

        let mut new_block = Block { chunks: BTreeMap::new() };
        for (shard_id, chunk) in &previous_block.chunks {
            self.apply_chunk(previous_block_height, *shard_id);
            let is_chunk_missing = self.rng.gen_bool(self.scenario.missing_chunk_probability);
            if is_chunk_missing {
                new_block.chunks.insert(*shard_id, NewOrOld::Old(chunk.get().clone()));
            } else {
                let new_chunk = self.produce_new_chunk(new_block_height, *shard_id);
                new_block.chunks.insert(*shard_id, NewOrOld::New(Rc::new(new_chunk)));
            }
        }

        self.blocks.insert(new_block_height, Some(Rc::new(new_block)));
    }

    fn apply_chunk(&mut self, height: BlockHeight, shard_id: ShardId) {
        // Load the current shard state
        let mut shard_state_opt = None;
        for previous_height in (0..height).rev() {
            if let Some(state) = self.shard_states.get(&(previous_height, shard_id)) {
                shard_state_opt = Some(state.clone());
                break;
            }
        }
        let mut shard_state = shard_state_opt.unwrap_or_else(|| ShardState {
            buffered_outgoing_receipts: BTreeMap::new(),
            scheduler_state: BandwidthSchedulerState::V1(BandwidthSchedulerStateV1 {
                link_allowances: Vec::new(),
                sanity_check_hash: CryptoHash::default(),
            }),
        });
        let pre_state_hash = CryptoHash::hash_borsh(&shard_state);

        // Find the block that contains this chunk
        let current_block = self.blocks.get(&height).unwrap().clone().unwrap();

        // Collect all bandwidth requests in the current block
        let mut shards_bandwidth_requests: BTreeMap<ShardId, BandwidthRequests> = BTreeMap::new();
        for (chunk_shard_id, chunk) in &current_block.chunks {
            shards_bandwidth_requests
                .insert(*chunk_shard_id, chunk.get().bandwidth_requests.clone());
        }
        let block_bandwidth_requests = BlockBandwidthRequests { shards_bandwidth_requests };

        // Run bandwidth scheduler to generate bandwidth grants
        let shards: Vec<ShardId> = self.shard_layout.shard_ids().collect();
        let scheduler_params = self.scheduler_params();
        let shards_status: BTreeMap<ShardId, ShardStatus> = shards
            .iter()
            .map(|chunk_shard_id| {
                let is_missing = match current_block.chunks.get(chunk_shard_id).unwrap() {
                    NewOrOld::New(_) => false,
                    NewOrOld::Old(_) => true,
                };

                let status = ShardStatus {
                    is_fully_congested: false,
                    last_chunk_missing: is_missing,
                    allowed_sender_shard_index: None,
                };

                (*chunk_shard_id, status)
            })
            .collect();
        let fake_prev_block_hash = CryptoHash::hash_borsh(height);
        let scheduler_output = BandwidthScheduler::run(
            self.shard_layout.clone(),
            &mut shard_state.scheduler_state,
            &scheduler_params,
            &block_bandwidth_requests,
            &shards_status,
            fake_prev_block_hash.0,
        );

        // Don't process receipts for missing chunks, return early if a chunk is missing
        let chunk_is_missing = match current_block.chunks.get(&shard_id).unwrap() {
            NewOrOld::New(_) => false,
            NewOrOld::Old(_) => true,
        };
        if chunk_is_missing {
            self.application_results.insert(
                (height, shard_id),
                ChunkApplicationResult {
                    outgoing_receipts: Rc::new(BTreeMap::new()),
                    bandwidth_requests: BandwidthRequests::empty(),
                    pre_state_hash,
                    post_state_hash: CryptoHash::hash_borsh(&shard_state),
                },
            );
            self.shard_states.insert((height, shard_id), shard_state);
            return;
        }

        let mut stats = ChunkBandwidthStats::new();
        stats.congestion_level = 0.0;

        let incoming_receipts = self.get_incoming_receipts_for_chunk(height, shard_id);
        stats.total_incoming_receipts_size =
            ByteSize::b(incoming_receipts.iter().map(|r| r.size).sum());

        // Define outgoing limits for this shard using bandwidth grants
        let mut outgoing_limits: BTreeMap<ShardId, u64> = BTreeMap::new();
        for receiver_shard in self.shard_layout.shard_ids() {
            outgoing_limits.insert(
                receiver_shard,
                scheduler_output.get_granted_bandwidth(shard_id, receiver_shard),
            );
        }

        let mut outgoing_receipts: BTreeMap<ShardId, Vec<SimReceipt>> = BTreeMap::new();

        // Forward buffered receipts
        for (receiver, buffered_receipts) in &mut shard_state.buffered_outgoing_receipts {
            let outgoing_limit = outgoing_limits.get_mut(receiver).unwrap();
            'inner: while let Some(first_receipt) = buffered_receipts.front() {
                if *outgoing_limit >= first_receipt.size {
                    *outgoing_limit -= first_receipt.size;
                    let receipt = buffered_receipts.pop_front().unwrap();
                    outgoing_receipts.entry(*receiver).or_insert_with(Vec::new).push(receipt);
                } else {
                    break 'inner;
                }
            }
        }

        // Generate and send out new receipts
        let sender_idx = self.shard_layout.get_shard_index(shard_id).unwrap();
        if let Some(links_vec) = self.scenario.link_generators.get_mut(&sender_idx) {
            for (receiver_idx, link_generator) in links_vec.iter_mut() {
                let receiver = self.shard_layout.get_shard_id(*receiver_idx).unwrap();

                let outgoing_limit = outgoing_limits.get_mut(&receiver).unwrap();
                let outgoing_receipts_to_receiver =
                    outgoing_receipts.entry(receiver).or_insert_with(Vec::new);
                let outgoing_buffer = shard_state
                    .buffered_outgoing_receipts
                    .entry(receiver)
                    .or_insert_with(VecDeque::new);
                let mut outgoing_buffer_size: u64 = outgoing_buffer.iter().map(|r| r.size).sum();
                // Produce new receipts until the buffer size exceeds 20MB
                while outgoing_buffer_size < 20_000_000 {
                    let new_receipt_size = link_generator.generate_receipt_size(&mut self.rng);
                    let new_receipt = SimReceipt { size: new_receipt_size.as_u64() };

                    if *outgoing_limit >= new_receipt.size {
                        *outgoing_limit -= new_receipt.size;
                        outgoing_receipts_to_receiver.push(new_receipt);
                    } else {
                        outgoing_buffer_size += new_receipt.size;
                        outgoing_buffer.push_back(new_receipt);
                    }
                }
            }
        }

        for (to_shard_id, outgoing_receipts_to_shard) in &outgoing_receipts {
            let mut total_to_shard = ByteSize::b(0);
            for receipt in outgoing_receipts_to_shard {
                total_to_shard += ByteSize::b(receipt.size);
            }

            let to_shard_index = self.shard_layout.get_shard_index(*to_shard_id).unwrap();
            stats.size_of_outgoing_receipts_to_shard.insert(to_shard_index, total_to_shard);
            stats.total_outgoing_receipts_size += total_to_shard;
        }

        // Generate bandwidth requests based on buffered receipts
        let mut bandwidth_requests = Vec::new();
        for (receiver, buffered_receipts) in &shard_state.buffered_outgoing_receipts {
            let sizes_iter = buffered_receipts.iter().map(|r| Ok::<u64, Infallible>(r.size));
            if let Some(request) =
                BandwidthRequest::make_from_receipt_sizes(*receiver, sizes_iter, &scheduler_params)
                    .unwrap()
            {
                bandwidth_requests.push(request);
            }

            let receiver_idx = self.shard_layout.get_shard_index(*receiver).unwrap();

            let first5: Vec<ByteSize> =
                buffered_receipts.iter().map(|r| ByteSize::b(r.size)).take(5).collect();
            stats.first_five_buffered_sizes.insert(receiver_idx, first5);

            let first5_big: Vec<ByteSize> = buffered_receipts
                .iter()
                .map(|r| ByteSize::b(r.size))
                .filter(|size| *size > ByteSize::kb(500))
                .take(5)
                .collect();
            stats.first_five_big_buffered_sizes.insert(receiver_idx, first5_big);
        }

        let shard_index = self.shard_layout.get_shard_index(shard_id).unwrap();
        self.chunk_stats.insert((height, shard_index), stats);

        // Save the application result and new shard state
        let post_state_hash = CryptoHash::hash_borsh(&shard_state);
        let application_result = ChunkApplicationResult {
            outgoing_receipts: Rc::new(outgoing_receipts),
            bandwidth_requests: BandwidthRequests::V1(BandwidthRequestsV1 {
                requests: bandwidth_requests,
            }),
            pre_state_hash,
            post_state_hash,
        };
        self.application_results.insert((height, shard_id), application_result);
        self.shard_states.insert((height, shard_id), shard_state);
    }

    fn produce_new_chunk(&self, height: BlockHeight, shard_id: ShardId) -> Chunk {
        // Find outgoing receipts generated while applying the previous non-missing chunk on this shard
        let previous_chunk_height = self.get_previous_non_missing_chunk_height(height, shard_id);
        let application_result =
            self.application_results.get(&(previous_chunk_height, shard_id)).unwrap();

        Chunk {
            prev_outgoing_receipts: application_result.outgoing_receipts.clone(),
            bandwidth_requests: application_result.bandwidth_requests.clone(),
        }
    }

    fn get_incoming_receipts_for_chunk(
        &self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Vec<SimReceipt> {
        if height == 0 {
            return Vec::new();
        }

        let previous_chunk_height = self.get_previous_non_missing_chunk_height(height, shard_id);

        let mut incoming_receipts = Vec::new();
        for height in (previous_chunk_height + 1)..=height {
            let Some(block) = self.blocks.get(&height).unwrap() else {
                continue;
            };
            for (_, chunk) in &block.chunks {
                let NewOrOld::New(new_chunk) = chunk else {
                    continue;
                };

                if let Some(receipts) = new_chunk.prev_outgoing_receipts.get(&shard_id) {
                    incoming_receipts.extend(receipts.iter().cloned());
                }
            }
        }

        incoming_receipts
    }

    fn get_previous_non_missing_chunk_height(
        &self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> BlockHeight {
        for height in (0..height).rev() {
            if let Some(block) = self.blocks.get(&height).unwrap() {
                if let NewOrOld::New(_new_chunk) = block.chunks.get(&shard_id).unwrap() {
                    return height;
                }
            }
        }
        panic!("No previous non-missing chunk found for shard {} at height {}", shard_id, height);
    }

    fn scheduler_params(&self) -> BandwidthSchedulerParams {
        // TODO(bandwidth_scheduler) - use current RuntimeConfig?
        BandwidthSchedulerParams::for_test(self.shard_layout.num_shards())
    }

    /// Make sure that post state of previous chunk application is always pre state for the next application.
    /// Sanity check.
    fn assert_pre_post_state(&self) {
        let mut last_state_hash: BTreeMap<ShardId, CryptoHash> = BTreeMap::new();
        for ((_height, shard), application_res) in &self.application_results {
            if let Some(last_hash) = last_state_hash.get(shard) {
                assert_eq!(last_hash, &application_res.pre_state_hash);
            }
            last_state_hash.insert(*shard, application_res.post_state_hash);
        }
    }

    fn stats(self) -> TestBandwidthStats {
        let scheduler_params = self.scheduler_params();
        let num_shards = self.shard_layout.num_shards();
        TestBandwidthStats { chunk_stats: self.chunk_stats, num_shards, scheduler_params }
    }
}

fn run_scenario(scenario: TestScenario) -> TestSummary {
    let blocks_num = 200;
    let active_links = scenario.get_active_links();
    let mut simulator = ChainSimulator::new(scenario);
    simulator.run_for(blocks_num);
    let summary = simulator.stats().summarize(&active_links);
    println!("{}", summary);
    summary
}

/// All links send small receipts
#[test]
fn test_bandwidth_scheduler_simulator_small_receipts() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(5)
        .default_link_generator(|| Box::new(SmallReceiptSizeGenerator))
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.95); // 95% utilization
    assert!(summary.link_imbalance_ratio < 1.05); // < 5% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.95); // 95% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// All links send receipts with random sizes
#[test]
fn test_bandwidth_scheduler_simulator_random_receipts() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(10)
        .default_link_generator(|| Box::new(RandomReceiptSizeGenerator))
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.75); // 75% utilization
    assert!(summary.link_imbalance_ratio < 1.3); // < 30% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.7); // 70% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// All links send only maximum size receipts
#[test]
fn test_bandwidth_scheduler_simulator_max_size_receipts() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(10)
        .default_link_generator(|| Box::new(MaxReceiptSizeGenerator))
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.90); // 90% utilization
    assert!(summary.link_imbalance_ratio < 1.3); // < 30% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.75); // 75% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// Every link has a different ReceiptSizeGenerator, every link sends receipts of different type.
#[test]
fn test_bandwidth_scheduler_simulator_random_link_generators() {
    let num_shards = 10;
    let mut scenario_builder = TestScenarioBuilder::new().num_shards(10);
    let mut rng = ChaCha20Rng::seed_from_u64(0);
    for sender in 0..num_shards {
        for receiver in 0..num_shards {
            match rng.gen_range(0..4) {
                0 => {
                    scenario_builder =
                        scenario_builder.link_generator(sender, receiver, SmallReceiptSizeGenerator)
                }
                1 => {
                    scenario_builder = scenario_builder.link_generator(
                        sender,
                        receiver,
                        MediumReceiptSizeGenerator,
                    )
                }
                2 => {
                    scenario_builder =
                        scenario_builder.link_generator(sender, receiver, LargeReceiptSizeGenerator)
                }
                3 => {
                    scenario_builder = scenario_builder.link_generator(
                        sender,
                        receiver,
                        RandomReceiptSizeGenerator,
                    )
                }
                _ => unreachable!(),
            }
        }
    }
    let scenario = scenario_builder.build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.80); // 80% utilization
    assert!(summary.link_imbalance_ratio < 1.3); // < 30% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.75); // 75% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// 0 -> 0 sends only big receipts
/// 1, 2, 3, 4 -> 0 send small receipts
/// Things should be fair and utilization should be high
#[test]
fn test_bandwidth_scheduler_simulator_small_vs_big() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(5)
        .link_generator(0, 0, LargeReceiptSizeGenerator)
        .link_generator(1, 0, SmallReceiptSizeGenerator)
        .link_generator(2, 0, SmallReceiptSizeGenerator)
        .link_generator(3, 0, SmallReceiptSizeGenerator)
        .link_generator(4, 0, SmallReceiptSizeGenerator)
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.90); // 90% utilization
    assert!(summary.link_imbalance_ratio < 1.06); // < 6% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.90); // 90% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// Run bandwidth scheduler with 32 shards and see what happens.
/// The utilization stays high, but fairness drops a bit. Running the simulation for longer
/// makes the fairness better, but then the test would run forever.
#[test]
fn slow_test_bandwidth_scheduler_simulator_32_shards() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(32)
        .default_link_generator(|| Box::new(RandomReceiptSizeGenerator))
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.80); // 80% utilization
    assert!(summary.link_imbalance_ratio < 1.70); // < 70% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.6); // 60% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// 10% of chunks are missing. How will the scheduler behave?
#[test]
fn test_bandwidth_scheduler_simulator_missing_chunks() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(6)
        .default_link_generator(|| Box::new(RandomReceiptSizeGenerator))
        .missing_chunk_probability(0.1)
        .build();
    let summary = run_scenario(scenario);
    assert!(summary.bandwidth_utilization > 0.7); // > 70% utilization
    assert!(summary.link_imbalance_ratio < 1.6); // < 60% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.50); // 50% of estimated link throughput

    // Incoming max_shard_bandwidth is not respected! When a chunk is missing, the receipts that
    // were sent previously will arrive later and they can mix with other incoming receipts, and the
    // receiver can receive more than max_shard_bandwidth of receipts :/
    // TODO(bandwidth_scheduler) - prevent shard from having too many incoming receipts
    assert!(summary.max_incoming > summary.max_shard_bandwidth);

    // Outgoing max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth);
}
