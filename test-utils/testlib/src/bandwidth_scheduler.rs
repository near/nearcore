use std::collections::{BTreeMap, BTreeSet};

use bytesize::ByteSize;
use near_primitives::bandwidth_scheduler::{Bandwidth, BandwidthSchedulerParams};
use near_primitives::types::{BlockHeight, ShardIndex};
use rand::Rng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha20Rng;

const MAX_RECEIPT_SIZE: u64 = 4 * 1024 * 1024;

/// Get a random receipt size for testing.
/// The sizes are sampled from a reasonable distribution where most receipts are small,
/// some are medium sized, and a few are large.
/// See `RandomReceiptSizeGenerator` for the exact implementation.
pub fn get_random_receipt_size_for_test(rng: &mut ChaCha20Rng) -> u64 {
    RandomReceiptSizeGenerator.generate_receipt_size(rng).as_u64()
}

/// Objects with this trait are responsible for generating receipt sizes that are used in testing.
/// Each implementation generates different distribution of sizes.
pub trait ReceiptSizeGenerator: std::fmt::Debug {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize;
}

/// Generates small receipt sizes (< 4kB)
#[derive(Debug)]
pub struct SmallReceiptSizeGenerator;

impl ReceiptSizeGenerator for SmallReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(200..4_000))
    }
}

/// Generates medium receipt sizes.
#[derive(Debug)]
pub struct MediumReceiptSizeGenerator;

impl ReceiptSizeGenerator for MediumReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(4_000..300_000))
    }
}

/// Generates large receipt sizes (> 300kB)
#[derive(Debug)]
pub struct LargeReceiptSizeGenerator;

impl ReceiptSizeGenerator for LargeReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(300_000..=MAX_RECEIPT_SIZE))
    }
}

/// Always generates a maximum size receipt.
#[derive(Debug)]
pub struct MaxReceiptSizeGenerator;

impl ReceiptSizeGenerator for MaxReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, _rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(MAX_RECEIPT_SIZE)
    }
}

/// Generates random receipt sizes from a reasonable distribution.
/// Most receipts are small, some are medium sized, and a few are large.
/// This is more similar to the real-life distribution of receipts.
/// And small receipts are more interesting than large ones, we need
/// a lot of small receipts to properly test receipt groups.
/// Naive sampling of sizes between 0 and 4MB would result in 97.5% of receipts
/// being larger than 100kB, which is undesirable.
#[derive(Debug)]
pub struct RandomReceiptSizeGenerator;

impl ReceiptSizeGenerator for RandomReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        let weighted_sizes = [
            (SmallReceiptSizeGenerator.generate_receipt_size(rng), 70), // 70% of receipts are small
            (MediumReceiptSizeGenerator.generate_receipt_size(rng), 20), // 20% of receipts are medium
            (LargeReceiptSizeGenerator.generate_receipt_size(rng), 8),   // 8% of receipts are large
            (MaxReceiptSizeGenerator.generate_receipt_size(rng), 2), // 2% of receipts are max size
        ];
        weighted_sizes.choose_weighted(rng, |item| item.1).unwrap().0
    }
}

/// A `ReceiptSizeGenerator` for every pair of shards.
/// Every pair of shards that sends receipts has an entry. No entry means that no receipts should be
/// sent between this pair of shards.
/// Sender -> Receiver -> ReceiptSizeGenerator
pub type LinkGenerators = BTreeMap<ShardIndex, Vec<(ShardIndex, Box<dyn ReceiptSizeGenerator>)>>;

/// Bandwidth scheduler test scenario
#[derive(Debug)]
pub struct TestScenario {
    pub num_shards: u64,
    pub link_generators: LinkGenerators,
    pub missing_chunk_probability: f64,
    pub missing_block_probability: f64,
}

impl TestScenario {
    pub fn get_active_links(&self) -> BTreeSet<(ShardIndex, ShardIndex)> {
        let mut res = BTreeSet::new();
        for (sender, links_vec) in &self.link_generators {
            for (receiver, _) in links_vec {
                res.insert((*sender, *receiver));
            }
        }
        res
    }
}

pub struct TestScenarioBuilder {
    scenario: TestScenario,
    default_link_generator_factory: Option<Box<dyn Fn() -> Box<dyn ReceiptSizeGenerator>>>,
}

impl TestScenarioBuilder {
    pub fn new() -> TestScenarioBuilder {
        TestScenarioBuilder {
            scenario: TestScenario {
                num_shards: 0,
                link_generators: BTreeMap::new(),
                missing_block_probability: 0.,
                missing_chunk_probability: 0.,
            },
            default_link_generator_factory: None,
        }
    }

    /// Set number of shards in the test scenario
    pub fn num_shards(mut self, num_shards: u64) -> Self {
        self.scenario.num_shards = num_shards;
        self
    }

    /// Set `ReceiptSizeGenerator` that will be used to generate sizes of receipts sent from `sender` to `receiver`.
    pub fn link_generator(
        mut self,
        sender: ShardIndex,
        receiver: ShardIndex,
        generator: impl ReceiptSizeGenerator + 'static,
    ) -> Self {
        let links_vec = self.scenario.link_generators.entry(sender).or_insert_with(Vec::new);
        assert!(
            links_vec
                .iter()
                .position(|(shard_index, _generator)| *shard_index == receiver)
                .is_none(),
            "Duplicate receipt size generator from {} to {}!",
            sender,
            receiver
        );
        links_vec.push((receiver, Box::new(generator)));
        self
    }

    /// Specify a `ReceiptSizeGenerator` that will be used on links where the generator wasn't set
    /// with `Self::link_generator`. The default behavior is that no receipts are sent on links
    /// which don't have a generator set. This method allows to override the default behavior and
    /// make these links use the default generator.
    pub fn default_link_generator(
        mut self,
        link_generator_factory: impl Fn() -> Box<dyn ReceiptSizeGenerator> + 'static,
    ) -> Self {
        assert!(
            self.default_link_generator_factory.is_none(),
            "default link generator is already set!"
        );
        self.default_link_generator_factory = Some(Box::new(link_generator_factory));
        self
    }

    pub fn missing_chunk_probability(mut self, probability: f64) -> Self {
        self.scenario.missing_chunk_probability = probability;
        self
    }

    pub fn missing_block_probability(mut self, probability: f64) -> Self {
        self.scenario.missing_block_probability = probability;
        self
    }

    pub fn build(mut self) -> TestScenario {
        if self.scenario.num_shards == 0 {
            panic!("Zero shards in the scenario!");
        }

        if let Some(link_generator_factory) = self.default_link_generator_factory {
            for sender_idx in (0..self.scenario.num_shards).map(|i| i as ShardIndex) {
                let links_vec = self
                    .scenario
                    .link_generators
                    .entry(sender_idx as ShardIndex)
                    .or_insert_with(Vec::new);
                for receiver_idx in (0..self.scenario.num_shards).map(|i| i as ShardIndex) {
                    if links_vec.iter().position(|(idx, _generator)| *idx == receiver_idx).is_none()
                    {
                        links_vec.push((receiver_idx, link_generator_factory()));
                    }
                }
            }
        }
        self.scenario
    }
}

/// Bandwidth information about one chunk in the test.
/// How much was sent, received, what's in the buffered outgoing receipts.
pub struct ChunkBandwidthStats {
    pub total_incoming_receipts_size: ByteSize,
    pub total_outgoing_receipts_size: ByteSize,
    pub size_of_outgoing_receipts_to_shard: BTreeMap<ShardIndex, ByteSize>,
    pub size_of_buffered_receipts_to_shard: BTreeMap<ShardIndex, ByteSize>,
    /// Sizes of the first five receipts in the outgoing buffer.
    pub first_five_buffered_sizes: BTreeMap<ShardIndex, Vec<ByteSize>>,
    /// Sizes of the first five big (>100kB) receipts in the outgoing buffer.
    /// The big sizes is often what really dictates the generated bandwidth requests.
    pub first_five_big_buffered_sizes: BTreeMap<ShardIndex, Vec<ByteSize>>,
    pub congestion_level: f64,
}

impl ChunkBandwidthStats {
    pub fn new() -> ChunkBandwidthStats {
        ChunkBandwidthStats {
            total_incoming_receipts_size: ByteSize::b(0),
            total_outgoing_receipts_size: ByteSize::b(0),
            size_of_outgoing_receipts_to_shard: BTreeMap::new(),
            size_of_buffered_receipts_to_shard: BTreeMap::new(),
            first_five_buffered_sizes: BTreeMap::new(),
            first_five_big_buffered_sizes: BTreeMap::new(),
            congestion_level: -99999.0,
        }
    }
}

/// Bandwidth information about all chunks applied during the test.
pub struct TestBandwidthStats {
    pub chunk_stats: BTreeMap<(BlockHeight, ShardIndex), ChunkBandwidthStats>,
    pub num_shards: u64,
    pub scheduler_params: BandwidthSchedulerParams,
}

impl TestBandwidthStats {
    pub fn summarize(&self, active_links: &BTreeSet<(ShardIndex, ShardIndex)>) -> TestSummary {
        // When set to true, the function will print information about every chunk
        let print_chunks = true;

        let mut link_sent: BTreeMap<(ShardIndex, ShardIndex), ByteSize> = BTreeMap::new();
        let mut total_incoming: BTreeMap<ShardIndex, ByteSize> = BTreeMap::new();
        let mut total_outgoing: BTreeMap<ShardIndex, ByteSize> = BTreeMap::new();
        let mut max_incoming = ByteSize::b(0);
        let mut max_outgoing = ByteSize::b(0);

        let mut last_height = BlockHeight::MAX;
        for ((height, shard_idx), chunk_stat) in &self.chunk_stats {
            if print_chunks {
                if *height != last_height {
                    println!("======= Height: {} =======", height);
                }
                println!("[ Height = {}, shard_idx = {} ]", height, shard_idx);
                println!("  Incoming receipts: {:?}", chunk_stat.total_incoming_receipts_size);
                println!("  Outgoing receipts: {:?}", chunk_stat.total_outgoing_receipts_size);
                println!("");
                println!("  Congestion level: {:.2}", chunk_stat.congestion_level);
                println!("");
                println!("  Outgoing buffers:");
                for (receiver_shard_idx, outgoing_buffer_size) in
                    &chunk_stat.size_of_buffered_receipts_to_shard
                {
                    print!(
                        "    {} -> {} total: {:?}",
                        shard_idx, receiver_shard_idx, outgoing_buffer_size
                    );
                    if let Some(first_five) =
                        chunk_stat.first_five_buffered_sizes.get(receiver_shard_idx)
                    {
                        print!(", First five: {:?}", first_five);
                    }
                    if let Some(first_five_big) =
                        chunk_stat.first_five_big_buffered_sizes.get(receiver_shard_idx)
                    {
                        print!(", First big five: {:?}", first_five_big);
                    }
                    println!("");
                }
                println!("");
                println!("  Sent receipts:");
            }
            *total_incoming.entry(*shard_idx).or_default() +=
                chunk_stat.total_incoming_receipts_size;
            *total_outgoing.entry(*shard_idx).or_default() +=
                chunk_stat.total_outgoing_receipts_size;
            max_incoming = std::cmp::max(max_incoming, chunk_stat.total_incoming_receipts_size);
            max_outgoing = std::cmp::max(max_outgoing, chunk_stat.total_outgoing_receipts_size);
            for (receiver_shard_idx, sent) in &chunk_stat.size_of_outgoing_receipts_to_shard {
                if print_chunks {
                    println!("    {} -> {}: {:?}", shard_idx, receiver_shard_idx, sent);
                }
                if !active_links.contains(&(*shard_idx, *receiver_shard_idx)) {
                    continue;
                }
                *link_sent.entry((*shard_idx, *receiver_shard_idx)).or_insert(ByteSize::b(0)) +=
                    *sent;
            }
            if print_chunks {
                println!("");
            }
            last_height = *height;
        }

        let block_heights_num: u64 = self
            .chunk_stats
            .iter()
            .map(|((height, _), _)| *height)
            .collect::<BTreeSet<BlockHeight>>()
            .len()
            .try_into()
            .unwrap();

        let div_bytesize = |a: ByteSize, b: u64| ByteSize::b(a.as_u64() / b);

        let mut incoming_sum = ByteSize::b(0);
        for (_shard, incoming) in &total_incoming {
            incoming_sum += *incoming;
        }
        let avg_incoming = div_bytesize(incoming_sum, block_heights_num);
        let mut outgoing_sum = ByteSize::b(0);
        for (_shard, outgoing) in &total_outgoing {
            outgoing_sum += *outgoing;
        }
        let avg_outgoing = div_bytesize(outgoing_sum, block_heights_num);
        let avg_throughput = div_bytesize(avg_incoming + avg_outgoing, 2);

        let avg_incoming_per_shard = total_incoming
            .into_iter()
            .map(|(shard, incoming)| (shard, div_bytesize(incoming, block_heights_num)))
            .collect();
        let avg_outgoing_per_shard = total_outgoing
            .into_iter()
            .map(|(shard, outgoing)| (shard, div_bytesize(outgoing, block_heights_num)))
            .collect();

        let avg_sent_on_link: BTreeMap<(usize, usize), ByteSize> = link_sent
            .into_iter()
            .map(|(link, sent)| (link, div_bytesize(sent, block_heights_num)))
            .collect();
        let mut max_sent_on_link = ByteSize::b(1);
        let mut min_sent_on_link = ByteSize::b(u64::MAX);
        for (_link, avg_sent) in &avg_sent_on_link {
            if *avg_sent < min_sent_on_link {
                min_sent_on_link = std::cmp::max(*avg_sent, ByteSize::b(1));
            }
            if *avg_sent > max_sent_on_link {
                max_sent_on_link = *avg_sent;
            }
        }
        let link_imbalance_ratio =
            max_sent_on_link.as_u64() as f64 / min_sent_on_link.as_u64() as f64;

        let max_budget = vec![self.scheduler_params.max_shard_bandwidth; 1000];
        let estimated_link_throughputs =
            estimate_link_throughputs(active_links, &max_budget, &max_budget);

        let mut estimated_throughput = ByteSize::b(1);
        for (_link, link_throughput) in &estimated_link_throughputs {
            estimated_throughput += *link_throughput;
        }
        let bandwidth_utilization =
            avg_throughput.as_u64() as f64 / estimated_throughput.as_u64() as f64;

        let mut worst_link_estimation_ratio = 999999999999999.0;
        for (link, avg_sent) in &avg_sent_on_link {
            let estimation = estimated_link_throughputs.get(link).unwrap();
            let cur_ratio = if *estimation == ByteSize::b(0) {
                avg_sent.as_u64() as f64
            } else {
                avg_sent.as_u64() as f64 / estimation.as_u64() as f64
            };
            if cur_ratio < worst_link_estimation_ratio {
                worst_link_estimation_ratio = cur_ratio;
            }
        }

        TestSummary {
            avg_throughput,
            estimated_throughput,
            bandwidth_utilization,
            avg_incoming,
            avg_outgoing,
            avg_incoming_per_shard,
            avg_outgoing_per_shard,
            max_incoming,
            max_outgoing,
            avg_sent_on_link,
            estimated_link_throughputs,
            link_imbalance_ratio,
            worst_link_estimation_ratio,
            max_shard_bandwidth: ByteSize::b(self.scheduler_params.max_shard_bandwidth),
        }
    }
}

#[derive(Debug)]
pub struct TestSummary {
    /// How much was sent between all senders and receivers. Averaged over all heights.
    /// Ideally should be equal to `num_shards * max_shard_bandwidth`
    pub avg_throughput: ByteSize,
    /// Estimated maximum avg_throughput in this test.
    pub estimated_throughput: ByteSize,
    /// Total number of bytes sent / estimated ideal throughput.
    pub bandwidth_utilization: f64,
    /// Average size of incoming receipts in all chunks
    pub avg_incoming: ByteSize,
    /// Average size of outgoing receipts in all chunks
    pub avg_outgoing: ByteSize,
    /// Average size of incoming receipts to every shard
    pub avg_incoming_per_shard: BTreeMap<ShardIndex, ByteSize>,
    /// Average size of outgoing receipts on every shard
    pub avg_outgoing_per_shard: BTreeMap<ShardIndex, ByteSize>,
    /// Maximum size of incoming receipts to a chunk
    pub max_incoming: ByteSize,
    /// Maximum size of outgoing receipts generated in a chunk
    pub max_outgoing: ByteSize,
    /// Average size of receipts sent on every link
    pub avg_sent_on_link: BTreeMap<(ShardIndex, ShardIndex), ByteSize>,
    /// How much should ideally be sent on this link, theoretical estimation.
    pub estimated_link_throughputs: BTreeMap<(ShardIndex, ShardIndex), ByteSize>,
    /// Ratio between the link that sent the most and the link that sent the least.
    /// imbalance = max(avg_sent_on_link) / min(avg_sent_on_link)
    /// In an ideally fair system it should be 1.0.
    /// In reality it should be at most 2.0. Imagine an example where one link sends receipts with
    /// size equal to `max_bandwidth/2 + 1`, and another link sends tons of small receipts. In this
    /// case the first link will send at most half of what the other sends, as sending two receipts
    /// at once would cause it to go over the `max_bandwidth` limit.
    /// Lower is better.
    pub link_imbalance_ratio: f64,
    /// Ratio between how much was sent on a link and the calculated ideal throughput for this link.
    /// estimation_ratio = sent / estimation
    /// Ideally should be 1.0. In reality should be at least 0.5
    /// This field contains the worst (smallest) ratio from all the links.
    pub worst_link_estimation_ratio: f64,
    /// Maximum amount of data that a shard should be allowed to send or receive at a single height.
    pub max_shard_bandwidth: ByteSize,
}

impl std::fmt::Display for TestSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TestSummary {{")?;
        writeln!(f, "  avg_throughput {:?}", self.avg_throughput)?;
        writeln!(f, "  estimated_throughput {:?}", self.estimated_throughput)?;
        writeln!(f, "  avg_incoming {:?}", self.avg_incoming)?;
        writeln!(f, "  avg_outgoing {:?}", self.avg_outgoing)?;
        writeln!(f, "  avg_incoming_per_shard {:?}", self.avg_incoming_per_shard)?;
        writeln!(f, "  avg_outgoing_per_shard {:?}", self.avg_outgoing_per_shard)?;
        writeln!(f, "  max_incoming {:?}", self.max_incoming)?;
        writeln!(f, "  max_outgoing {:?}", self.max_outgoing)?;
        writeln!(f, "  avg_sent_on_link:")?;
        fn write_link_info(
            info: &BTreeMap<(ShardIndex, ShardIndex), ByteSize>,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            let mut last_sender = ShardIndex::MAX;
            for (link, link_info) in info {
                if link.0 != last_sender {
                    if last_sender != ShardIndex::MAX {
                        writeln!(f, "")?;
                    }
                    write!(f, "    {} -> ", link.0)?;
                    last_sender = link.0;
                }
                write!(f, "({}: {:?}) ", link.1, link_info)?;
            }
            writeln!(f, "")
        }
        write_link_info(&self.avg_sent_on_link, f)?;
        writeln!(f, "  estimated_link_throughputs:")?;
        write_link_info(&self.estimated_link_throughputs, f)?;
        writeln!(f, "")?;
        writeln!(
            f,
            "  bandwidth_utilization {:.2}% (Should be at least 50%)",
            self.bandwidth_utilization * 100.0
        )?;
        writeln!(
            f,
            "  link_imbalance_ratio {:.2} (Should be at most 2.0)",
            self.link_imbalance_ratio
        )?;
        writeln!(
            f,
            "  worst_link_estimation_ratio {:.2} (Should be at least 0.5)",
            self.worst_link_estimation_ratio
        )?;
        writeln!(f, "  max_shard_bandwidth {:?}", self.max_shard_bandwidth)?;
        writeln!(f, "}}")
    }
}

/// cspell:ignore Flukerson
/// Estimate maximum throughput of each link when the active links are sending receipts at full
/// speed.
/// In a simple situation like [0 -> 0], maximum link throughput is equal to `max_shard_bandwidth`.
/// If the situation is [0 -> 0, 1 -> 0], then the maximum throughput would be `max_shard_bandwidth
/// / 2`. In more complex situations things can get much more complicated. The function estimates
/// link throughputs by repeatedly granting a bit of bandwidth on every link, as long as possible.
/// Ideally this would be done with some sort of network flow algorithm, but for now this will do.I
/// guess granting a bit on all links is like poor man's Ford-Flukerson.
/// TODO(bandwidth_scheduler) - make this better.
pub fn estimate_link_throughputs(
    active_links: &BTreeSet<(ShardIndex, ShardIndex)>,
    sender_budgets: &[Bandwidth],
    receiver_budgets: &[Bandwidth],
) -> BTreeMap<(ShardIndex, ShardIndex), ByteSize> {
    if active_links.is_empty() {
        return BTreeMap::new();
    }
    let max_index = active_links.iter().map(|(a, b)| std::cmp::max(*a, *b)).max().unwrap();
    let num_shards = max_index + 1;

    let min_nonzero_budget = sender_budgets
        .iter()
        .chain(receiver_budgets.iter())
        .filter(|b| **b > 0)
        .min()
        .unwrap_or(&0);
    let single_increase = std::cmp::max(1, min_nonzero_budget / num_shards as u64);

    let mut sender_granted = vec![0; num_shards];
    let mut receiver_granted = vec![0; num_shards];
    let mut link_granted = vec![vec![0; num_shards]; num_shards];

    let mut links: Vec<(ShardIndex, ShardIndex)> = active_links.iter().copied().collect();
    while !links.is_empty() {
        let mut next_links = Vec::new();
        for link in links {
            if sender_granted[link.0] + single_increase <= sender_budgets[link.0]
                && receiver_granted[link.1] + single_increase <= receiver_budgets[link.1]
            {
                sender_granted[link.0] += single_increase;
                receiver_granted[link.1] += single_increase;
                link_granted[link.0][link.1] += single_increase;
                next_links.push(link);
            }
        }
        links = next_links;
    }

    let mut res = BTreeMap::new();
    for link in active_links {
        res.insert(*link, ByteSize::b(link_granted[link.0][link.1]));
    }
    res
}
