use near_lake_framework::LakeConfigBuilder;
use near_lake_framework::near_indexer_primitives::StreamerMessage;
use near_lake_framework::near_indexer_primitives::views::ExecutionOutcomeView;
use near_o11y::tracing_subscriber::EnvFilter;
use near_o11y::tracing_subscriber::fmt::Subscriber;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::shard_utilization::ShardUtilization;
use near_primitives::types::{AccountId, ShardId, StateChangeCause, StateRoot};
use near_store::shard_utilization::{
    account_utilization, find_split_account, total_utilization, update_shard_utilization,
};
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::mem::node::MemTrieNodeView;
use near_store::trie::update::TrieUpdateResult;
use near_store::{ShardTries, Trie, TrieUpdate};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tracing::info;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct ShardUpdate<'a> {
    epoch_stats: &'a mut EpochStats,
    trie_update: TrieUpdate,
    shard_uid: ShardUId,
    shard_left_boundary: Option<AccountId>,
    exec_duration: Duration,
}

impl<'a> ShardUpdate<'a> {
    fn new(epoch_stats: &'a mut EpochStats, shard_id: impl Into<u64>) -> anyhow::Result<Self> {
        let start = Instant::now();
        let shard_id = ShardId::from(shard_id.into());
        let shard_idx = epoch_stats.shard_layout.get_shard_index(shard_id)?;
        let shard_uid = epoch_stats.shard_layout.get_shard_uid(shard_idx)?;
        let shard_left_boundary = (shard_idx > 0)
            .then(|| epoch_stats.shard_layout.boundary_accounts()[shard_idx - 1].clone());
        let state_root = epoch_stats
            .state_roots
            .get(&shard_uid)
            .ok_or_else(|| anyhow::anyhow!("no state root for shard {shard_uid}"))?;
        let trie = epoch_stats.shard_tries.get_trie_for_shard(shard_uid, *state_root);
        let trie_update = TrieUpdate::new(trie);
        let total_duration = start.elapsed();
        Ok(Self {
            epoch_stats,
            trie_update,
            shard_uid,
            shard_left_boundary,
            exec_duration: total_duration,
        })
    }

    fn update_shard_utilization(&mut self, outcome: &ExecutionOutcomeView) -> anyhow::Result<()> {
        let start = Instant::now();

        update_shard_utilization(
            &mut self.trie_update,
            &outcome.executor_id,
            ShardUtilization::V1(outcome.gas_burnt.into()),
            self.shard_left_boundary.as_ref(),
        )?;

        self.exec_duration += start.elapsed();
        Ok(())
    }

    fn finalize(mut self) -> anyhow::Result<Duration> {
        let start = Instant::now();

        self.trie_update.commit(StateChangeCause::InitialState);
        let TrieUpdateResult { trie_changes, .. } = self.trie_update.finalize()?;
        let shard_tries = &self.epoch_stats.shard_tries;
        let mut store_update = shard_tries.store_update();
        let new_root = shard_tries.apply_all(&trie_changes, self.shard_uid, &mut store_update);
        shard_tries.apply_memtrie_changes(&trie_changes, self.shard_uid, 0);
        store_update.commit()?;
        self.epoch_stats.state_roots.insert(self.shard_uid, new_root);

        Ok(start.elapsed())
    }
}

struct EpochStats {
    shard_layout: ShardLayout,
    shard_tries: ShardTries,
    state_roots: HashMap<ShardUId, StateRoot>,
    exec_time: HashMap<ShardUId, Duration>,
    num_blocks: u64,
}

impl EpochStats {
    fn new(shard_layout: ShardLayout) -> Self {
        let (shard_tries, shard_layout) = TestTriesBuilder::new()
            .with_shard_layout(shard_layout)
            .with_in_memory_tries(true)
            .with_flat_storage(true)
            .build2();
        let state_roots = shard_layout.shard_uids().map(|uid| (uid, Trie::EMPTY_ROOT)).collect();
        Self {
            shard_layout,
            shard_tries,
            state_roots,
            exec_time: Default::default(),
            num_blocks: 0,
        }
    }

    fn process_block(&mut self, msg: &StreamerMessage) -> anyhow::Result<()> {
        for shard in &msg.shards {
            let mut shard_update = ShardUpdate::new(self, shard.shard_id)?;
            let shard_uid = shard_update.shard_uid;

            // Process transactions
            for tx in shard.chunk.iter().flat_map(|c| &c.transactions) {
                let outcome = &tx.outcome.execution_outcome.outcome;
                shard_update.update_shard_utilization(outcome)?;
            }

            // Process receipts
            for outcome in &shard.receipt_execution_outcomes {
                let outcome = &outcome.execution_outcome.outcome;
                shard_update.update_shard_utilization(outcome)?;
            }

            let exec_time = shard_update.finalize()?;
            *self.exec_time.entry(shard_uid).or_default() += exec_time;
        }
        self.num_blocks += 1;
        Ok(())
    }

    fn shard_trie_size(&self, shard_uid: ShardUId) -> anyhow::Result<u64> {
        let state_root = self
            .state_roots
            .get(&shard_uid)
            .ok_or_else(|| anyhow::anyhow!("no state root for shard {shard_uid}"))?;

        let memtries = self
            .shard_tries
            .get_memtries(shard_uid)
            .ok_or_else(|| anyhow::anyhow!("Cannot get memtrie"))?;
        let read_guard = memtries.read();
        let root_ptr = read_guard.get_root(state_root)?;

        let mut queue = VecDeque::new();
        queue.push_back(root_ptr);
        let mut total_size = 0;

        while let Some(node_ptr) = queue.pop_front() {
            total_size += node_ptr.size_of_allocation() as u64;
            match node_ptr.view() {
                MemTrieNodeView::Leaf { .. } => {}
                MemTrieNodeView::Extension { child, .. } => queue.push_back(child),
                MemTrieNodeView::Branch { children, .. }
                | MemTrieNodeView::BranchWithValue { children, .. } => {
                    queue.extend(children.iter())
                }
            }
        }

        Ok(total_size)
    }

    fn print_stats(&self) -> anyhow::Result<()> {
        for shard_uid in self.shard_layout.shard_uids() {
            let state_root = *self.state_roots.get(&shard_uid).unwrap();
            let trie = self.shard_tries.get_trie_for_shard(shard_uid, state_root);
            let total_utilization = total_utilization(&trie)?;
            let split_account = find_split_account(&trie)?;
            let split_acc_utilization = split_account
                .as_ref()
                .map(|acc| account_utilization(&trie, acc))
                .transpose()?
                .unwrap_or_default();
            let split_acc_util_perc = split_acc_utilization / total_utilization * 100.0;
            let shard_trie_size_mib = self.shard_trie_size(shard_uid)? / 1024 / 1024;
            let shard_exec_time = self.exec_time.get(&shard_uid).cloned().unwrap_or_default();
            let ms_per_block = shard_exec_time.as_millis() as u64 / self.num_blocks;
            let split_account = split_account.map(|acc| acc.to_string());
            info!(
                "Shard {shard_uid} stats:
    total utilization = {total_utilization:?}
    split account = '{split_account:?}'
    split account utilization = {split_acc_utilization:?} ({split_acc_util_perc:.2} %)
    trie size = {shard_trie_size_mib} MiB
    exec time = {shard_exec_time:?} ({ms_per_block} ms / block)"
            );
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::new("info");
    Subscriber::builder().with_env_filter(env_filter).with_writer(std::io::stderr).init();

    let start_block: u64 = std::env::args().nth(1).unwrap().parse()?;
    let end_block: u64 = std::env::args().nth(2).unwrap().parse()?;

    let config = LakeConfigBuilder::default().mainnet().start_block_height(start_block).build()?;
    let (_, mut receiver) = near_lake_framework::streamer(config);

    let config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let shard_layout = config_store.get_config(76).shard_layout.clone();
    let mut epoch_stats = EpochStats::new(shard_layout);

    while let Some(msg) = receiver.recv().await {
        let block_height = msg.block.header.height;
        if block_height >= end_block {
            break;
        }
        epoch_stats.process_block(&msg)?;

        let num_blocks = block_height - start_block + 1;
        if num_blocks % 100 == 0 {
            info!("{num_blocks} blocks processed.");
            epoch_stats.print_stats()?
        }
    }

    Ok(())
}
