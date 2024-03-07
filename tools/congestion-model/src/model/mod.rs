mod block_info;
mod chunk_execution;
mod queue;
mod queue_bundle;
mod transaction;
mod transaction_registry;

pub use block_info::BlockInfo;
use chrono::{DateTime, Utc};
pub use chunk_execution::*;
pub use queue::*;
pub use queue_bundle::*;
pub use transaction::Receipt;
pub use transaction_registry::TransactionId;

pub(crate) use transaction::Transaction;

use crate::strategy::StatsWriter;
use crate::workload::Producer;
use crate::{CongestionStrategy, Round};
use std::collections::BTreeMap;
use std::time::Duration;
use transaction_registry::TransactionRegistry;

pub struct Model {
    /// Model execution round incremental value.
    ///
    /// This is akin to block height, starting at round = 1 for the first
    /// execution round. (round = 0 means execution hasn't started, yet)
    pub(crate) round: Round,

    // Congestion strategy state
    pub(crate) shards: Vec<Box<dyn CongestionStrategy>>,
    pub(crate) shard_ids: Vec<ShardId>,
    pub(crate) block_info: BTreeMap<ShardId, BlockInfo>,
    pub(crate) queues: QueueBundle,

    // Workload state
    pub(crate) transactions: TransactionRegistry,
    pub(crate) producer: Box<dyn Producer>,

    stats_writer: StatsWriter,
    start_time: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub usize);

impl Model {
    pub fn new(
        mut shards: Vec<Box<dyn CongestionStrategy>>,
        mut producer: Box<dyn Producer>,
        mut stats_writer: StatsWriter,
    ) -> Self {
        // Set the start time to an hour ago to make it visible by default in
        // grafana. Each round is 1 virtual second so 1 hour is good for looking
        // at a maximum of 3600 rounds, beyond that you'll need to customize the
        // grafana time range.
        let start_time = Utc::now() - Duration::from_secs(1 * 60 * 60);
        let num_shards = shards.len();
        let shard_ids: Vec<_> = (0..num_shards).map(ShardId).collect();
        let mut queues = QueueBundle::new(&shard_ids);

        if let Some(stats_writer) = &mut stats_writer {
            stats_writer.write_field("time").unwrap();
            stats_writer.write_field("round").unwrap();
        }

        for (shard, &id) in shards.iter_mut().zip(&shard_ids) {
            shard.init(id, &shard_ids, &mut queues, &mut stats_writer);
        }

        producer.init(&shard_ids);

        if let Some(stats_writer) = &mut stats_writer {
            stats_writer.write_record(None::<&[u8]>).unwrap();
        }

        Self {
            shards,
            shard_ids,
            block_info: Default::default(),
            transactions: Default::default(),
            producer,
            round: 0,
            queues,
            stats_writer,
            start_time,
        }
    }

    /// execute one round of the model
    pub fn step(&mut self) {
        self.round += 1;

        if let Some(stats_writer) = &mut self.stats_writer {
            let time = self.start_time + Duration::from_secs(self.round);

            stats_writer.write_field(format!("{:?}", time)).unwrap();
            stats_writer.write_field(format!("{}", self.round)).unwrap();
        }

        // Generate new transactions and place them in the per-shard transaction queues.
        let new_transactions = self.generate_tx_for_round();
        for tx_id in new_transactions {
            let shard_id = self.transactions[tx_id].sender_shard;
            self.queues.incoming_transactions_mut(shard_id).push_back(tx_id);
        }

        // Give each shard a chance to their computations and buffer all created outputs.
        let mut outgoing = vec![];
        let mut next_block = BTreeMap::new();
        for (i, shard) in self.shards.iter_mut().enumerate() {
            let id = ShardId(i);
            let mut ctx = ChunkExecutionContext::new(
                &mut self.queues,
                &mut self.transactions,
                &self.block_info,
                self.round,
                ShardId(i),
            );
            shard.compute_chunk(&mut ctx, &mut self.stats_writer);
            let (mut forwarded_receipts, shared_block_info) = ctx.finish();

            outgoing.append(&mut forwarded_receipts);
            next_block.insert(id, shared_block_info);
        }

        if let Some(stats_writer) = &mut self.stats_writer {
            stats_writer.write_record(None::<&[u8]>).unwrap();
        }

        // Propagate outputs from this round to inputs for the next round.
        self.block_info = next_block;
        for receipt in outgoing {
            // TODO: Deal with postponed receipts. There should be a separate
            // queue where they are kept until all dependencies have been
            // resolved. But for now, there is no producer of such workload.
            self.queues.incoming_receipts_mut(receipt.receiver).push_back(receipt);
        }
    }

    fn generate_tx_for_round(&mut self) -> Vec<TransactionId> {
        // Scenario implementations need access to a tx factory, they should be
        // able to create as many transactions as they want. But we want the
        // model to control how these are created and registered.
        // Hence, we inject a factory as a dependency and collect the created
        // builders as the output.
        let mut tx_factory = |shard_id| self.transactions.new_transaction_builder(shard_id);
        let tx_builders =
            self.producer.produce_transactions(self.round, &self.shard_ids, &mut tx_factory);

        // Now we take all created transactions and register them properly. Return tx ids.
        tx_builders
            .into_iter()
            .map(|builder| self.transactions.build_transaction(builder))
            .collect()
    }

    pub fn shard(&mut self, id: ShardId) -> &mut dyn CongestionStrategy {
        self.shards[id.0].as_mut()
    }

    /// Ordered list of shard IDs
    pub fn shard_ids(&self) -> &[ShardId] {
        &self.shard_ids
    }

    pub fn queue(&mut self, id: QueueId) -> &mut Queue {
        self.queues.queue_mut(id)
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
